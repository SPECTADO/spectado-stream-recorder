package recorder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/spectado/stream-recorder/internal/adts"
)

// FFmpegCapabilities describes version-dependent options of the ffmpeg binary
// in use (detected once at startup).
type FFmpegCapabilities struct {
	HLSExtensionPicky bool // hls demuxer knows -extension_picky (ffmpeg >= 7.1)
	CAFile            string
}

// activeRecording is a session that is currently being captured: the file is
// open and a supervisor goroutine keeps an ffmpeg process alive.
type activeRecording struct {
	s    *Session
	file *os.File

	bytes    atomic.Int64 // bytes in the file
	runBytes atomic.Int64 // bytes written by the current ffmpeg run
	lastData atomic.Int64 // unix nanoseconds of the last write
	running  atomic.Bool
	pid      atomic.Int64
	writeErr atomic.Pointer[error]
	stderr   *stderrBuffer

	bytesCounter    prometheus.Counter
	runningGauge    prometheus.Gauge
	lastDataGauge   prometheus.Gauge
	restartsCounter prometheus.Counter

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // closed when the supervisor goroutine exits

	supervised   bool // supervisor started
	stopping     bool // stop requested; removed from active map
	recovered    bool // resumed from a sidecar after a restart
	copyFailures int  // consecutive copy-mode runs rejected by the muxer
	safeArgs     bool // ffmpeg rejected an optional option: use the minimal command line
	stallKilled  atomic.Bool
	savedBytes   int64

	// child process accounting (persister goroutine only)
	statPID int
	statCPU float64 // cumulative cpu seconds of statPID at last sample
}

func (ar *activeRecording) lastDataTime() time.Time {
	n := ar.lastData.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// captureWriter appends ffmpeg's stdout to the recording file.
type captureWriter struct {
	ar *activeRecording
}

func (w *captureWriter) Write(p []byte) (int, error) {
	n, err := w.ar.file.Write(p)
	if n > 0 {
		w.ar.bytes.Add(int64(n))
		w.ar.runBytes.Add(int64(n))
		now := time.Now()
		w.ar.lastData.Store(now.UnixNano())
		w.ar.bytesCounter.Add(float64(n))
		w.ar.lastDataGauge.Set(float64(now.Unix()))
	}
	if err != nil {
		e := err
		w.ar.writeErr.Store(&e)
	}
	return n, err
}

// ---------------------------------------------------------------------------
// stderr handling
// ---------------------------------------------------------------------------

var (
	levelTagRe = regexp.MustCompile(`\[(panic|fatal|error|warning|info|verbose|debug|trace)\] `)
	urlRe      = regexp.MustCompile(`https?://[^\s"'<>\]\)]+`)
)

// RedactURL hides credentials and query parameter values of a URL. Strings
// that do not look like URLs are returned unchanged.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return raw
	}
	if u.User != nil {
		if _, has := u.User.Password(); has {
			u.User = url.UserPassword("REDACTED", "REDACTED")
		} else {
			u.User = url.User("REDACTED")
		}
	}
	if u.RawQuery != "" {
		q := u.Query()
		keys := make([]string, 0, len(q))
		for k := range q {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte('&')
			}
			sb.WriteString(url.QueryEscape(k))
			sb.WriteString("=REDACTED")
		}
		u.RawQuery = sb.String()
	}
	return u.String()
}

// redactLine masks every URL in a log line.
func redactLine(line string) string {
	return urlRe.ReplaceAllStringFunc(line, RedactURL)
}

// stderrBuffer splits ffmpeg's stderr into lines, keeps the last few and
// forwards a rate-limited subset to the logger.
type stderrBuffer struct {
	mu          sync.Mutex
	partial     []byte
	tail        []string
	maxTail     int
	logLine     func(level slog.Level, line string)
	mode        string // warn | debug | off
	window      time.Time
	inWindow    int
	perMinute   int
	suppressed  int
	lastMessage string
}

func newStderrBuffer(mode string, logLine func(slog.Level, string)) *stderrBuffer {
	return &stderrBuffer{maxTail: 20, perMinute: 10, logLine: logLine, mode: mode}
}

func (b *stderrBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.partial = append(b.partial, p...)
	for {
		i := bytes.IndexAny(b.partial, "\r\n")
		if i < 0 {
			break
		}
		line := strings.TrimSpace(string(b.partial[:i]))
		b.partial = b.partial[i+1:]
		if line != "" {
			b.pushLocked(line)
		}
	}
	if len(b.partial) > 4096 { // pathological line without newline
		b.pushLocked(strings.TrimSpace(string(b.partial)))
		b.partial = b.partial[:0]
	}
	return len(p), nil
}

// parseLevel extracts ffmpeg's "[warning] " tag (present with -loglevel level+...).
func parseLevel(line string) (slog.Level, string) {
	level := slog.LevelWarn
	if m := levelTagRe.FindStringSubmatchIndex(line); m != nil {
		switch line[m[2]:m[3]] {
		case "panic", "fatal", "error":
			level = slog.LevelError
		case "warning":
			level = slog.LevelWarn
		case "info":
			level = slog.LevelInfo
		default:
			level = slog.LevelDebug
		}
		line = line[:m[0]] + line[m[1]:]
	}
	return level, line
}

func (b *stderrBuffer) pushLocked(line string) {
	if len(line) > 1024 {
		line = line[:1024] + "…"
	}
	level, msg := parseLevel(line)
	msg = redactLine(msg)
	b.lastMessage = msg
	b.tail = append(b.tail, time.Now().UTC().Format(time.RFC3339)+" "+msg)
	if len(b.tail) > b.maxTail {
		b.tail = b.tail[len(b.tail)-b.maxTail:]
	}
	if b.logLine == nil || b.mode == "off" {
		return
	}
	if b.mode == "debug" {
		b.logLine(slog.LevelDebug, msg)
		return
	}
	if level < slog.LevelWarn {
		return
	}
	now := time.Now()
	if now.Sub(b.window) > time.Minute {
		if b.suppressed > 0 {
			b.logLine(slog.LevelWarn, fmt.Sprintf("(%d further ffmpeg messages suppressed in the last minute)", b.suppressed))
		}
		b.window, b.inWindow, b.suppressed = now, 0, 0
	}
	b.inWindow++
	if b.inWindow <= b.perMinute {
		b.logLine(level, msg)
	} else {
		b.suppressed++
	}
}

// Tail returns a copy of the last lines.
func (b *stderrBuffer) Tail() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.tail))
	copy(out, b.tail)
	return out
}

// Last returns the most recent stderr line.
func (b *stderrBuffer) Last() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastMessage
}

// ---------------------------------------------------------------------------
// command lines
// ---------------------------------------------------------------------------

// headerBlock renders extra HTTP headers in the format ffmpeg expects.
func headerBlock(h map[string]string) string {
	if len(h) == 0 {
		return ""
	}
	var sb strings.Builder
	for k, v := range h {
		sb.WriteString(k)
		sb.WriteString(": ")
		sb.WriteString(v)
		sb.WriteString("\r\n")
	}
	return sb.String()
}

// isHLS guesses whether the source is an HLS playlist.
func isHLS(s *Session) bool {
	if s.Type == "hls" {
		return true
	}
	if s.Type == "icecast" {
		return false
	}
	u, err := url.Parse(s.Source)
	if err != nil {
		return false
	}
	p := strings.ToLower(u.Path)
	return strings.HasSuffix(p, ".m3u8") || strings.HasSuffix(p, ".m3u")
}

// inputArgs are the network/robustness options shared by ffmpeg and ffprobe.
// restart selects the HLS live-edge behaviour: on a restart we start at the
// newest segment to avoid re-appending the ~3 segments ffmpeg replays by
// default; on a fresh start that replay is welcome pre-roll. safe drops every
// optional protocol/demuxer option (used after ffmpeg rejected one of them:
// unused input options are fatal for ffmpeg).
func (m *Manager) inputArgs(s *Session, restart, safe bool) []string {
	args := []string{
		"-protocol_whitelist", "http,https,tcp,tls,crypto,httpproxy,data",
		"-user_agent", m.cfg.FFmpegUserAgent,
		"-rw_timeout", strconv.FormatInt(m.cfg.FFmpegRWTimeout.Microseconds(), 10),
		"-reconnect", "1",
		"-reconnect_streamed", "1",
		"-reconnect_on_network_error", "1",
		"-reconnect_on_http_error", "4xx,5xx",
		"-reconnect_delay_max", "5",
	}
	if hb := headerBlock(s.Headers); hb != "" {
		args = append(args, "-headers", hb)
	}
	// TLS options only exist for the tls protocol: passing them to a plain
	// http input makes ffmpeg fail with "Option tls_verify not found". For
	// https they are always valid, so they stay even in safe mode.
	if strings.HasPrefix(strings.ToLower(s.Source), "https://") {
		if m.cfg.FFmpegTLSVerify && !s.InsecureTLS {
			args = append(args, "-tls_verify", "1")
			if m.caps.CAFile != "" {
				args = append(args, "-ca_file", m.caps.CAFile)
			}
		} else {
			args = append(args, "-tls_verify", "0")
		}
	}
	if safe {
		return args
	}
	args = append(args, "-icy", "0")
	if isHLS(s) {
		args = append(args, "-seg_max_retry", "3")
		if restart {
			args = append(args, "-live_start_index", "-1")
		}
		if m.caps.HLSExtensionPicky {
			args = append(args, "-extension_picky", "0")
		}
	}
	return args
}

// ffmpegArgs builds the full ffmpeg command line for one run.
func (m *Manager) ffmpegArgs(s *Session, codec string, restart, safe bool) []string {
	args := []string{
		"-nostdin", "-hide_banner", "-nostats",
		"-loglevel", "level+warning",
		"-threads", "1",
		"-filter_threads", "1",
		"-filter_complex_threads", "1",
	}
	args = append(args, m.inputArgs(s, restart, safe)...)
	args = append(args,
		"-i", s.Source,
		"-vn", "-sn", "-dn",
		"-map", "0:a:0",
	)
	if codec == "copy" {
		args = append(args, "-c:a", "copy")
	} else {
		bitrate := s.Bitrate
		if bitrate == "" {
			bitrate = m.cfg.AudioBitrate
		}
		args = append(args, "-c:a", "aac", "-b:a", bitrate)
	}
	args = append(args, "-f", "adts", "-flush_packets", "1", "pipe:1")
	return args
}

// probeResult is the subset of ffprobe's JSON output we care about.
type probeResult struct {
	Streams []struct {
		CodecName  string `json:"codec_name"`
		Profile    string `json:"profile"`
		SampleRate string `json:"sample_rate"`
		Channels   int    `json:"channels"`
	} `json:"streams"`
}

// probeCodec asks ffprobe for the codec/profile of the first audio stream.
func (m *Manager) probeCodec(ctx context.Context, s *Session) (codec, profile string, err error) {
	// Bound the number of concurrent probes (100 items can start together).
	select {
	case m.probeSem <- struct{}{}:
		defer func() { <-m.probeSem }()
	case <-ctx.Done():
		return "", "", ctx.Err()
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.FFprobeTimeout)
	defer cancel()

	args := []string{"-v", "error", "-analyzeduration", "2000000", "-probesize", "1000000"}
	args = append(args, m.inputArgs(s, false, false)...)
	args = append(args,
		"-select_streams", "a:0",
		"-show_entries", "stream=codec_name,profile,sample_rate,channels",
		"-of", "json",
		s.Source,
	)
	cmd := exec.CommandContext(ctx, m.cfg.FFprobePath, args...)
	cmd.WaitDelay = 2 * time.Second
	setSysProcAttr(cmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		if len(msg) > 300 {
			msg = msg[:300]
		}
		return "", "", errors.New(redactLine(msg))
	}
	var res probeResult
	if err := json.Unmarshal(out, &res); err != nil || len(res.Streams) == 0 {
		return "", "", errors.New("ffprobe returned no audio stream")
	}
	return strings.ToLower(res.Streams[0].CodecName), res.Streams[0].Profile, nil
}

// copyableProfile reports whether an AAC profile can be carried in ADTS.
func copyableProfile(profile string) bool {
	p := strings.ToLower(profile)
	return !strings.Contains(p, "xhe") && !strings.Contains(p, "usac")
}

// resolveCodec decides between stream copy and transcoding for the session.
func (m *Manager) resolveCodec(ctx context.Context, ar *activeRecording, snap *Session) string {
	if snap.ResolvedCodec != "" {
		return snap.ResolvedCodec
	}
	requested := snap.Codec
	if requested == "" || requested == "auto" {
		requested = m.cfg.AudioCodec
	}
	var resolved, why string
	switch requested {
	case "copy":
		resolved, why = "copy", "requested"
	case "aac":
		resolved, why = "aac", "requested"
	default: // auto
		codec, profile, err := m.probeCodec(ctx, snap)
		switch {
		case ctx.Err() != nil:
			return "aac"
		case err != nil:
			// Not persisted: a network blip must not pin the session to
			// transcoding; the next restart probes again.
			m.log.Warn("codec probe failed; transcoding this run, will re-probe on restart", "id", ar.s.ID, "error", err)
			return "aac"
		case codec == "aac" && copyableProfile(profile):
			resolved, why = "copy", "source codec aac "+profile
		default:
			resolved, why = "aac", "source codec "+codec+" "+profile
		}
	}
	m.mu.Lock()
	ar.s.ResolvedCodec = resolved
	_ = ar.s.save()
	m.mu.Unlock()
	m.log.Info("codec resolved", "id", ar.s.ID, "codec", resolved, "reason", why)
	return resolved
}

// ---------------------------------------------------------------------------
// supervisor
// ---------------------------------------------------------------------------

func isDiskFull(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT)
}

// supervise keeps an ffmpeg process running for the recording until its
// context is cancelled, restarting it with backoff after failures.
func (m *Manager) supervise(ar *activeRecording) {
	defer close(ar.done)
	ctx := ar.ctx
	backoff := m.cfg.FFmpegRestartBackoffMin
	log := m.log.With("id", ar.s.ID, "session", ar.s.SessionID)
	restart := ar.recovered // a resumed session behaves like a restart

	for ctx.Err() == nil {
		// Work from a copy: reconcile may update source/end/headers concurrently.
		m.mu.Lock()
		snap := *ar.s
		m.mu.Unlock()
		codec := m.resolveCodec(ctx, ar, &snap)
		if ctx.Err() != nil {
			return
		}
		if restart {
			m.trimTail(ar)
		}
		started := time.Now()
		ar.runBytes.Store(0)
		ar.writeErr.Store(nil)
		runErr := m.runFFmpegOnce(ctx, ar, &snap, codec, restart)
		ran := time.Since(started)
		runBytes := ar.runBytes.Load()
		if ctx.Err() != nil {
			return // intentional stop
		}
		restart = true

		var werr error
		if p := ar.writeErr.Load(); p != nil {
			werr = *p
		}
		diskFull := werr != nil && isDiskFull(werr)
		killed := wasKilled(runErr) && !ar.stallKilled.Load() // our own escalation is not an OOM kill

		m.mu.Lock()
		ar.s.Restarts++
		switch {
		case werr != nil:
			ar.s.WriteErrors++
			ar.s.LastError = fmt.Sprintf("write to recording file failed: %v", werr)
		case runErr != nil:
			ar.s.LastError = fmt.Sprintf("ffmpeg exited after %s: %v", ran.Truncate(time.Millisecond), runErr)
		default:
			ar.s.LastError = fmt.Sprintf("ffmpeg exited after %s (stream ended)", ran.Truncate(time.Millisecond))
		}
		if last := ar.stderr.Last(); last != "" && werr == nil {
			ar.s.LastError += " | " + last
		}
		// ffmpeg rejects unknown input options outright ("Option X not found"):
		// retry with the minimal, always-valid command line.
		if last := ar.stderr.Last(); runBytes == 0 && !ar.safeArgs && werr == nil &&
			strings.Contains(last, "Option") && strings.Contains(last, "not found") {
			ar.safeArgs = true
			ar.copyFailures = 0 // the empty run was not the codec's fault
			log.Warn("ffmpeg rejected an optional option; retrying with the minimal command line", "stderr", last)
		} else if codec == "copy" && runBytes == 0 && werr == nil && looksLikeCopyRejection(ar.stderr.Tail()) {
			// The muxer/codec stage rejected the stream (not a network outage).
			ar.copyFailures++
			explicit := snap.Codec == "copy" || ((snap.Codec == "" || snap.Codec == "auto") && m.cfg.AudioCodec == "copy")
			switch {
			case explicit:
				ar.s.LastError = "stream copy rejected by ffmpeg while codec=copy was requested (use aac or auto): " + ar.stderr.Last()
				log.Error("stream copy rejected; codec=copy was requested explicitly, not falling back", "stderr", ar.stderr.Last())
			case ar.copyFailures >= 2:
				ar.s.ResolvedCodec = "aac"
				m.met.CodecFallbacksTotal.Inc()
				log.Warn("stream copy rejected twice, falling back to transcoding", "stderr", ar.stderr.Last())
			}
		} else if runBytes > 0 {
			ar.copyFailures = 0
		}
		_ = ar.s.saveQuick()
		m.mu.Unlock()
		ar.restartsCounter.Inc()
		if werr != nil {
			m.met.WriteErrorsTotal.Inc()
		}
		if killed {
			m.met.FFmpegKilledTotal.Inc()
			log.Error("ffmpeg was killed by SIGKILL (OOM killer or external); restarting", "ran", ran.Truncate(time.Millisecond).String())
		}

		wait := backoff
		switch {
		case diskFull:
			wait = time.Minute
			log.Error("disk full while recording; retrying in 1m (recording continues into the same file when space frees up)",
				"error", werr, "bytes", ar.bytes.Load())
		case werr != nil:
			log.Error("recording file write failed; restarting ffmpeg", "error", werr, "backoff", wait.String())
		default:
			log.Warn("ffmpeg exited, restarting", "error", runErr, "ran", ran.Truncate(time.Millisecond).String(),
				"bytes", runBytes, "backoff", wait.String(), "stderr", ar.stderr.Last())
		}

		if ran > time.Minute {
			backoff = m.cfg.FFmpegRestartBackoffMin
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		backoff *= 2
		if backoff > m.cfg.FFmpegRestartBackoffMax {
			backoff = m.cfg.FFmpegRestartBackoffMax
		}
	}
}

// copyRejectionMarkers are stderr fragments ffmpeg prints when the ADTS muxer
// or codec stage refuses a stream-copied input (as opposed to network errors).
var copyRejectionMarkers = []string{
	"could not write header", "adts", "only aac", "matches no streams",
	"unsupported codec", "not currently supported in container",
}

func looksLikeCopyRejection(tail []string) bool {
	for _, line := range tail {
		l := strings.ToLower(line)
		for _, mk := range copyRejectionMarkers {
			if strings.Contains(l, mk) {
				return true
			}
		}
	}
	return false
}

// trimTail removes a partial ADTS frame left at the end of the file by a
// killed process before the next run appends to it.
func (m *Manager) trimTail(ar *activeRecording) {
	removed, err := adts.TrimPartialTail(ar.s.FilePath())
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			m.log.Warn("could not check recording tail", "id", ar.s.ID, "error", err)
		}
		return
	}
	if removed > 0 {
		ar.bytes.Add(-removed)
		m.mu.Lock()
		ar.s.TrimmedBytes += removed
		m.mu.Unlock()
		m.log.Info("trimmed partial ADTS frame before appending", "id", ar.s.ID, "session", ar.s.SessionID, "bytes", removed)
	}
}

// wasKilled reports whether the process died from SIGKILL.
func wasKilled(err error) bool {
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return false
	}
	if ws, ok := ee.Sys().(syscall.WaitStatus); ok {
		return ws.Signaled() && ws.Signal() == syscall.SIGKILL
	}
	return false
}

// runFFmpegOnce starts one ffmpeg process and waits for it to exit. It
// returns nil when ffmpeg exited with status 0.
func (m *Manager) runFFmpegOnce(ctx context.Context, ar *activeRecording, snap *Session, codec string, restart bool) error {
	pctx, pcancel := context.WithCancel(ctx)
	defer pcancel()

	args := m.ffmpegArgs(snap, codec, restart, ar.safeArgs)
	cmd := exec.CommandContext(pctx, m.cfg.FFmpegPath, args...)
	cmd.Stdout = &captureWriter{ar: ar}
	cmd.Stderr = ar.stderr
	cmd.Stdin = nil
	setSysProcAttr(cmd)
	exited := make(chan struct{})
	// On cancel ask ffmpeg to finish gracefully. ffmpeg's first SIGINT only
	// sets a flag that is checked between packets, so a process blocked in
	// network I/O (or the HLS playlist reload wait) would never notice; a
	// second SIGINT ~1s later aborts blocking I/O. WaitDelay escalates to
	// SIGKILL after the grace period.
	cmd.Cancel = func() error {
		err := cmd.Process.Signal(syscall.SIGINT)
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		go func() {
			select {
			case <-exited:
			case <-time.After(time.Second):
				_ = cmd.Process.Signal(syscall.SIGINT)
			}
		}()
		return err
	}
	cmd.WaitDelay = m.cfg.FFmpegStopGrace

	if err := cmd.Start(); err != nil {
		close(exited)
		m.met.FFmpegSpawnFailuresTotal.Inc()
		return fmt.Errorf("start ffmpeg: %w", err)
	}
	ar.stallKilled.Store(false)
	ar.lastData.Store(time.Now().UnixNano())
	ar.pid.Store(int64(cmd.Process.Pid))
	ar.running.Store(true)
	ar.runningGauge.Set(1)
	m.ffmpegCount.Add(1)
	m.met.FFmpegSpawnedTotal.Inc()
	m.log.Debug("ffmpeg started", "id", ar.s.ID, "pid", cmd.Process.Pid, "codec", codec, "restart", restart)

	// Stall watchdog.
	stall := m.cfg.FFmpegStallTimeout
	if snap.StallTimeout != nil && snap.StallTimeout.D() >= 5*time.Second {
		stall = snap.StallTimeout.D()
	}
	stopWatch := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopWatch:
				return
			case <-t.C:
				if time.Since(ar.lastDataTime()) > stall {
					m.log.Warn("no data from ffmpeg, restarting", "id", ar.s.ID, "pid", cmd.Process.Pid,
						"stall", stall.String(), "stderr", ar.stderr.Last())
					m.met.FFmpegStallsTotal.Inc()
					m.mu.Lock()
					ar.s.Stalls++
					m.mu.Unlock()
					ar.stallKilled.Store(true)
					pcancel()
					return
				}
			}
		}
	}()

	err := cmd.Wait()
	close(exited)
	close(stopWatch)
	ar.running.Store(false)
	ar.pid.Store(0)
	ar.runningGauge.Set(0)
	m.ffmpegCount.Add(-1)
	return err
}

// FFmpegVersion runs "<ffmpeg> -version" and returns the version token.
func FFmpegVersion(ctx context.Context, path string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, path, "-version").Output()
	if err != nil {
		return "", err
	}
	first := strings.SplitN(string(out), "\n", 2)[0]
	first = strings.TrimPrefix(first, "ffmpeg version ")
	first = strings.TrimPrefix(first, "ffprobe version ")
	if i := strings.Index(first, " Copyright"); i > 0 {
		first = first[:i]
	}
	return strings.TrimSpace(first), nil
}

// DetectFFmpegCapabilities inspects the ffmpeg binary for version-dependent
// options and locates a CA bundle for TLS verification.
func DetectFFmpegCapabilities(ctx context.Context, ffmpegPath string) FFmpegCapabilities {
	var caps FFmpegCapabilities
	hctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(hctx, ffmpegPath, "-hide_banner", "-h", "demuxer=hls").Output(); err == nil {
		caps.HLSExtensionPicky = bytes.Contains(out, []byte("extension_picky"))
	}
	for _, p := range []string{
		"/etc/ssl/certs/ca-certificates.crt", // Debian/Alpine
		"/etc/pki/tls/certs/ca-bundle.crt",   // RHEL
		"/etc/ssl/cert.pem",                  // macOS / BSD
	} {
		if _, err := os.Stat(p); err == nil {
			caps.CAFile = p
			break
		}
	}
	return caps
}
