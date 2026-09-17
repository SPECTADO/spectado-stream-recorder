package recorder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
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
	"github.com/spectado/stream-recorder/internal/id3"
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

	// Frame-aware writer state. splitter is owned by the writer goroutine
	// (single-threaded per run); run holds the live per-run counters and clock
	// anchor read by the persister and the API.
	splitter adts.Splitter
	run      runState

	// child process accounting (persister goroutine only)
	statPID int
	statCPU float64 // cumulative cpu seconds of statPID at last sample
}

// runState holds the live state of the current ffmpeg run for the frame-aware
// writer. The writer goroutine is its only mutator; readers (persister, API)
// take mu for a consistent snapshot. mu is always the inner lock: code that also
// needs Manager.mu takes m.mu first (never m.mu while holding this mu).
type runState struct {
	mu           sync.Mutex
	startedAt    time.Time
	anchor       time.Time
	anchorSource string  // "hls-pdt" | "wallclock"
	offset       int64   // file size at run start (first byte of the run)
	bytes        int64   // bytes written this run (frames + tags + junk)
	frames       int     // complete frames written this run
	seconds      float64 // media time of the run so far
	nextTagAt    float64 // media seconds at which the next ID3 tag is due
	tagsWritten  int
	mediaBefore  float64 // media seconds of the previous runs of this session
	started      bool    // at least one frame written this run
	closed       bool    // endRun has finalized this run's record
}

func (ar *activeRecording) lastDataTime() time.Time {
	n := ar.lastData.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// captureWriter appends ffmpeg's stdout to the recording file, splitting it into
// ADTS frames so it can insert in-band ID3 wall-clock tags between them. It never
// drops or reorders ffmpeg's bytes: frames and junk are written through verbatim
// (plus the inserted tags), so the file stays a faithful copy plus timing.
type captureWriter struct {
	ar *activeRecording
	m  *Manager
}

func (w *captureWriter) Write(p []byte) (int, error) {
	// The stall watchdog keys off raw arrival time, so stamp it here regardless
	// of whether p completes a frame (a partial frame still means data flows).
	now := time.Now()
	w.ar.lastData.Store(now.UnixNano())
	w.ar.lastDataGauge.Set(float64(now.Unix()))
	w.ar.splitter.Feed(p, w.onFrame, w.onJunk)
	if perr := w.ar.writeErr.Load(); perr != nil {
		return len(p), *perr
	}
	return len(p), nil
}

// onFrame handles one complete ADTS frame: it inserts an ID3 wall-clock tag when
// one is due, then writes the frame. The first frame of a run also opens the
// run's record.
func (w *captureWriter) onFrame(b []byte, h adts.Header) {
	ar := w.ar
	rs := &ar.run
	interval := w.m.cfg.ClockID3Interval

	rs.mu.Lock()
	first := !rs.started
	rs.started = true
	t := rs.seconds // media time of this frame's first sample within the run
	if first && rs.anchorSource == "wallclock" {
		rs.anchor = time.Now()
	}
	anchor := rs.anchor
	source := rs.anchorSource
	mediaBefore := rs.mediaBefore
	writeTag := interval > 0 && (first || t >= rs.nextTagAt)
	if writeTag {
		if first {
			// The first tag sits at t=0 and does not shift the cadence.
			rs.nextTagAt = interval.Seconds()
		} else {
			rs.nextTagAt = t + interval.Seconds()
		}
		rs.tagsWritten++
	}
	rs.mu.Unlock()

	if first {
		w.m.openRunRecord(ar)
	}
	if writeTag {
		wall := anchor.Add(time.Duration(t * float64(time.Second)))
		pts90k := uint64(math.Round((mediaBefore + t) * 90000))
		tag := id3.Tag(
			id3.AppleTimestamp(pts90k),
			id3.TXXX("WALLCLOCK", wall.UTC().Format("2006-01-02T15:04:05.000Z07:00")),
			id3.TXXX("WALLCLOCK-SOURCE", source),
		)
		ar.writeRaw(tag)
	}
	ar.writeRaw(b)

	rs.mu.Lock()
	rs.frames++
	rs.seconds += h.Duration()
	rs.mu.Unlock()
}

// onJunk writes bytes that are not part of a complete frame straight through so
// the file remains a faithful copy of ffmpeg's output.
func (w *captureWriter) onJunk(b []byte) { w.ar.writeRaw(b) }

// writeRaw appends b to the recording file and updates the byte counters. After
// a write error it becomes a no-op (ffmpeg will exit on EPIPE), matching the
// previous pass-through behaviour where a failing write stopped the copy.
func (ar *activeRecording) writeRaw(b []byte) {
	if ar.writeErr.Load() != nil {
		return
	}
	n, err := ar.file.Write(b)
	if n > 0 {
		ar.bytes.Add(int64(n))
		ar.runBytes.Add(int64(n))
		ar.bytesCounter.Add(float64(n))
		ar.run.mu.Lock()
		ar.run.bytes += int64(n)
		ar.run.mu.Unlock()
	}
	if err != nil {
		e := err
		ar.writeErr.Store(&e)
	}
}

// beginRun resets the frame-aware writer for the upcoming ffmpeg run with the
// clock anchor looked up for it. mediaBefore is the media time already captured
// by earlier runs of this session, so in-band PTS values keep increasing across
// restarts. It must be called before runFFmpegOnce (after any tail trim, so the
// offset is the true file size at run start).
func (ar *activeRecording) beginRun(anchor time.Time, source string, mediaBefore float64) {
	ar.run.mu.Lock()
	ar.run.startedAt = time.Now()
	ar.run.anchor = anchor
	ar.run.anchorSource = source
	ar.run.offset = ar.bytes.Load()
	ar.run.bytes = 0
	ar.run.frames = 0
	ar.run.seconds = 0
	ar.run.nextTagAt = 0
	ar.run.tagsWritten = 0
	ar.run.mediaBefore = mediaBefore
	ar.run.started = false
	ar.run.closed = false
	ar.run.mu.Unlock()
	ar.splitter = adts.Splitter{}
}

// openRunRecord appends the record for the run that just produced its first
// frame. Runs that never produce audio leave no record. The record is persisted
// immediately (not only on the next persist tick) so a crash within the first
// persist interval keeps the run's offset and anchor durable: without it a lost
// run 0 record would leave ScanRuns attributing the pre-crash audio to a run
// whose offset is no longer 0, orphaning the leading segment (SPEC B5).
func (m *Manager) openRunRecord(ar *activeRecording) {
	ar.run.mu.Lock()
	r := Run{
		StartedAt:    ar.run.startedAt,
		Anchor:       ar.run.anchor,
		AnchorSource: ar.run.anchorSource,
		Offset:       ar.run.offset,
	}
	ar.run.mu.Unlock()

	m.mu.Lock()
	if ar.s.appendRun(r) {
		m.log.Warn("run history reached the cap; coalescing oldest runs", "id", ar.s.ID, "session", ar.s.SessionID, "cap", maxRuns)
	}
	_ = ar.s.saveQuick()
	m.mu.Unlock()
}

// sessionMediaBefore returns the media time already captured by the previous
// runs of the session, so the in-band PTS keeps increasing across restarts.
func (m *Manager) sessionMediaBefore(ar *activeRecording) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var sum float64
	for _, r := range ar.s.Runs {
		sum += r.DurationSeconds
	}
	return sum
}

// endRun flushes any buffered partial frame to the file and finalizes the open
// run record. It is idempotent (called from supervise on a normal run end and
// from finalize on the ctx-cancel path) and a no-op for a run that produced no
// audio.
func (m *Manager) endRun(ar *activeRecording) {
	ar.run.mu.Lock()
	if ar.run.closed {
		ar.run.mu.Unlock()
		return
	}
	ar.run.closed = true
	// Capture the byte count BEFORE flushing the trailing partial frame: for a
	// run that restarts, TrimPartialTail removes that partial before the next run
	// appends, so the run's real byte range ends here. The last run's partial
	// stays in the file and is reconciled by the finalize sanity check instead.
	started := ar.run.started
	bytes := ar.run.bytes
	frames := ar.run.frames
	seconds := ar.run.seconds
	ar.run.mu.Unlock()

	// Write the trailing partial frame (if any) as junk; TrimPartialTail removes
	// it before the next run appends (the last run keeps it until finalize).
	ar.splitter.Flush(func(b []byte) { ar.writeRaw(b) })

	if !started {
		return // no audio this run: no record to close
	}

	now := time.Now()
	m.mu.Lock()
	if r := ar.s.openRun(); r != nil {
		r.EndedAt = &now
		r.Bytes = bytes
		r.Frames = frames
		r.DurationSeconds = seconds
	}
	_ = ar.s.saveQuick()
	m.mu.Unlock()
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
	suppressed  int // log lines dropped by the per-minute rate limit
	lastMessage string
	// Benign-noise filter and per-run diagnostics (A1/A2).
	onSuppressed  func(reason string) // hook fired for each benign line dropped
	benignRun     int                 // benign lines dropped during the current run
	lastErrorLine string              // most recent [error]/[fatal]/[panic] line of the run
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
	// Drop provably-harmless noise (see benignStderrMarkers): it must not fill
	// the tail, pollute lastMessage/LastError, or burn the log rate budget. In
	// debug mode the operator asked for everything, so it is still logged (at
	// Debug) but stays out of the tail and lastMessage.
	if reason := benignReason(msg); reason != "" {
		b.benignRun++
		if b.onSuppressed != nil {
			b.onSuppressed(reason)
		}
		if b.mode == "debug" && b.logLine != nil {
			b.logLine(slog.LevelDebug, msg)
		}
		return
	}
	if level == slog.LevelError {
		b.lastErrorLine = msg
	}
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

// resetRun zeroes the per-run diagnostics (benign counter and error line) at
// the start of a new ffmpeg run. The rolling tail and lastMessage are kept.
func (b *stderrBuffer) resetRun() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.benignRun = 0
	b.lastErrorLine = ""
}

// suppressedRun returns how many benign lines were dropped in the current run.
func (b *stderrBuffer) suppressedRun() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.benignRun
}

// LastErrorLine returns the most recent error-level line of the current run
// (post-redaction), or "" when none was seen.
func (b *stderrBuffer) LastErrorLine() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastErrorLine
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
		// Anchor the upcoming run to the source playlist's wall clock (best
		// effort). Looked up synchronously right before ffmpeg starts, so the
		// race window to ffmpeg's own first playlist fetch is only its start-up.
		anchor, source := m.lookupAnchor(ctx, &snap, restart)
		if ctx.Err() != nil {
			return
		}
		ar.beginRun(anchor, source, m.sessionMediaBefore(ar))
		started := time.Now()
		ar.runBytes.Store(0)
		ar.stderr.resetRun()
		ar.writeErr.Store(nil)
		// Clear the previous run's stall flag here (not only after a successful
		// cmd.Start): a run that never starts, e.g. a fork/exec failure right
		// after a stalled run, must not inherit stallKilled and be mislabelled
		// "stall" by classifyExit.
		ar.stallKilled.Store(false)
		runErr := m.runFFmpegOnce(ctx, ar, &snap, codec, restart)
		ran := time.Since(started)
		runBytes := ar.runBytes.Load()
		if ctx.Err() != nil {
			return // intentional stop
		}
		// Close the run record (flush the partial, record byte/frame/duration
		// counters) before the exit bookkeeping. The ctx-cancel path above
		// returns first; finalize closes the run there instead (endRun is
		// idempotent).
		m.endRun(ar)
		restart = true

		var werr error
		if p := ar.writeErr.Load(); p != nil {
			werr = *p
		}
		diskFull := werr != nil && isDiskFull(werr)
		killed := wasKilled(runErr) && !ar.stallKilled.Load() // our own escalation is not an OOM kill
		stallKilled := ar.stallKilled.Load()

		// Read the filtered stderr once (each call locks the buffer): the last
		// meaningful line, the last error-level line (preferred for diagnosis)
		// and how many benign lines were dropped this run.
		lastLine := ar.stderr.Last()
		errorLine := ar.stderr.LastErrorLine()
		suppressed := ar.stderr.suppressedRun()
		reason := classifyExit(runErr, werr, diskFull, killed, stallKilled, errorLine)

		// base mirrors LastError's human prefix so RecordLastError reads the same.
		var base string
		switch {
		case werr != nil:
			base = fmt.Sprintf("write to recording file failed: %v", werr)
		case runErr != nil:
			base = fmt.Sprintf("ffmpeg exited after %s: %v", ran.Truncate(time.Millisecond), runErr)
		default:
			base = fmt.Sprintf("ffmpeg exited after %s (stream ended)", ran.Truncate(time.Millisecond))
		}

		m.mu.Lock()
		ar.s.Restarts++
		if werr != nil {
			ar.s.WriteErrors++
		}
		ar.s.LastError = base
		if lastLine != "" && werr == nil {
			ar.s.LastError += " | " + lastLine
		}
		// RecordLastError survives the upload and prefers the error-level line.
		recDetail := errorLine
		if recDetail == "" {
			recDetail = lastLine
		}
		ar.s.RecordLastError = base
		if recDetail != "" && werr == nil {
			ar.s.RecordLastError += " | " + recDetail
		}
		ar.s.addExit(RunExit{
			At:         time.Now(),
			RanSeconds: ran.Seconds(),
			Bytes:      runBytes,
			Reason:     reason,
			ExitError:  firstNonEmptyErr(runErr, werr),
			Stderr:     lastLine,
			ErrorLine:  errorLine,
			Suppressed: suppressed,
		})
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
		m.met.FFmpegExitsTotal.WithLabelValues(reason).Inc()
		if werr != nil {
			m.met.WriteErrorsTotal.Inc()
		}
		if killed {
			m.met.FFmpegKilledTotal.Inc()
			log.Error("ffmpeg was killed by SIGKILL (OOM killer or external); restarting", "ran", ran.Truncate(time.Millisecond).String())
		}

		// Backoff: a run > 1 min counts as stable and resets the sequence, so the
		// first restart after a long healthy run waits only min (not the stale
		// accumulated value); disk-full forces a fixed 1 min instead.
		wait, next := nextBackoff(backoff, m.cfg.FFmpegRestartBackoffMin, m.cfg.FFmpegRestartBackoffMax, ran)
		// exitCode is the process status; runErr==nil means ffmpeg exited 0 (a
		// stream-ended/demux-error run), so report 0 rather than the -1 sentinel
		// that would otherwise read as a signal/unknown death.
		ec := exitCode(runErr)
		if runErr == nil {
			ec = 0
		}
		switch {
		case diskFull:
			wait = time.Minute
			log.Error("disk full while recording; retrying in 1m (recording continues into the same file when space frees up)",
				"error", werr, "bytes", ar.bytes.Load(), "reason", reason)
		case werr != nil:
			log.Error("recording file write failed; restarting ffmpeg", "error", werr, "backoff", wait.String(), "reason", reason)
		default:
			log.Warn("ffmpeg exited, restarting", "reason", reason, "error", runErr,
				"exitCode", ec, "ran", ran.Truncate(time.Millisecond).String(),
				"bytes", runBytes, "backoff", wait.String(), "errorLine", errorLine,
				"suppressed", suppressed, "stderr", lastLine)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		backoff = next
	}
}

// firstNonEmptyErr returns the message of the first non-nil error, or "".
func firstNonEmptyErr(errs ...error) string {
	for _, e := range errs {
		if e != nil {
			return e.Error()
		}
	}
	return ""
}

// copyRejectionMarkers are stderr fragments ffmpeg prints when the ADTS muxer
// or codec stage refuses a stream-copied input (as opposed to network errors).
var copyRejectionMarkers = []string{
	"could not write header", "adts", "only aac", "matches no streams",
	"unsupported codec", "not currently supported in container",
}

// benignStderrMarkers are stderr fragments that are PROVABLY harmless for this
// recorder and are dropped before they reach the tail, lastMessage or the log.
// Add an entry ONLY when the message is verified benign for both copy and
// transcode — otherwise a real error could be hidden.
//
//   - duplicate_moov: with fMP4/CMAF live HLS the hls demuxer re-fetches and
//     re-pushes init.mp4 before every segment (it compares init sections by
//     pointer and never resets them across reloads), so mov_read_moov warns
//     "Found duplicated MOOV Atom. Skipped it" once per ~5 s reload. Timing
//     comes from moof/tfdt, so the skipped second moov changes nothing; no
//     ffmpeg option suppresses it. It otherwise fills the 20-line tail, is
//     glued onto LastError and burns the 10/min log budget.
var benignStderrMarkers = []struct{ reason, substr string }{
	{"duplicate_moov", "Found duplicated MOOV Atom"},
}

// benignReason returns the marker reason when line is provably-harmless noise,
// or "" otherwise.
func benignReason(line string) string {
	for _, mk := range benignStderrMarkers {
		if strings.Contains(line, mk.substr) {
			return mk.reason
		}
	}
	return ""
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

// classifyExit maps one ffmpeg run's outcome to a stable reason label. Order of
// precedence: write errors first (disk-full before write-error), then stall, a
// SIGKILL we did not send, a non-zero exit, an exit-0 that trailed an "Error
// during demuxing" line (transient live-reload failure, fact 0.3), and finally
// a genuine stream end.
func classifyExit(runErr, werr error, diskFull, killed, stallKilled bool, errorLine string) (reason string) {
	switch {
	case diskFull:
		return "disk-full"
	case werr != nil:
		return "write-error"
	case stallKilled:
		return "stall"
	case killed:
		return "killed"
	case runErr != nil:
		return "exit-error"
	case strings.Contains(errorLine, "Error during demuxing"):
		return "demux-error"
	default:
		return "stream-ended"
	}
}

// nextBackoff computes the wait before the upcoming restart and the backoff to
// carry into the one after it. A run that lasted longer than a minute counts as
// stable, so the sequence restarts from min (the first restart after a long,
// healthy run waits only min); otherwise it doubles up to max. Pure so the
// sequence (long run → min; consecutive short runs → min,2·min,…,max,max) is
// unit-testable.
func nextBackoff(cur, min, max, ran time.Duration) (wait, next time.Duration) {
	if ran > time.Minute {
		cur = min
	}
	wait = cur
	next = cur * 2
	if next > max {
		next = max
	}
	return wait, next
}

// exitCode returns the process exit status from an *exec.ExitError, or -1 when
// the error is not an exit (signal, start failure, nil).
func exitCode(err error) int {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
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
	cmd.Stdout = &captureWriter{ar: ar, m: m}
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
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopWatch:
				return
			case <-t.C:
				// ffmpeg may have exited between ticks. A stall check that
				// coincides with a self-exit must not be counted as a stall: the
				// process is already gone and its real exit reason (e.g.
				// demux-error) must stand. exited is closed the instant Wait
				// returns, before stopWatch, and select picks randomly when both
				// it and the ticker are ready — so check it explicitly first.
				select {
				case <-exited:
					return
				default:
				}
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
	// Join the watchdog so ar.stallKilled is settled before supervise reads it
	// to classify this run: an unjoined late tick could otherwise flip the
	// classification between demux-error and stall.
	<-watchDone
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
