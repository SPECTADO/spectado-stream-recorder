package recorder

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/spectado/stream-recorder/internal/config"
	"github.com/spectado/stream-recorder/internal/metrics"
	"github.com/spectado/stream-recorder/internal/schedule"
)

func TestWithSuffix(t *testing.T) {
	cases := map[[2]string]string{
		{"a/b.aac", "_x"}:       "a/b_x.aac",
		{"a/b", "_x"}:           "a/b_x",
		{"a.b/c", "-2"}:         "a.b/c-2",
		{"show.part1.aac", "1"}: "show.part11.aac",
	}
	for in, want := range cases {
		if got := withSuffix(in[0], in[1]); got != want {
			t.Errorf("withSuffix(%q,%q) = %q, want %q", in[0], in[1], got, want)
		}
	}
}

func TestDefaultKeys(t *testing.T) {
	s := &Session{SafeID: "radio-1", SessionID: "radio-1_20260829T212950Z",
		Start:        time.Date(2026, 8, 29, 23, 30, 0, 0, time.FixedZone("CEST", 2*3600)), // 21:30Z
		SessionStart: time.Date(2026, 8, 29, 21, 29, 50, 0, time.UTC)}
	folder := defaultFolder("archive/", s)
	if want := "archive/2026-08-29/radio-1/"; folder != want {
		t.Fatalf("folder = %q want %q", folder, want)
	}
	key := mediaKey(folder, s)
	if want := "archive/2026-08-29/radio-1/radio-1_20260829T212950Z.aac"; key != want {
		t.Fatalf("media key = %q want %q", key, want)
	}
	if got, want := playlistKey(key), "archive/2026-08-29/radio-1/index.m3u8"; got != want {
		t.Fatalf("playlist key = %q want %q", got, want)
	}
	if got, want := mediaKey(defaultFolder("", s), s), "2026-08-29/radio-1/radio-1_20260829T212950Z.aac"; got != want {
		t.Fatalf("media key without prefix = %q want %q", got, want)
	}
}

func TestRedactURL(t *testing.T) {
	cases := map[string]string{
		"https://u:p@h/x.m3u8?token=abc&b=2": "https://REDACTED:REDACTED@h/x.m3u8?b=REDACTED&token=REDACTED",
		"http://h/plain":                     "http://h/plain",
		"https://u@h/":                       "https://REDACTED@h/",
		"not a url":                          "not a url",
		"/relative/path?x=1":                 "/relative/path?x=1",
	}
	for in, want := range cases {
		if got := RedactURL(in); got != want {
			t.Errorf("RedactURL(%q) = %q, want %q", in, got, want)
		}
	}
	line := `[hls @ 0x1] Opening 'https://cdn/seg1.ts?sig=SECRET' for reading`
	if out := redactLine(line); strings.Contains(out, "SECRET") {
		t.Fatalf("not redacted: %s", out)
	}
}

func TestParseLevel(t *testing.T) {
	lvl, msg := parseLevel("[hls @ 0x55] [warning] skipping 2 segments")
	if lvl != slog.LevelWarn || msg != "[hls @ 0x55] skipping 2 segments" {
		t.Fatalf("got %v %q", lvl, msg)
	}
	lvl, msg = parseLevel("[error] Server returned 404 Not Found")
	if lvl != slog.LevelError || msg != "Server returned 404 Not Found" {
		t.Fatalf("got %v %q", lvl, msg)
	}
	lvl, msg = parseLevel("untagged line")
	if lvl != slog.LevelWarn || msg != "untagged line" {
		t.Fatalf("got %v %q", lvl, msg)
	}
}

func TestStderrBufferTailAndRateLimit(t *testing.T) {
	var logged []string
	b := newStderrBuffer("warn", func(_ slog.Level, l string) { logged = append(logged, l) })
	for i := 0; i < 50; i++ {
		_, _ = b.Write([]byte("[warning] line " + strings.Repeat("x", i) + "\n"))
	}
	_, _ = b.Write([]byte("[info] not logged in warn mode\npartial"))
	if tail := b.Tail(); len(tail) != 20 || !strings.HasSuffix(tail[19], "not logged in warn mode") {
		t.Fatalf("tail=%v", tail)
	}
	if len(logged) != b.perMinute {
		t.Fatalf("logged %d lines, want %d (rate limit)", len(logged), b.perMinute)
	}
	if b.Last() != "not logged in warn mode" {
		t.Fatalf("last=%q", b.Last())
	}
	// Split writes across a line boundary.
	b2 := newStderrBuffer("off", nil)
	_, _ = b2.Write([]byte("abc"))
	_, _ = b2.Write([]byte("def\nsecond\r\n"))
	if tail := b2.Tail(); len(tail) != 2 || !strings.HasSuffix(tail[0], "abcdef") || !strings.HasSuffix(tail[1], "second") {
		t.Fatalf("tail=%v", tail)
	}
}

func TestStderrBufferBenignFilterAndErrorLine(t *testing.T) {
	var logged []string
	var suppressedReasons []string
	b := newStderrBuffer("warn", func(_ slog.Level, l string) { logged = append(logged, l) })
	b.onSuppressed = func(reason string) { suppressedReasons = append(suppressedReasons, reason) }

	// A benign MOOV line (with an ffmpeg level tag) is dropped: not in the tail,
	// not in Last, counted, and the hook fires with the reason.
	_, _ = b.Write([]byte("[mov,mp4,m4a @ 0x1] [warning] Found duplicated MOOV Atom. Skipped it\n"))
	// A real error line is kept and remembered as the run's error line.
	_, _ = b.Write([]byte("[error] Error during demuxing: Server returned 500\n"))
	// Another benign line after the error must not overwrite Last/LastErrorLine.
	_, _ = b.Write([]byte("[warning] Found duplicated MOOV Atom. Skipped it\n"))

	for _, l := range b.Tail() {
		if strings.Contains(l, "MOOV") {
			t.Fatalf("benign line leaked into tail: %v", b.Tail())
		}
	}
	if got := b.Last(); got != "Error during demuxing: Server returned 500" {
		t.Fatalf("Last()=%q", got)
	}
	if got := b.LastErrorLine(); got != "Error during demuxing: Server returned 500" {
		t.Fatalf("LastErrorLine()=%q", got)
	}
	if got := b.suppressedRun(); got != 2 {
		t.Fatalf("suppressedRun()=%d, want 2", got)
	}
	if len(suppressedReasons) != 2 || suppressedReasons[0] != "duplicate_moov" {
		t.Fatalf("hook reasons=%v", suppressedReasons)
	}
	// Benign lines are not logged in warn mode; the error line is (within budget).
	for _, l := range logged {
		if strings.Contains(l, "MOOV") {
			t.Fatalf("benign line was logged in warn mode: %v", logged)
		}
	}

	// resetRun clears the per-run counter and error line but keeps the tail/Last.
	b.resetRun()
	if b.suppressedRun() != 0 || b.LastErrorLine() != "" {
		t.Fatalf("resetRun did not clear: suppressed=%d errorLine=%q", b.suppressedRun(), b.LastErrorLine())
	}
	if b.Last() != "Error during demuxing: Server returned 500" {
		t.Fatalf("resetRun must not clear Last(): %q", b.Last())
	}

	// In debug mode a benign line is still logged (at Debug) but stays out of tail/Last.
	var dbg []string
	d := newStderrBuffer("debug", func(_ slog.Level, l string) { dbg = append(dbg, l) })
	_, _ = d.Write([]byte("[warning] Found duplicated MOOV Atom. Skipped it\n"))
	if len(dbg) != 1 || !strings.Contains(dbg[0], "MOOV") {
		t.Fatalf("debug mode should log the benign line: %v", dbg)
	}
	if len(d.Tail()) != 0 || d.Last() != "" {
		t.Fatalf("debug mode must keep benign lines out of tail/Last: tail=%v last=%q", d.Tail(), d.Last())
	}
	if d.suppressedRun() != 1 {
		t.Fatalf("debug mode must still count suppressed: %d", d.suppressedRun())
	}
}

func TestClassifyExit(t *testing.T) {
	exit := errors.New("exit status 1")
	write := errors.New("no space")
	cases := []struct {
		name                          string
		runErr, werr                  error
		diskFull, killed, stallKilled bool
		errorLine                     string
		want                          string
	}{
		{"disk-full wins", exit, write, true, false, false, "", "disk-full"},
		{"write-error", exit, write, false, false, false, "", "write-error"},
		{"stall beats killed", exit, nil, false, true, true, "", "stall"},
		{"killed not ours", exit, nil, false, true, false, "", "killed"},
		{"exit-error beats demux", exit, nil, false, false, false, "Error during demuxing: x", "exit-error"},
		{"demux-error on exit 0", nil, nil, false, false, false, "[some] Error during demuxing: x", "demux-error"},
		{"stream-ended", nil, nil, false, false, false, "", "stream-ended"},
		{"stream-ended ignores non-demux error line", nil, nil, false, false, false, "unrelated error", "stream-ended"},
	}
	for _, c := range cases {
		if got := classifyExit(c.runErr, c.werr, c.diskFull, c.killed, c.stallKilled, c.errorLine); got != c.want {
			t.Errorf("%s: classifyExit = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestNextBackoff(t *testing.T) {
	min, max := time.Second, 30*time.Second
	// Consecutive short runs double up to the cap: 1,2,4,8,16,30,30.
	cur := min
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30}
	for i, w := range want {
		wait, next := nextBackoff(cur, min, max, time.Second /* short run */)
		if wait != time.Duration(w)*time.Second {
			t.Fatalf("step %d: wait=%s want %ds", i, wait, w)
		}
		cur = next
	}
	// A run longer than a minute resets the sequence to min regardless of cur.
	wait, next := nextBackoff(max, min, max, 2*time.Minute)
	if wait != min || next != 2*time.Second {
		t.Fatalf("long run reset: wait=%s next=%s", wait, next)
	}
}

func TestExitReasonsSummary(t *testing.T) {
	if got := exitReasonsSummary(nil); got != "" {
		t.Fatalf("empty exits should yield empty string, got %q", got)
	}
	exits := []RunExit{
		{Reason: "stream-ended"}, {Reason: "demux-error"}, {Reason: "demux-error"}, {Reason: "demux-error"},
	}
	if got := exitReasonsSummary(exits); got != "demux-error=3,stream-ended=1" {
		t.Fatalf("summary=%q", got)
	}
}

func TestSessionAddExitBounded(t *testing.T) {
	s := &Session{}
	for i := 0; i < maxExits+5; i++ {
		s.addExit(RunExit{Bytes: int64(i)})
	}
	if len(s.Exits) != maxExits {
		t.Fatalf("len(Exits)=%d, want %d", len(s.Exits), maxExits)
	}
	// Oldest dropped: the first kept entry is index 5 (Bytes==5).
	if s.Exits[0].Bytes != 5 {
		t.Fatalf("oldest not dropped: first Bytes=%d", s.Exits[0].Bytes)
	}
}

func TestIsHLS(t *testing.T) {
	if !isHLS(&Session{Source: "https://x/live/playlist.M3U8?x=1"}) {
		t.Error("m3u8 should be HLS")
	}
	if isHLS(&Session{Source: "https://x/stream", Type: ""}) {
		t.Error("plain url is not HLS")
	}
	if !isHLS(&Session{Source: "https://x/stream", Type: "hls"}) {
		t.Error("type hls wins")
	}
	if isHLS(&Session{Source: "https://x/a.m3u8", Type: "icecast"}) {
		t.Error("type icecast wins")
	}
}

func TestFFmpegArgs(t *testing.T) {
	cfg := &config.Config{FFmpegUserAgent: "ua", FFmpegRWTimeout: 10 * time.Second, AudioBitrate: "128k"}
	m := NewManager(&config.Config{DataDir: t.TempDir(), ProbeConcurrency: 1, UploadConcurrency: 1, SchedulePollInterval: time.Minute},
		slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("t"), nil, "t")
	m.cfg = cfg
	m.caps = FFmpegCapabilities{HLSExtensionPicky: true, CAFile: "/etc/ssl/cert.pem"}
	s := &Session{Source: "https://x/live.m3u8", Headers: map[string]string{"X-Key": "v"}, Bitrate: "96k"}

	args := strings.Join(m.ffmpegArgs(s, "aac", false, false), " ")
	for _, want := range []string{
		"-nostdin", "-loglevel level+warning", "-threads 1", "-filter_threads 1", "-filter_complex_threads 1",
		"-protocol_whitelist http,https,tcp,tls,crypto,httpproxy,data", "-user_agent ua", "-rw_timeout 10000000",
		"-reconnect 1", "-icy 0", "-tls_verify 0", "-seg_max_retry 3", "-extension_picky 0",
		"-headers X-Key: v\r\n", "-i https://x/live.m3u8", "-map 0:a:0", "-c:a aac -b:a 96k", "-f adts -flush_packets 1 pipe:1",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("args missing %q:\n%s", want, args)
		}
	}
	if strings.Contains(args, "-live_start_index") {
		t.Error("first run must keep ffmpeg's default live start (pre-roll)")
	}
	restart := strings.Join(m.ffmpegArgs(s, "copy", true, false), " ")
	if !strings.Contains(restart, "-live_start_index -1") || !strings.Contains(restart, "-c:a copy") {
		t.Errorf("restart args: %s", restart)
	}
	cfg.FFmpegTLSVerify = true
	verify := strings.Join(m.ffmpegArgs(s, "aac", false, false), " ")
	if !strings.Contains(verify, "-tls_verify 1 -ca_file /etc/ssl/cert.pem") {
		t.Errorf("tls verify args: %s", verify)
	}
	s.InsecureTLS = true
	if v := strings.Join(m.ffmpegArgs(s, "aac", false, false), " "); !strings.Contains(v, "-tls_verify 0") {
		t.Errorf("insecureTLS override: %s", v)
	}
	plain := &Session{Source: "http://cdn/live.m3u8"}
	if a := strings.Join(m.ffmpegArgs(plain, "aac", false, false), " "); strings.Contains(a, "tls_verify") || strings.Contains(a, "ca_file") {
		t.Errorf("plain http must not get tls options (ffmpeg fails on unused input options): %s", a)
	}
	safe := strings.Join(m.ffmpegArgs(s, "aac", true, true), " ")
	for _, bad := range []string{"seg_max_retry", "live_start_index", "extension_picky", "-icy"} {
		if strings.Contains(safe, bad) {
			t.Errorf("safe args must not contain %q: %s", bad, safe)
		}
	}
	// TLS options are always valid for https, so safe mode keeps them (never silently drop verification).
	for _, want := range []string{"-headers X-Key: v", "-reconnect 1", "-rw_timeout 10000000", "-protocol_whitelist", "-tls_verify"} {
		if !strings.Contains(safe, want) {
			t.Errorf("safe args missing %q: %s", want, safe)
		}
	}
	ice := &Session{Source: "https://ice/stream", Type: "icecast"}
	if a := strings.Join(m.ffmpegArgs(ice, "aac", true, false), " "); strings.Contains(a, "seg_max_retry") || strings.Contains(a, "live_start_index") {
		t.Errorf("icecast must not get hls options: %s", a)
	}
}

func TestCopyableProfile(t *testing.T) {
	for p, want := range map[string]bool{"LC": true, "HE-AAC": true, "HE-AACv2": true, "": true, "xHE-AAC": false, "USAC": false} {
		if copyableProfile(p) != want {
			t.Errorf("copyableProfile(%q) = %v", p, !want)
		}
	}
}

func TestApplyItem(t *testing.T) {
	s := &Session{Source: "http://a", Codec: "auto", ResolvedCodec: "copy"}
	d := schedule.Duration(15 * time.Second)
	changed := s.applyItem(schedule.Item{Source: "http://b", Codec: "", Bitrate: "64k", StopLate: &d})
	if !changed || s.ResolvedCodec != "" || s.Codec != "auto" || s.Bitrate != "64k" || s.effectiveStopLate(time.Minute) != 15*time.Second {
		t.Fatalf("after applyItem: %+v changed=%v", s, changed)
	}
	if s.applyItem(schedule.Item{Source: "http://b"}) {
		t.Fatal("same source must not report a change")
	}
	if s.effectiveStopLate(time.Minute) != time.Minute {
		t.Fatal("override removed -> default applies")
	}
}
