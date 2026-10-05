package recorder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spectado/stream-recorder/internal/config"
	"github.com/spectado/stream-recorder/internal/metrics"
	"github.com/spectado/stream-recorder/internal/schedule"
)

func TestWithSuffix(t *testing.T) {
	cases := map[[2]string]string{
		{"a/b.m4a", "-2"}:       "a/b-2.m4a",
		{"a/b", "_x"}:           "a/b_x",
		{"a.b/c", "-2"}:         "a.b/c-2",
		{"show.part1.m4a", "1"}: "show.part11.m4a",
	}
	for in, want := range cases {
		if got := withSuffix(in[0], in[1]); got != want {
			t.Errorf("withSuffix(%q,%q) = %q, want %q", in[0], in[1], got, want)
		}
	}
}

func TestDefaultKeys(t *testing.T) {
	// The date folder follows the SCHEDULED start in KEY_DATE_TZ, not the
	// session start: a show that starts at 23:30 CEST is a 21:30Z recording and
	// belongs to that day in UTC.
	s := &Session{SafeID: "radio-1", SessionID: "radio-1_20260829T212950Z",
		Start:        time.Date(2026, 8, 29, 23, 30, 0, 0, time.FixedZone("CEST", 2*3600)), // 21:30Z
		SessionStart: time.Date(2026, 8, 29, 21, 29, 50, 0, time.UTC)}
	if got, want := objectKey("archive/", time.UTC, s), "archive/2026-08-29/radio-1.m4a"; got != want {
		t.Fatalf("object key = %q want %q", got, want)
	}
	if got, want := objectKey("", time.UTC, s), "2026-08-29/radio-1.m4a"; got != want {
		t.Fatalf("object key without prefix = %q want %q", got, want)
	}
	// A nil location must never panic; it means UTC.
	if got, want := objectKey("", nil, s), "2026-08-29/radio-1.m4a"; got != want {
		t.Fatalf("object key without location = %q want %q", got, want)
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

// TestFFmpegArgsAudioOnlyGolden pins the complete command line of an audio-only
// run: RECORD_VIDEO must not change a single byte of it, and a video session
// without an active video output (no video in the source, capture turned off,
// probe failed) runs exactly the same command.
func TestFFmpegArgsAudioOnlyGolden(t *testing.T) {
	m := NewManager(&config.Config{DataDir: t.TempDir(), ProbeConcurrency: 1, UploadConcurrency: 1, SchedulePollInterval: time.Minute},
		slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("t"), nil, "t")
	m.cfg = &config.Config{FFmpegUserAgent: "ua", FFmpegRWTimeout: 10 * time.Second, AudioBitrate: "128k"}
	m.caps = FFmpegCapabilities{HLSExtensionPicky: true}
	golden := []string{
		"-nostdin", "-hide_banner", "-nostats", "-loglevel", "level+warning",
		"-threads", "1", "-filter_threads", "1", "-filter_complex_threads", "1",
		"-protocol_whitelist", "http,https,tcp,tls,crypto,httpproxy,data", "-user_agent", "ua", "-rw_timeout", "10000000",
		"-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_on_network_error", "1",
		"-reconnect_on_http_error", "4xx,5xx", "-reconnect_delay_max", "5",
		"-icy", "0", "-seg_max_retry", "3", "-extension_picky", "0",
		"-i", "http://x/live.m3u8", "-vn", "-sn", "-dn", "-map", "0:a:0", "-c:a", "aac", "-b:a", "128k",
		"-f", "adts", "-flush_packets", "1", "pipe:1",
	}
	for _, s := range []*Session{
		{Source: "http://x/live.m3u8"},
		{Source: "http://x/live.m3u8", Video: true, VideoSource: "none"},
		{Source: "http://x/live.m3u8", Video: true, VideoSource: "copy", VideoMap: "0:v:0"},
	} {
		if got := m.captureArgs(s, "aac", false, false, videoPlan{}); !slices.Equal(got, golden) {
			t.Errorf("video=%v source=%q:\n got %q\nwant %q", s.Video, s.VideoSource, got, golden)
		}
	}
}

// TestCaptureArgsVideoOutput checks the second (video companion) output: the
// first output is left exactly as in an audio-only run, and the second one
// carries the chosen video plus the same audio, encoded like the ADTS output.
func TestCaptureArgsVideoOutput(t *testing.T) {
	m := NewManager(&config.Config{DataDir: t.TempDir(), ProbeConcurrency: 1, UploadConcurrency: 1, SchedulePollInterval: time.Minute},
		slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("t"), nil, "t")
	m.cfg = &config.Config{FFmpegUserAgent: "ua", FFmpegRWTimeout: 10 * time.Second, AudioBitrate: "128k"}
	s := &Session{Source: "http://x/live.m3u8", Video: true, Bitrate: "96k"}
	cases := []struct {
		codec string
		plan  videoPlan
		want  []string
	}{
		{"copy", videoPlan{source: "copy", vmap: "0:p:2:v:0", path: "/d/s.v0001.ts"}, []string{
			"-map", "0:p:2:v:0", "-map", "0:a:0", "-sn", "-dn", "-c:v", "copy", "-c:a", "copy",
			"-f", "mpegts", "-flush_packets", "1", "-y", "file:/d/s.v0001.ts"}},
		{"aac", videoPlan{source: "x264", vmap: "0:v:0", path: "/d/s.v0002.ts"}, []string{
			"-map", "0:v:0", "-map", "0:a:0", "-sn", "-dn",
			"-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "96k",
			"-f", "mpegts", "-flush_packets", "1", "-y", "file:/d/s.v0002.ts"}},
	}
	for _, tc := range cases {
		first := m.ffmpegArgs(s, tc.codec, true, false)
		got := m.captureArgs(s, tc.codec, true, false, tc.plan)
		if !slices.Equal(got[:len(first)], first) {
			t.Fatalf("first output changed:\n got %q\nwant %q", got[:len(first)], first)
		}
		if first[len(first)-1] != "pipe:1" {
			t.Fatalf("ADTS output must stay first and end in pipe:1: %q", first)
		}
		if rest := got[len(first):]; !slices.Equal(rest, tc.want) {
			t.Errorf("video output (%s):\n got %q\nwant %q", tc.plan.source, rest, tc.want)
		}
	}
}

// hlsMasterProbe is real ffprobe output (ffmpeg 9.0.1) for a three-variant HLS
// master whose highest-bandwidth variant (640x360) is listed second.
const hlsMasterProbe = `{"programs": [
 {"program_id": 0, "tags": {"variant_bitrate": "280872"}, "streams": [{"index": 0, "codec_type": "video"}, {"index": 1, "codec_type": "audio"}]},
 {"program_id": 1, "tags": {"variant_bitrate": "1990920"}, "streams": [{"index": 2, "codec_type": "video"}, {"index": 3, "codec_type": "audio"}]},
 {"program_id": 2, "tags": {"variant_bitrate": "627168"}, "streams": [{"index": 4, "codec_type": "video"}, {"index": 5, "codec_type": "audio"}]}],
 "streams": [
 {"index": 0, "codec_name": "h264", "profile": "Constrained Baseline", "codec_type": "video", "width": 160, "height": 90},
 {"index": 1, "codec_name": "aac", "profile": "LC", "codec_type": "audio", "sample_rate": "48000", "channels": 1},
 {"index": 2, "codec_name": "h264", "profile": "Constrained Baseline", "codec_type": "video", "width": 640, "height": 360},
 {"index": 3, "codec_name": "aac", "profile": "LC", "codec_type": "audio", "sample_rate": "48000", "channels": 1},
 {"index": 4, "codec_name": "h264", "profile": "Constrained Baseline", "codec_type": "video", "width": 320, "height": 180},
 {"index": 5, "codec_name": "aac", "profile": "LC", "codec_type": "audio", "sample_rate": "48000", "channels": 1}]}`

func TestDecideVideo(t *testing.T) {
	cases := []struct {
		name, json   string
		source, vmap string
	}{
		{"hls master: highest variant_bitrate wins", hlsMasterProbe, "copy", "0:p:1:v:0"},
		{"audio only", `{"programs":[{"program_id":0,"tags":{"variant_bitrate":"64000"},"streams":[{"index":0,"codec_type":"audio"}]}],
			"streams":[{"index":0,"codec_type":"audio","codec_name":"aac"}]}`, "none", ""},
		{"no programs, hevc", `{"streams":[{"index":0,"codec_type":"audio","codec_name":"aac"},{"index":1,"codec_type":"video","codec_name":"hevc"}]}`,
			"copy", "0:v:0"},
		{"single program mpegts, mpeg2 needs x264", `{"programs":[{"program_id":1,"streams":[{"index":0,"codec_type":"video"},{"index":1,"codec_type":"audio"}]}],
			"streams":[{"index":0,"codec_type":"video","codec_name":"mpeg2video"},{"index":1,"codec_type":"audio","codec_name":"mp2"}]}`, "x264", "0:v:0"},
		{"cover art is not video", `{"streams":[{"index":0,"codec_type":"audio","codec_name":"mp3"},
			{"index":1,"codec_type":"video","codec_name":"mjpeg","disposition":{"attached_pic":1}}]}`, "none", ""},
		{"tie: first listed program", `{"programs":[
			{"program_id":7,"tags":{"variant_bitrate":"500"},"streams":[{"index":0,"codec_type":"video"}]},
			{"program_id":3,"tags":{"variant_bitrate":"500"},"streams":[{"index":1,"codec_type":"video"}]}],
			"streams":[{"index":0,"codec_type":"video","codec_name":"vp9"},{"index":1,"codec_type":"video","codec_name":"h264"}]}`, "x264", "0:p:7:v:0"},
	}
	for _, tc := range cases {
		var r probeResult
		if err := json.Unmarshal([]byte(tc.json), &r); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		source, vmap := decideVideo(&r)
		if source != tc.source || vmap != tc.vmap {
			t.Errorf("%s: got (%q, %q), want (%q, %q)", tc.name, source, vmap, tc.source, tc.vmap)
		}
	}
	var r probeResult
	_ = json.Unmarshal([]byte(hlsMasterProbe), &r)
	if a, ok := r.firstAudio(); !ok || a.Index != 1 || a.CodecName != "aac" || a.Profile != "LC" {
		t.Errorf("firstAudio = %+v %v, want stream 1 (what -map 0:a:0 picks)", a, ok)
	}
}

func TestSessionVideoHelpers(t *testing.T) {
	dir := t.TempDir()
	s := &Session{SessionID: "show_20261005T100000Z", dir: dir, Source: "http://a", VideoSource: "copy", VideoMap: "0:v:0"}
	if got := s.videoFileName(7); got != "show_20261005T100000Z.v0007.ts" {
		t.Errorf("videoFileName = %q", got)
	}
	if got := videoKey("2026-10-05/show.m4a"); got != "2026-10-05/show.mp4" {
		t.Errorf("videoKey = %q", got)
	}
	if got := s.OutputVideoPath(); got != filepath.Join(dir, "show_20261005T100000Z.mp4") {
		t.Errorf("OutputVideoPath = %q", got)
	}
	s.Runs = []Run{{VideoFiles: []string{"a.ts"}}, {}, {VideoFiles: []string{"b.ts", "c.ts"}}}
	want := []string{filepath.Join(dir, "a.ts"), filepath.Join(dir, "b.ts"), filepath.Join(dir, "c.ts")}
	if got := s.VideoFilesAll(); !slices.Equal(got, want) {
		t.Errorf("VideoFilesAll = %q", got)
	}

	// Coalescing the two oldest runs keeps their TS files, in order.
	s.Runs = make([]Run, maxRuns)
	s.Runs[0].VideoFiles = []string{"x.v0001.ts"}
	s.Runs[1].VideoFiles = []string{"x.v0002.ts"}
	s.appendRun(Run{VideoFiles: []string{"x.v2001.ts"}})
	if got := s.Runs[0].VideoFiles; !slices.Equal(got, []string{"x.v0001.ts", "x.v0002.ts"}) {
		t.Errorf("coalesced VideoFiles = %q", got)
	}

	// A source change forgets the video decision along with the codec.
	s.applyItem(schedule.Item{Source: "http://b"})
	if s.VideoSource != "" || s.VideoMap != "" {
		t.Errorf("source change kept the video decision: %q %q", s.VideoSource, s.VideoMap)
	}

	// removeVideoFiles also finds TS files no run lists (a run without audio).
	s.Runs = []Run{{VideoFiles: []string{s.videoFileName(1)}}}
	for _, n := range []string{s.videoFileName(1), s.videoFileName(2), "show_20261005T100000Z-2.v0001.ts", "show_20261005T100000Z.aac"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	s.removeVideoFiles()
	left, _ := filepath.Glob(filepath.Join(dir, "*"))
	for i := range left {
		left[i] = filepath.Base(left[i])
	}
	slices.Sort(left)
	if !slices.Equal(left, []string{"show_20261005T100000Z-2.v0001.ts", "show_20261005T100000Z.aac"}) {
		t.Errorf("after removeVideoFiles: %q", left)
	}
}

// realFFmpeg returns the ffmpeg/ffprobe binaries, skipping the test when they
// (or libx264, needed to generate video) are missing.
func realFFmpeg(t *testing.T) (ffmpeg, ffprobe string) {
	t.Helper()
	var err error
	ffmpeg, err = exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not on PATH")
	}
	ffprobe, err = exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe not on PATH")
	}
	if !DetectFFmpegCapabilities(context.Background(), ffmpeg).LibX264 {
		t.Skip("ffmpeg has no libx264")
	}
	return ffmpeg, ffprobe
}

// TestVideoCaptureMultiVariantHLS drives the real binaries against a local
// three-variant HLS master (variants listed lowest bandwidth first, so the
// audio ffmpeg's -map 0:a:0 picks is NOT the variant the video comes from):
//   - one ffprobe call resolves both the codec and the video decision;
//   - the video comes from the highest-bandwidth variant and, apart from the
//     probing reads of each variant's first segment, ffmpeg only fetches the
//     segments of the variants it maps;
//   - the ADTS on stdout is byte-identical to an audio-only run;
//   - the TS file holds h264 640x360 plus the audio.
func TestVideoCaptureMultiVariantHLS(t *testing.T) {
	ffmpeg, ffprobe := realFFmpeg(t)
	dir := t.TempDir()
	hls := filepath.Join(dir, "hls")
	if err := os.MkdirAll(hls, 0o755); err != nil {
		t.Fatal(err)
	}
	gen := exec.Command(ffmpeg, "-nostdin", "-hide_banner", "-v", "error",
		"-f", "lavfi", "-i", "testsrc2=s=640x360:r=25:d=24", "-f", "lavfi", "-i", "sine=f=440:d=24:sample_rate=48000",
		"-filter_complex", "[0:v]split=3[a][b][c];[b]scale=320:180[b2];[c]scale=160:90[c2];[1:a]asplit=3[a1][a2][a3]",
		"-map", "[a]", "-map", "[a1]", "-map", "[b2]", "-map", "[a2]", "-map", "[c2]", "-map", "[a3]",
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "50", "-b:v:0", "1500k", "-b:v:1", "400k", "-b:v:2", "100k",
		"-c:a", "aac", "-b:a", "96k",
		"-f", "hls", "-hls_time", "4", "-hls_list_size", "0", "-hls_playlist_type", "vod",
		"-var_stream_map", "v:0,a:0 v:1,a:1 v:2,a:2", "-hls_segment_filename", filepath.Join(hls, "v%v_%03d.ts"),
		filepath.Join(hls, "v%v.m3u8"))
	if out, err := gen.CombinedOutput(); err != nil {
		t.Fatalf("generate hls: %v\n%s", err, out)
	}
	master := "#EXTM3U\n#EXT-X-VERSION:3\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=280000,RESOLUTION=160x90\nv2.m3u8\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=1990000,RESOLUTION=640x360\nv0.m3u8\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=627000,RESOLUTION=320x180\nv1.m3u8\n"
	if err := os.WriteFile(filepath.Join(hls, "master.m3u8"), []byte(master), 0o644); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	gets := map[string]int{}
	fs := http.FileServer(http.Dir(hls))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gets[r.URL.Path]++
		mu.Unlock()
		fs.ServeHTTP(w, r)
	}))
	defer srv.Close()
	resetGets := func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		out := gets
		gets = map[string]int{}
		return out
	}

	cfg := testConfig(dir, ffmpeg)
	cfg.FFprobePath = ffprobe
	cfg.AudioCodec = "auto"
	m := NewManager(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("t"), nil, "t")
	s := &Session{ID: "v", SessionID: "v_1", Source: srv.URL + "/master.m3u8", Codec: "auto", Video: true, dir: dir}
	ar := &activeRecording{s: s}
	snap := *s
	var mp mediaProbe
	ctx := context.Background()
	codec := m.resolveCodec(ctx, ar, &snap, &mp)
	source, vmap := m.resolveVideo(ctx, ar, &snap, &mp)
	if codec != "copy" || source != "copy" || vmap != "0:p:1:v:0" {
		t.Fatalf("resolved codec=%q video=%q map=%q, want copy/copy/0:p:1:v:0", codec, source, vmap)
	}
	if s.ResolvedCodec != "copy" || s.VideoSource != "copy" || s.VideoMap != "0:p:1:v:0" {
		t.Fatalf("decision not persisted on the session: %+v", s)
	}
	if gets := resetGets(); gets["/master.m3u8"] != 1 {
		t.Errorf("want exactly one probe of the master, got %v", gets)
	}

	run := func(args []string) []byte {
		t.Helper()
		var stdout, stderr bytes.Buffer
		cmd := exec.Command(ffmpeg, args...)
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("ffmpeg: %v\n%s", err, stderr.String())
		}
		return stdout.Bytes()
	}
	audioOnly := run(m.captureArgs(s, codec, false, false, videoPlan{}))
	resetGets()
	ts := filepath.Join(dir, s.videoFileName(1))
	withVideo := run(m.captureArgs(s, codec, false, false, videoPlan{source: source, vmap: vmap, path: ts}))
	fetched := resetGets()
	if len(audioOnly) == 0 || !bytes.Equal(audioOnly, withVideo) {
		t.Errorf("ADTS output differs with the video output (%d vs %d bytes)", len(audioOnly), len(withVideo))
	}
	// Segments: v2 (first listed: the audio of -map 0:a:0) and v0 (the video
	// variant) entirely; v1 only what stream probing reads before ffmpeg
	// discards the unused variant (its first one or two segments).
	count := func(prefix string) (n int) {
		for p, c := range fetched {
			if strings.HasPrefix(p, prefix) && strings.HasSuffix(p, ".ts") {
				n += c
			}
		}
		return n
	}
	if count("/v0_") != 6 || count("/v2_") != 6 || count("/v1_") > 2 {
		t.Errorf("segment fetches %v: want all 6 of v0 and v2, at most 2 of v1", fetched)
	}

	out, err := exec.Command(ffprobe, "-v", "error", "-count_packets", "-show_entries",
		"stream=codec_type,codec_name,width,height,nb_read_packets", "-of", "json", ts).Output()
	if err != nil {
		t.Fatalf("ffprobe ts: %v", err)
	}
	var pr probeResult
	if err := json.Unmarshal(out, &pr); err != nil {
		t.Fatal(err)
	}
	var v, a probeStream
	for _, st := range pr.Streams {
		switch st.CodecType {
		case "video":
			v = st
		case "audio":
			a = st
		}
	}
	if v.CodecName != "h264" || v.Width != 640 || v.Height != 360 || a.CodecName != "aac" {
		t.Errorf("TS streams: video %+v audio %+v", v, a)
	}
}

// TestVideoProbeAudioOnlySource: a video session on an audio-only source
// resolves to "none" (black at upload) and its runs have no video output.
func TestVideoProbeAudioOnlySource(t *testing.T) {
	ffmpeg, ffprobe := realFFmpeg(t)
	dir := t.TempDir()
	gen := exec.Command(ffmpeg, "-nostdin", "-hide_banner", "-v", "error", "-f", "lavfi", "-i", "sine=f=440:d=8:sample_rate=48000",
		"-c:a", "aac", "-f", "hls", "-hls_time", "2", "-hls_list_size", "0", "-hls_playlist_type", "vod", filepath.Join(dir, "a.m3u8"))
	if out, err := gen.CombinedOutput(); err != nil {
		t.Fatalf("generate hls: %v\n%s", err, out)
	}
	srv := httptest.NewServer(http.FileServer(http.Dir(dir)))
	defer srv.Close()
	cfg := testConfig(dir, ffmpeg)
	cfg.FFprobePath = ffprobe
	m := NewManager(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("t"), nil, "t")
	s := &Session{ID: "a", SessionID: "a_1", Source: srv.URL + "/a.m3u8", Codec: "auto", Video: true, dir: dir}
	snap := *s
	var mp mediaProbe
	source, vmap := m.resolveVideo(context.Background(), &activeRecording{s: s}, &snap, &mp)
	if source != "none" || vmap != "" || s.VideoSource != "none" {
		t.Fatalf("audio-only source: got (%q, %q), session %q", source, vmap, s.VideoSource)
	}
}
