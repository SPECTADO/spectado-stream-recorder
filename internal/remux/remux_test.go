package remux

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spectado/stream-recorder/internal/adts"
	"github.com/spectado/stream-recorder/internal/id3"
)

// The tests drive the real binaries: this package is a thin wrapper around
// ffmpeg, so a test with a fake ffmpeg would only prove that the wrapper calls
// itself. Everything is generated from lavfi sine into t.TempDir(); the
// expectations come from the ADTS scanner, which counts frame headers and is
// the recorder's own source of truth.

func tools(t *testing.T) *Remuxer {
	t.Helper()
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not on PATH")
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe not on PATH")
	}
	return &Remuxer{FFmpegPath: ffmpeg, FFprobePath: ffprobe}
}

// sineADTS writes seconds of a 440 Hz sine as raw ADTS (AAC-LC) and returns the
// path.
func sineADTS(t *testing.T, dir, name string, sampleRate, channels int, seconds float64, bitrate string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	args := []string{
		"-nostdin", "-hide_banner", "-v", "error", "-y",
		"-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=440:sample_rate=%d:duration=%g", sampleRate, seconds),
		"-ac", fmt.Sprint(channels), "-c:a", "aac", "-b:a", bitrate, "-f", "adts", path,
	}
	out, err := exec.Command("ffmpeg", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("generate %s: %v\n%s", name, err, out)
	}
	return path
}

// sampleRateIndexes is the ADTS sampling_frequency_index table, needed to build
// the Params a caller would hand to Build.
var sampleRateIndexes = map[int]int{96000: 0, 88200: 1, 64000: 2, 48000: 3, 44100: 4, 32000: 5, 24000: 6, 22050: 7, 16000: 8, 12000: 9, 11025: 10, 8000: 11, 7350: 12}

func paramsFor(t *testing.T, sampleRate, channels int) adts.Params {
	t.Helper()
	idx, ok := sampleRateIndexes[sampleRate]
	if !ok {
		t.Fatalf("no sampling_frequency_index for %d Hz", sampleRate)
	}
	return adts.Params{Profile: 1, SampleRateIndex: idx, SampleRate: sampleRate, ChannelConfig: channels, Blocks: 1}
}

func scan(t *testing.T, path string) adts.Info {
	t.Helper()
	info, err := adts.Scan(path)
	if err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	if info.Frames == 0 {
		t.Fatalf("scan %s: no frames", path)
	}
	return info
}

func mustNotExist(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s still exists (stat err %v)", p, err)
		}
	}
}

func abs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// runTable is the kind of JSON the recorder puts into the description tag: a
// few hundred bytes with quotes and colons, which must survive the ilst atom.
func runTable() string {
	type entry struct {
		SID string  `json:"sid"`
		T   string  `json:"t"`
		Src string  `json:"src"`
		Off float64 `json:"off"`
		Dur float64 `json:"dur"`
	}
	var entries []entry
	for i := range 6 {
		entries = append(entries, entry{
			SID: fmt.Sprintf("2026-09-22T10-00-00Z-match-ro-jpOkle8Mp0-%d", i),
			T:   time.Date(2026, 9, 22, 10, 0, i, 0, time.UTC).Format("2006-01-02T15:04:05.000Z"),
			Src: "hls-pdt",
			Off: float64(i) * 3600.24,
			Dur: 3600.24,
		})
	}
	b, _ := json.Marshal(entries)
	return string(b)
}

// TestBuildStreamCopyExactDurationAndMetadata is the core guarantee of the
// package: the .m4a carries the exact frame count and duration the ADTS
// scanner measured (raw ADTS only ever estimates it) and the metadata the
// recorder needs to describe the recording.
func TestBuildStreamCopyExactDurationAndMetadata(t *testing.T) {
	r := tools(t)
	dir := t.TempDir()
	in := sineADTS(t, dir, "in.aac", 48000, 2, 20, "128k")
	info := scan(t, in)

	created := time.Date(2026, 9, 22, 10, 11, 12, 345_000_000, time.UTC)
	meta := Metadata{
		CreationTime: created,
		Title:        "Match RO vs JP",
		Date:         "2026-09-22",
		Comment:      "spectado-stream-recorder 1.1.0; id=match-ro-jpOkle8Mp0",
		Description:  runTable(),
	}
	if len(meta.Description) < 300 {
		t.Fatalf("description is only %d bytes, wanted a multi-hundred-byte payload", len(meta.Description))
	}

	out := filepath.Join(dir, "out.m4a")
	res, err := r.Build(t.Context(), []string{in}, out, Expect{
		Frames: info.Frames, Duration: info.Duration, Params: paramsFor(t, 48000, 2),
	}, meta)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if res.Frames != info.Frames {
		t.Errorf("frames = %d, scanned %d", res.Frames, info.Frames)
	}
	if d := abs(res.Duration - info.Duration); d > time.Millisecond {
		t.Errorf("duration = %s, scanned %s (off by %s)", res.Duration, info.Duration, d)
	}
	if res.Codec != "aac" {
		t.Errorf("codec = %q", res.Codec)
	}
	if f := strings.ToLower(res.Format); !strings.Contains(f, "mp4") && !strings.Contains(f, "m4a") {
		t.Errorf("format = %q", res.Format)
	}
	if res.SampleRate != 48000 || res.Channels != 2 {
		t.Errorf("stream = %d Hz / %d ch", res.SampleRate, res.Channels)
	}
	if res.StartTime != 0 {
		t.Errorf("start_time = %s, want 0", res.StartTime)
	}

	// Brand M4A in the leading ftyp box: that is what makes players (and
	// Apple devices) treat the object as an audio file.
	head, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read out: %v", err)
	}
	if len(head) < 16 || string(head[4:8]) != "ftyp" || string(head[8:12]) != "M4A " {
		t.Errorf("leading box = %q, want ftyp/M4A ", head[:min(16, len(head))])
	}

	if got := res.Tags["title"]; got != meta.Title {
		t.Errorf("title = %q, want %q", got, meta.Title)
	}
	if got := res.Tags["date"]; got != meta.Date {
		t.Errorf("date = %q, want %q", got, meta.Date)
	}
	if got := res.Tags["comment"]; got != meta.Comment {
		t.Errorf("comment = %q, want %q", got, meta.Comment)
	}
	if got := res.Tags["description"]; got != meta.Description {
		t.Errorf("description = %q, want %q", got, meta.Description)
	}
	ct, err := time.Parse(creationTimeLayout, res.Tags["creation_time"])
	if err != nil {
		t.Fatalf("creation_time %q: %v", res.Tags["creation_time"], err)
	}
	// The mov muxer stores seconds, so the sub-second part is expected to go.
	if d := ct.Sub(created); d > time.Second || d < -time.Second {
		t.Errorf("creation_time = %s, want ≈ %s", ct, created)
	}
}

// TestExtractReproducesTheOriginalADTS covers the merge path: an object that is
// already stored as .m4a must be convertible back to the exact bytes it was
// built from, otherwise appending a later session would change the audio.
func TestExtractReproducesTheOriginalADTS(t *testing.T) {
	r := tools(t)
	dir := t.TempDir()
	in := sineADTS(t, dir, "in.aac", 48000, 2, 8, "128k")
	info := scan(t, in)

	m4a := filepath.Join(dir, "out.m4a")
	if _, err := r.Build(t.Context(), []string{in}, m4a, Expect{
		Frames: info.Frames, Duration: info.Duration, Params: paramsFor(t, 48000, 2),
	}, Metadata{Title: "roundtrip"}); err != nil {
		t.Fatalf("Build: %v", err)
	}

	back := filepath.Join(dir, "back.aac")
	if err := r.Extract(t.Context(), m4a, back); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	want, err := os.ReadFile(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(back)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, got) {
		t.Fatalf("extracted ADTS differs: %d bytes vs %d", len(got), len(want))
	}
	mustNotExist(t, back+".part")
}

// TestBuildMergesPartsExactly is the merge guarantee the upload path relies on:
// several captures of one recording become one object whose duration is the sum
// of the parts, with nothing dropped at the seam.
func TestBuildMergesPartsExactly(t *testing.T) {
	r := tools(t)
	dir := t.TempDir()
	a := sineADTS(t, dir, "a.aac", 48000, 2, 7, "128k")
	b := sineADTS(t, dir, "b.aac", 48000, 2, 5, "128k")
	ia, ib := scan(t, a), scan(t, b)
	wantFrames := ia.Frames + ib.Frames
	wantDur := ia.Duration + ib.Duration

	out := filepath.Join(dir, "merged.m4a")
	res, err := r.Build(t.Context(), []string{a, b}, out, Expect{
		Frames: wantFrames, Duration: wantDur, Params: paramsFor(t, 48000, 2),
	}, Metadata{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if res.Frames != wantFrames {
		t.Errorf("frames = %d, want %d", res.Frames, wantFrames)
	}
	if d := abs(res.Duration - wantDur); d > time.Millisecond {
		t.Errorf("duration = %s, want %s (off by %s)", res.Duration, wantDur, d)
	}
}

// TestBuildDropsLegacyInBandID3Tags: 1.0.x recordings carry ID3v2 tags between
// frames (the removed wall-clock mechanism). Those files stay on disk across
// the upgrade, so the remux must swallow the tags — keep the exact duration and
// leave no trace of them in the object.
func TestBuildDropsLegacyInBandID3Tags(t *testing.T) {
	r := tools(t)
	dir := t.TempDir()
	plain := sineADTS(t, dir, "plain.aac", 48000, 2, 12, "128k")
	info := scan(t, plain)

	raw, err := os.ReadFile(plain)
	if err != nil {
		t.Fatal(err)
	}
	var tagged bytes.Buffer
	var sp adts.Splitter
	n := 0
	tags := 0
	sp.Feed(raw, func(b []byte, _ adts.Header) {
		if n%200 == 0 {
			tagged.Write(id3.Tag(
				id3.AppleTimestamp(uint64(n)*1024*90000/48000),
				id3.TXXX("WALLCLOCK", time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)),
			))
			tags++
		}
		tagged.Write(b)
		n++
	}, func(b []byte) { tagged.Write(b) })
	sp.Flush(func(b []byte) { tagged.Write(b) })
	if tags < 2 {
		t.Fatalf("only %d tags inserted", tags)
	}
	in := filepath.Join(dir, "tagged.aac")
	if err := os.WriteFile(in, tagged.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := scan(t, in); got.Frames != info.Frames || got.Tags != tags {
		t.Fatalf("tagged file scans as %d frames / %d tags, want %d / %d", got.Frames, got.Tags, info.Frames, tags)
	}

	out := filepath.Join(dir, "out.m4a")
	res, err := r.Build(t.Context(), []string{in}, out, Expect{
		Frames: info.Frames, Duration: info.Duration, Params: paramsFor(t, 48000, 2),
	}, Metadata{Description: runTable()})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if d := abs(res.Duration - info.Duration); d > time.Millisecond {
		t.Errorf("duration = %s, scanned %s", res.Duration, info.Duration)
	}
	if res.Tags["description"] == "" {
		t.Errorf("description tag missing")
	}
	blob, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(blob, []byte("WALLCLOCK")) {
		t.Errorf("in-band ID3 payload survived into the .m4a")
	}
}

// TestBuildRejectsFrameCountMismatch proves the verification is what stands
// between a silently broken remux and the bucket, and that a rejected build
// leaves nothing behind for the retry to trip over.
func TestBuildRejectsFrameCountMismatch(t *testing.T) {
	r := tools(t)
	dir := t.TempDir()
	in := sineADTS(t, dir, "in.aac", 48000, 2, 5, "128k")
	info := scan(t, in)

	out := filepath.Join(dir, "out.m4a")
	_, err := r.Build(t.Context(), []string{in}, out, Expect{
		Frames: info.Frames + 5, Duration: info.Duration, Params: paramsFor(t, 48000, 2),
	}, Metadata{})
	if !errors.Is(err, ErrDurationMismatch) {
		t.Fatalf("err = %v, want ErrDurationMismatch", err)
	}
	mustNotExist(t, out, out+".part")
}

// TestChangedStreamParamsFailCopyAndSurviveTranscode is the reason
// BuildTranscode exists: a stream copy of a 48 kHz + 44.1 kHz concatenation
// exits 0 and yields a short file, so only the duration guard catches it; the
// transcode path then produces a correct, single-parameter object.
func TestChangedStreamParamsFailCopyAndSurviveTranscode(t *testing.T) {
	r := tools(t)
	dir := t.TempDir()
	a := sineADTS(t, dir, "a48.aac", 48000, 2, 10, "128k")
	b := sineADTS(t, dir, "b44.aac", 44100, 2, 10, "128k")
	ia, ib := scan(t, a), scan(t, b)
	wantDur := ia.Duration + ib.Duration

	joined := filepath.Join(dir, "joined.aac")
	n, err := Concat([]string{a, b}, joined)
	if err != nil {
		t.Fatalf("Concat: %v", err)
	}
	if fi, err := os.Stat(joined); err != nil || fi.Size() != n {
		t.Fatalf("Concat wrote %d bytes, stat says %v/%v", n, fi, err)
	}

	out := filepath.Join(dir, "copy.m4a")
	if _, err := r.Build(t.Context(), []string{joined}, out, Expect{
		Frames: ia.Frames + ib.Frames, Duration: wantDur, Params: paramsFor(t, 48000, 2),
	}, Metadata{}); !errors.Is(err, ErrDurationMismatch) {
		t.Fatalf("stream copy of a mixed-rate input: err = %v, want ErrDurationMismatch", err)
	}
	mustNotExist(t, out, out+".part")

	tr := filepath.Join(dir, "transcoded.m4a")
	res, err := r.BuildTranscode(t.Context(), []Chunk{
		{Path: a, Params: paramsFor(t, 48000, 2)},
		{Path: b, Params: paramsFor(t, 44100, 2)},
	}, tr, wantDur, "128k", Metadata{Title: "transcoded"})
	if err != nil {
		t.Fatalf("BuildTranscode: %v", err)
	}
	if d := abs(res.Duration - wantDur); d > time.Second {
		t.Errorf("duration = %s, want %s (off by %s)", res.Duration, wantDur, d)
	}
	if res.SampleRate != 48000 || res.Channels != 2 || res.Codec != "aac" {
		t.Errorf("transcoded stream = %s %d Hz / %d ch, want aac 48000/2", res.Codec, res.SampleRate, res.Channels)
	}
}

// TestBuildFailsOnNonMediaInput: -xerror plus the verification must turn junk
// into an error instead of an empty object, and clean up after itself. The
// input is deliberately far larger than a pipe buffer: ffmpeg gives up on the
// first bytes, so the remaining megabytes have nowhere to go and a naive
// "write it all, then wait" would deadlock here.
func TestBuildFailsOnNonMediaInput(t *testing.T) {
	r := tools(t)
	dir := t.TempDir()
	in := filepath.Join(dir, "in.aac")
	if err := os.WriteFile(in, bytes.Repeat([]byte("not audio at all\n"), 500_000), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out.m4a")

	done := make(chan error, 1)
	go func() {
		_, err := r.Build(t.Context(), []string{in}, out, Expect{
			Frames: 100, Duration: 2 * time.Second, Params: paramsFor(t, 48000, 2),
		}, Metadata{})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Build succeeded on a non-media input")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Build hung after ffmpeg rejected the input")
	}
	mustNotExist(t, out, out+".part")
}

// TestConcatFailsOnMissingPart: the fallback path must not write a half object
// when one capture disappeared under it.
func TestConcatFailsOnMissingPart(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.aac")
	if err := os.WriteFile(a, []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out.aac")
	if _, err := Concat([]string{a, filepath.Join(dir, "gone.aac")}, out); err == nil {
		t.Fatal("Concat succeeded with a missing part")
	}
	mustNotExist(t, out, out+".part")

	empty := filepath.Join(dir, "empty.aac")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Concat([]string{a, empty}, out); err == nil {
		t.Fatal("Concat succeeded with an empty part")
	}
	mustNotExist(t, out, out+".part")
}

func TestProbeMissingFileErrors(t *testing.T) {
	r := tools(t)
	if _, err := r.Probe(t.Context(), filepath.Join(t.TempDir(), "nope.m4a")); err == nil {
		t.Fatal("Probe succeeded on a missing file")
	}
}

// --- pure helpers (no ffmpeg needed) ---------------------------------------

func TestMetadataArgsOmitsEmptyAndSanitises(t *testing.T) {
	args := metadataArgs(Metadata{
		CreationTime: time.Date(2026, 9, 22, 10, 11, 12, 345_000_000, time.UTC),
		Title:        "line\nbreak\x00",
	})
	want := []string{
		"-metadata", "creation_time=2026-09-22T10:11:12.345000Z",
		"-metadata", "title=line break ",
	}
	if strings.Join(args, "\x1f") != strings.Join(want, "\x1f") {
		t.Fatalf("args = %q, want %q", args, want)
	}
	if got := metadataArgs(Metadata{}); got != nil {
		t.Fatalf("empty metadata produced %q", got)
	}
}

func TestConcatFilterGraph(t *testing.T) {
	got := concatFilter(2, adts.Params{SampleRate: 48000, ChannelConfig: 1, Blocks: 1})
	want := "[0:a]aresample=48000,aformat=channel_layouts=mono[a0];" +
		"[1:a]aresample=48000,aformat=channel_layouts=mono[a1];" +
		"[a0][a1]concat=n=2:v=0:a=1[out]"
	if got != want {
		t.Fatalf("filter = %q, want %q", got, want)
	}
	// channel_configuration 7 (8 channels) has no layout name we force.
	if got := concatFilter(1, adts.Params{SampleRate: 44100, ChannelConfig: 7}); !strings.Contains(got, "[0:a]aresample=44100[a0];") {
		t.Fatalf("filter = %q, want no aformat", got)
	}
}

func TestDurationToleranceIsThreeFramesOrMore(t *testing.T) {
	// 3 frames at 48 kHz = 64 ms, below the 250 ms floor.
	if got := durationTolerance(adts.Params{SampleRate: 48000, Blocks: 1}); got != minDurationTolerance {
		t.Errorf("tolerance = %s, want %s", got, minDurationTolerance)
	}
	// 3 frames at 8 kHz with 4 raw data blocks = 1.536 s, above the floor.
	if got := durationTolerance(adts.Params{SampleRate: 8000, Blocks: 4}); got != 1536*time.Millisecond {
		t.Errorf("tolerance = %s, want 1.536s", got)
	}
	if got := durationTolerance(adts.Params{}); got != minDurationTolerance {
		t.Errorf("tolerance = %s, want %s", got, minDurationTolerance)
	}
}

func TestStderrTailKeepsLastMeaningfulLineRedacted(t *testing.T) {
	var s stderrTail
	fmt.Fprint(&s, "[warning] Estimating duration from bitrate, this may be inaccurate\n")
	fmt.Fprint(&s, "[error] Error opening https://cdn.example.com/live.m3u8?token=SECRET\n")
	fmt.Fprint(&s, "[warning] Estimating duration from bitrate, this may be inaccurate\n")
	last := s.last()
	if strings.Contains(last, "SECRET") {
		t.Errorf("token not redacted: %q", last)
	}
	if !strings.Contains(last, "Error opening") {
		t.Errorf("last meaningful line = %q", last)
	}
	// A fatal message without a trailing newline must still be reported.
	var s2 stderrTail
	fmt.Fprint(&s2, "[fatal] Error opening input files: End of file")
	if !strings.Contains(s2.last(), "End of file") {
		t.Errorf("unterminated line lost: %q", s2.last())
	}
	if got := (&stderrTail{}).last(); got == "" {
		t.Errorf("empty tail should describe itself, got %q", got)
	}
}

func TestTimeoutScalesWithInput(t *testing.T) {
	r := &Remuxer{}
	if got := r.timeoutFor(0); got != defaultBaseTimeout {
		t.Errorf("timeout = %s, want %s", got, defaultBaseTimeout)
	}
	if got := r.timeoutFor(10 << 20); got != defaultBaseTimeout+10*time.Second {
		t.Errorf("timeout = %s, want %s", got, defaultBaseTimeout+10*time.Second)
	}
	r2 := &Remuxer{BaseTimeout: time.Minute, MinThroughput: 1 << 10}
	if got := r2.timeoutFor(4 << 10); got != time.Minute+4*time.Second {
		t.Errorf("timeout = %s, want %s", got, time.Minute+4*time.Second)
	}
}

func TestBuildWithoutPartsFails(t *testing.T) {
	r := &Remuxer{FFmpegPath: "ffmpeg", FFprobePath: "ffprobe"}
	if _, err := r.Build(context.Background(), nil, filepath.Join(t.TempDir(), "x.m4a"), Expect{}, Metadata{}); err == nil {
		t.Fatal("Build succeeded without inputs")
	}
	if _, err := r.BuildTranscode(context.Background(), nil, filepath.Join(t.TempDir(), "x.m4a"), time.Second, "128k", Metadata{}); err == nil {
		t.Fatal("BuildTranscode succeeded without inputs")
	}
	if _, err := Concat(nil, filepath.Join(t.TempDir(), "x.aac")); err == nil {
		t.Fatal("Concat succeeded without inputs")
	}
}
