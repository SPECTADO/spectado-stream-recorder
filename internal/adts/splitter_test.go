package adts

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spectado/stream-recorder/internal/id3"
)

func tag() []byte {
	return id3.Tag(
		id3.AppleTimestamp(90000),
		id3.TXXX("WALLCLOCK", "2026-09-17T10:00:00.000Z"),
		id3.TXXX("WALLCLOCK-SOURCE", "hls-pdt"),
	)
}

// runSplitter feeds data in fixed-size chunks and returns the exact bytes the
// splitter emitted (frames + junk, in order) and the number of complete frames.
// The emitted bytes must always reconstruct the input verbatim.
func runSplitter(data []byte, chunk int) (out []byte, frames int) {
	var s Splitter
	fr := func(b []byte, h Header) { out = append(out, b...); frames++ }
	jk := func(b []byte) { out = append(out, b...) }
	if chunk <= 0 {
		chunk = len(data)
		if chunk == 0 {
			chunk = 1
		}
	}
	for i := 0; i < len(data); i += chunk {
		end := i + chunk
		if end > len(data) {
			end = len(data)
		}
		s.Feed(data[i:end], fr, jk)
	}
	s.Flush(jk)
	return out, frames
}

func TestSplitterFaithfulAndCounts(t *testing.T) {
	tg := tag()
	cases := []struct {
		name   string
		data   []byte
		frames int
	}{
		{"clean frames", frames(10, 100, false), 10},
		{"crc frames", frames(8, 200, true), 8},
		{"mixed sizes", concat(frame(1, false), frame(8182, false), frame(400, true), frame(0, false)), 4},
		{"junk between frames", concat(frames(3, 100, false), bytes.Repeat([]byte{0x00, 0x11, 0x22}, 20), frames(3, 100, false)), 6},
		{"leading junk", concat(junk(37, 9), frames(4, 120, false)), 4},
		{"trailing partial frame", concat(frames(3, 100, false), frame(100, false)[:50]), 3},
		{"trailing partial header", concat(frames(3, 100, false), frame(100, false)[:3]), 3},
		// A run that is a single frame never gets a following header to confirm
		// it, so it is flushed as junk (bytes preserved); the file scan still
		// counts it via the clean-EOF rule. Real runs are hundreds of frames.
		{"lone frame flushed as junk", frame(100, false), 0},
		{"two frames confirm and emit", frames(2, 100, false), 2},
		{"tag between frames", concat(frames(3, 100, false), tg, frames(3, 100, false)), 6},
		{"tag at eof", concat(frames(4, 100, false), tg), 4},
		{"tag then partial frame", concat(frames(3, 100, false), tg, frame(100, false)[:40]), 3},
		{"leading tag", concat(tg, frames(5, 100, false)), 5},
		{"empty", nil, 0},
	}
	for _, c := range cases {
		for _, chunk := range []int{1, 2, 3, 7, 13, 107, 1000, 0} {
			out, gotFrames := runSplitter(c.data, chunk)
			if !bytes.Equal(out, c.data) {
				t.Fatalf("%s chunk=%d: output is not a faithful copy (%d vs %d bytes)", c.name, chunk, len(out), len(c.data))
			}
			if gotFrames != c.frames {
				t.Fatalf("%s chunk=%d: frames = %d, want %d", c.name, chunk, gotFrames, c.frames)
			}
		}
	}
}

// TestSplitterFakeSyncInPayload ensures a sync-looking pattern inside a payload
// does not split a frame or become junk.
func TestSplitterFakeSyncInPayload(t *testing.T) {
	data := concat(withFakeSync(100, 30), withFakeSync(100, 30), withFakeSync(100, 30))
	for _, chunk := range []int{1, 5, 50, 0} {
		out, frames := runSplitter(data, chunk)
		if !bytes.Equal(out, data) || frames != 3 {
			t.Fatalf("chunk=%d: frames=%d faithful=%v", chunk, frames, bytes.Equal(out, data))
		}
	}
}

func TestParseHeaderAndDuration(t *testing.T) {
	f := frame(100, false)
	h, ok := ParseHeader(f)
	if !ok || h.Length != 107 || h.SampleRate != 44100 || h.Blocks != 1 {
		t.Fatalf("ParseHeader = %+v ok=%v", h, ok)
	}
	// 1024 samples at 44.1 kHz.
	if d := h.Duration(); d < 0.0231 || d > 0.0233 {
		t.Fatalf("Duration = %v", d)
	}
	if _, ok := ParseHeader([]byte{0x00, 0x01}); ok {
		t.Fatal("short/garbage header should not parse")
	}
	if (Header{}).Duration() != 0 {
		t.Fatal("zero header duration must be 0, not NaN")
	}
}

func TestScanCountsTagsAnywhere(t *testing.T) {
	tg := tag()
	data := concat(tg, frames(5, 100, false), tg, frames(5, 100, false), tg)
	info := scanBytes(t, data)
	if info.Frames != 10 {
		t.Fatalf("frames = %d, want 10", info.Frames)
	}
	if info.Tags != 3 || info.TagBytes != int64(3*len(tg)) {
		t.Fatalf("tags = %d tagBytes = %d, want 3 / %d", info.Tags, info.TagBytes, 3*len(tg))
	}
	if info.Junk != 0 {
		t.Fatalf("junk = %d, want 0", info.Junk)
	}
}

func TestScanTagBetweenFramesThenJunk(t *testing.T) {
	tg := tag()
	stray := bytes.Repeat([]byte{0x01, 0x02, 0x03}, 10)
	data := concat(frames(3, 100, false), tg, stray, frames(3, 100, false))
	info := scanBytes(t, data)
	if info.Frames != 6 || info.Tags != 1 || info.Junk != int64(len(stray)) {
		t.Fatalf("info = %+v (want 6 frames, 1 tag, %d junk)", info, len(stray))
	}
}

func TestScanRuns(t *testing.T) {
	tg := tag()
	fl := len(frame(100, false)) // 107
	run0 := frames(10, 100, false)
	run1 := concat(tg, frames(5, 100, false))
	file := concat(run0, run1)
	p := filepath.Join(t.TempDir(), "runs.aac")
	if err := os.WriteFile(p, file, 0o644); err != nil {
		t.Fatal(err)
	}

	// Boundary exactly at the start of run1 (offset len(run0)).
	runs, total, err := ScanRuns(p, []int64{0, int64(len(run0))})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("runs = %d", len(runs))
	}
	if runs[0].Frames != 10 || runs[0].Tags != 0 {
		t.Fatalf("run0 = %+v", runs[0])
	}
	if runs[1].Frames != 5 || runs[1].Tags != 1 || runs[1].TagBytes != int64(len(tg)) {
		t.Fatalf("run1 = %+v", runs[1])
	}
	if total.Frames != 15 || total.Tags != 1 {
		t.Fatalf("total = %+v", total)
	}

	// A boundary that falls exactly on a frame start inside run0 (5 frames each).
	runs, _, err = ScanRuns(p, []int64{0, int64(5 * fl)})
	if err != nil {
		t.Fatal(err)
	}
	if runs[0].Frames != 5 || runs[1].Frames != 10 {
		t.Fatalf("mid split runs = %d / %d, want 5 / 10", runs[0].Frames, runs[1].Frames)
	}

	// Bytes before offsets[0] are attributed to run 0.
	runs, _, err = ScanRuns(p, []int64{int64(3 * fl), int64(len(run0))})
	if err != nil {
		t.Fatal(err)
	}
	if runs[0].Frames != 10 || runs[1].Frames != 5 {
		t.Fatalf("pre-offset runs = %d / %d, want 10 / 5", runs[0].Frames, runs[1].Frames)
	}

	// A single-run (empty offsets) falls back to the whole file.
	runs, total, err = ScanRuns(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Frames != 15 || total.Frames != 15 {
		t.Fatalf("single run = %+v total = %+v", runs, total)
	}
}

func TestScanRunsThreeWithDurations(t *testing.T) {
	r0 := frames(3, 100, false)
	r1 := frames(7, 100, false)
	r2 := frames(2, 100, false)
	file := concat(r0, r1, r2)
	p := filepath.Join(t.TempDir(), "three.aac")
	if err := os.WriteFile(p, file, 0o644); err != nil {
		t.Fatal(err)
	}
	offsets := []int64{0, int64(len(r0)), int64(len(r0) + len(r1))}
	runs, total, err := ScanRuns(p, offsets)
	if err != nil {
		t.Fatal(err)
	}
	want := []int{3, 7, 2}
	for i, w := range want {
		if runs[i].Frames != w {
			t.Fatalf("run %d frames = %d, want %d", i, runs[i].Frames, w)
		}
		if runs[i].Duration <= 0 {
			t.Fatalf("run %d duration should be positive: %v", i, runs[i].Duration)
		}
	}
	if total.Frames != 12 {
		t.Fatalf("total frames = %d, want 12", total.Frames)
	}
	// Total duration is the sum of the per-run durations.
	sum := runs[0].Duration + runs[1].Duration + runs[2].Duration
	if d := total.Duration - sum; d < -time.Microsecond || d > time.Microsecond {
		t.Fatalf("total duration %v != sum %v", total.Duration, sum)
	}
}

func TestScanTailWithTagNearEnd(t *testing.T) {
	tg := tag()
	five := frames(5, 100, false)

	// Partial frame after a trailing tag: the tag continues the chain, the
	// partial is trimmed.
	partial := frame(100, false)[:50]
	data := concat(five, tg, partial)
	at, ok := ScanTail(data, int64(len(data)))
	if !ok || at != int64(len(data)-len(partial)) {
		t.Fatalf("partial after tag: truncateAt = %d ok = %v, want %d", at, ok, len(data)-len(partial))
	}

	// File ending exactly after a tag is clean.
	data = concat(five, tg)
	at, ok = ScanTail(data, int64(len(data)))
	if !ok || at != int64(len(data)) {
		t.Fatalf("clean tag end: truncateAt = %d ok = %v, want %d", at, ok, len(data))
	}

	// A truncated tag at the very end is itself the trailing partial.
	data = concat(five, tg[:6])
	at, ok = ScanTail(data, int64(len(data)))
	if !ok || at != int64(len(five)) {
		t.Fatalf("truncated tag: truncateAt = %d ok = %v, want %d", at, ok, len(five))
	}

	// Trimming through TrimPartialTail on disk.
	p := filepath.Join(t.TempDir(), "tail.aac")
	if err := os.WriteFile(p, concat(five, tg, partial), 0o644); err != nil {
		t.Fatal(err)
	}
	removed, err := TrimPartialTail(p)
	if err != nil || removed != int64(len(partial)) {
		t.Fatalf("TrimPartialTail removed = %d err = %v, want %d", removed, err, len(partial))
	}
	got, _ := os.ReadFile(p)
	if !bytes.Equal(got, concat(five, tg)) {
		t.Fatal("trim must keep the frames and the trailing tag")
	}
}
