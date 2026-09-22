package adts

import (
	"bufio"
	"bytes"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func scanBytes(t *testing.T, data []byte) Info {
	t.Helper()
	info, err := scan(bufio.NewReaderSize(bytes.NewReader(data), 256<<10))
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestScanCleanFile(t *testing.T) {
	// 1000 frames at 44.1 kHz (sampling index 4 in the test frame): 1024 samples each.
	data := frames(1000, 300, false)
	info := scanBytes(t, data)
	samples := 1000 * 1024.0
	want := time.Duration(samples / 44100 * float64(time.Second))
	if info.Frames != 1000 || info.Junk != 0 {
		t.Fatalf("info = %+v", info)
	}
	if d := math.Abs(float64(info.Duration - want)); d > float64(time.Microsecond) {
		t.Fatalf("duration = %v, want %v", info.Duration, want)
	}
}

func TestScanMixedHeaderSizesAndPartialTail(t *testing.T) {
	data := concat(frames(10, 100, false), frames(10, 200, true), frame(50, false)[:30])
	info := scanBytes(t, data)
	if info.Frames != 20 || info.Junk != 30 {
		t.Fatalf("info = %+v, want 20 frames and 30 junk bytes", info)
	}
	// A CRC changes the header size, not the stream parameters, and the
	// truncated tail belongs to the chunk it sits in: one chunk, whole file.
	if len(info.Chunks) != 1 || info.ParamsChanged {
		t.Fatalf("chunks = %+v, want one unchanged chunk", info.Chunks)
	}
	checkPartition(t, info, int64(len(data)))
}

func TestScanResyncsAfterJunk(t *testing.T) {
	junk := bytes.Repeat([]byte{0x00, 0x11, 0x22}, 20)
	data := concat(frames(5, 100, false), junk, frames(5, 100, false))
	info := scanBytes(t, data)
	if info.Frames != 10 || info.Junk != int64(len(junk)) {
		t.Fatalf("info = %+v, want 10 frames and %d junk bytes", info, len(junk))
	}
	// A lone sync-looking pair inside the junk must not count as a frame.
	fake := frame(100, false)[:7] // header only, claims 107 bytes but is followed by junk
	data = concat(frames(5, 100, false), fake, junk, frames(5, 100, false))
	info = scanBytes(t, data)
	if info.Frames != 10 {
		t.Fatalf("frames = %d, want 10 (fake header must be skipped)", info.Frames)
	}
}

func TestScanSkipsID3AndHandlesEmpty(t *testing.T) {
	id3 := append([]byte("ID3\x04\x00\x00\x00\x00\x00\x0a"), make([]byte, 10)...) // 10-byte header + 10 bytes of tag
	data := concat(id3, frames(3, 80, false))
	info := scanBytes(t, data)
	if info.Frames != 3 || info.Junk != 0 {
		t.Fatalf("info = %+v", info)
	}
	if info := scanBytes(t, nil); info.Frames != 0 || info.Duration != 0 || info.Junk != 0 {
		t.Fatalf("empty: %+v", info)
	}
	if info := scanBytes(t, []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")); info.Frames != 0 || info.Junk != 32 {
		t.Fatalf("garbage: %+v", info)
	}
}

func TestScanFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.aac")
	if err := os.WriteFile(p, frames(431, 250, false), 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := Scan(p)
	if err != nil || info.Frames != 431 {
		t.Fatalf("info=%+v err=%v", info, err)
	}
	if _, err := Scan(filepath.Join(t.TempDir(), "missing.aac")); err == nil {
		t.Fatal("missing file must be an error")
	}
}

// Frames straddling the reader's buffer boundary must be handled with the
// bytes of the most recent Peek: an earlier Peek's slice is invalidated when
// bufio slides the buffer (this panicked on real recordings > 256 KiB).
func TestScanAcrossBufferRefills(t *testing.T) {
	// Varying frame sizes so that no stale offset happens to land on a header.
	rng := rand.New(rand.NewPCG(7, 11))
	var data []byte
	for i := 0; i < 5000; i++ {
		data = append(data, frame(50+rng.IntN(350), i%7 == 0)...)
	}
	info, err := scan(bufio.NewReaderSize(bytes.NewReader(data), 512))
	if err != nil {
		t.Fatal(err)
	}
	if info.Frames != 5000 || info.Junk != 0 {
		t.Fatalf("info = %+v, want 5000 frames and no junk", info)
	}
	// Same through the public API with the production buffer size and a
	// file several times larger than that buffer.
	p := filepath.Join(t.TempDir(), "big.aac")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if info, err := Scan(p); err != nil || info.Frames != 5000 || info.Junk != 0 {
		t.Fatalf("Scan(big) = %+v, %v", info, err)
	}
}

// params is the Params that a frame built by frameWith from o carries.
func (o frameOpts) params() Params {
	blocks := o.blocks
	if blocks == 0 {
		blocks = 1
	}
	return Params{Profile: o.profile, SampleRateIndex: o.sfi, SampleRate: sampleRates[o.sfi], ChannelConfig: o.chanCfg, Blocks: blocks}
}

var (
	stereo441 = frameOpts{profile: 1, sfi: 4, chanCfg: 2}            // LC, 44.1 kHz, stereo
	mono48    = frameOpts{profile: 1, sfi: 3, chanCfg: 1}            // LC, 48 kHz, mono
	dualBlock = frameOpts{profile: 1, sfi: 4, chanCfg: 2, blocks: 2} // two raw data blocks per frame
)

// checkPartition asserts the documented Chunk contract: the chunks tile the
// whole scanned region [0,size) without gap or overlap and account for every
// frame the scan counted.
func checkPartition(t *testing.T, info Info, size int64) {
	t.Helper()
	var off int64
	frames := 0
	for i, c := range info.Chunks {
		if c.Offset != off {
			t.Fatalf("chunk %d offset = %d, want %d", i, c.Offset, off)
		}
		if c.Length <= 0 || c.Frames <= 0 {
			t.Fatalf("chunk %d = %+v, want a non-empty range", i, c)
		}
		off += c.Length
		frames += c.Frames
	}
	if off != size {
		t.Fatalf("chunks cover %d bytes, want %d", off, size)
	}
	if frames != info.Frames {
		t.Fatalf("chunks hold %d frames, want %d", frames, info.Frames)
	}
}

// dur converts a seconds figure derived from sample counts into a Duration the
// way the scanners do.
func dur(seconds float64) time.Duration { return time.Duration(seconds * float64(time.Second)) }

// approx compares durations with a tolerance that absorbs the float rounding of
// the per-frame sums.
func approx(got, want time.Duration) bool {
	d := got - want
	return d > -time.Millisecond && d < time.Millisecond
}

func TestScanUniformFileIsOneChunk(t *testing.T) {
	data := frames(100, 300, false)
	info := scanBytes(t, data)
	if info.ParamsChanged {
		t.Fatalf("uniform file must not report a parameter change: %+v", info)
	}
	if want := stereo441.params(); info.Params != want {
		t.Fatalf("params = %+v, want %+v", info.Params, want)
	}
	if len(info.Chunks) != 1 {
		t.Fatalf("chunks = %+v, want exactly one", info.Chunks)
	}
	c := info.Chunks[0]
	if c.Offset != 0 || c.Length != int64(len(data)) || c.Frames != 100 || c.Params != info.Params {
		t.Fatalf("chunk = %+v, want the whole file with %+v", c, info.Params)
	}
	if !approx(c.Duration, info.Duration) {
		t.Fatalf("chunk duration = %v, want the file duration %v", c.Duration, info.Duration)
	}
	checkPartition(t, info, int64(len(data)))

	// A file without a single frame has no chunks at all.
	if info := scanBytes(t, []byte("AAAAAAAAAAAAAAAA")); info.Chunks != nil || info.Params != (Params{}) || info.ParamsChanged {
		t.Fatalf("frameless file: %+v, want no chunks and zero params", info)
	}
}

func TestScanChunksOnParameterChange(t *testing.T) {
	a := framesWith(5, 100, stereo441)
	b := framesWith(3, 120, mono48)
	c := framesWith(4, 100, stereo441)
	data := concat(a, b, c)
	info := scanBytes(t, data)

	if info.Frames != 12 || !info.ParamsChanged {
		t.Fatalf("info = %+v, want 12 frames and a parameter change", info)
	}
	if want := stereo441.params(); info.Params != want {
		t.Fatalf("params = %+v, want the first frame's %+v", info.Params, want)
	}
	if len(info.Chunks) != 3 {
		t.Fatalf("chunks = %+v, want 3", info.Chunks)
	}
	want := []Chunk{
		{Offset: 0, Length: int64(len(a)), Params: stereo441.params(), Frames: 5},
		{Offset: int64(len(a)), Length: int64(len(b)), Params: mono48.params(), Frames: 3},
		{Offset: int64(len(a) + len(b)), Length: int64(len(c)), Params: stereo441.params(), Frames: 4},
	}
	secs := []float64{5 * 1024.0 / 44100, 3 * 1024.0 / 48000, 4 * 1024.0 / 44100}
	for i, w := range want {
		got := info.Chunks[i]
		if got.Offset != w.Offset || got.Length != w.Length || got.Frames != w.Frames || got.Params != w.Params {
			t.Fatalf("chunk %d = %+v, want %+v", i, got, w)
		}
		if d := dur(secs[i]); !approx(got.Duration, d) {
			t.Fatalf("chunk %d duration = %v, want %v", i, got.Duration, d)
		}
	}
	checkPartition(t, info, int64(len(data)))
}

func TestScanChunkAbsorbsTagsAndJunk(t *testing.T) {
	tg := tag()
	stray := bytes.Repeat([]byte{0x01, 0x02, 0x03}, 10)
	// The tag and the junk sit inside the 44.1 kHz region, so they belong to
	// chunk 0; the leading tag belongs to it as well because chunk 0 starts at
	// the beginning of the file.
	head := concat(tg, framesWith(4, 100, stereo441), tg, stray, framesWith(2, 100, stereo441))
	tail := framesWith(3, 100, mono48)
	data := concat(head, tail)
	info := scanBytes(t, data)

	if info.Frames != 9 || info.Tags != 2 || info.Junk != int64(len(stray)) {
		t.Fatalf("info = %+v, want 9 frames, 2 tags, %d junk", info, len(stray))
	}
	if len(info.Chunks) != 2 {
		t.Fatalf("chunks = %+v, want 2", info.Chunks)
	}
	c0, c1 := info.Chunks[0], info.Chunks[1]
	if c0.Offset != 0 || c0.Length != int64(len(head)) || c0.Frames != 6 {
		t.Fatalf("chunk 0 = %+v, want offset 0, length %d, 6 frames", c0, len(head))
	}
	if c1.Offset != int64(len(head)) || c1.Length != int64(len(tail)) || c1.Frames != 3 {
		t.Fatalf("chunk 1 = %+v, want offset %d, length %d, 3 frames", c1, len(head), len(tail))
	}
	checkPartition(t, info, int64(len(data)))

	// Trailing junk belongs to the last chunk, so the chunks still tile the file.
	data = concat(data, stray)
	info = scanBytes(t, data)
	checkPartition(t, info, int64(len(data)))
}

func TestScanMultiBlockFramesFormTheirOwnChunk(t *testing.T) {
	a := framesWith(4, 100, stereo441)
	b := framesWith(2, 100, dualBlock)
	c := framesWith(4, 100, stereo441)
	data := concat(a, b, c)
	info := scanBytes(t, data)

	if len(info.Chunks) != 3 || !info.ParamsChanged {
		t.Fatalf("info = %+v, want 3 chunks (Blocks is a parameter)", info)
	}
	mid := info.Chunks[1]
	if mid.Params.Blocks != 2 || mid.Frames != 2 {
		t.Fatalf("middle chunk = %+v, want 2 frames with Blocks 2", mid)
	}
	// Two frames of two raw data blocks each: 2 x 2048 samples at 44.1 kHz.
	if want := dur(2 * 2048.0 / 44100); !approx(mid.Duration, want) {
		t.Fatalf("middle chunk duration = %v, want %v", mid.Duration, want)
	}
	// The whole file: 8 single-block frames + 2 double-block ones.
	if want := dur((8*1024.0 + 2*2048.0) / 44100); !approx(info.Duration, want) {
		t.Fatalf("duration = %v, want %v", info.Duration, want)
	}
	checkPartition(t, info, int64(len(data)))
}

func TestScanRunsReportsParamsPerRunAndChunksPerFile(t *testing.T) {
	run0 := framesWith(6, 100, stereo441)                                    // uniform
	run1 := concat(framesWith(2, 100, stereo441), framesWith(3, 80, mono48)) // switches mid-run
	data := concat(run0, run1)
	p := filepath.Join(t.TempDir(), "params.aac")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	runs, total, err := ScanRuns(p, []int64{0, int64(len(run0))})
	if err != nil {
		t.Fatal(err)
	}
	if runs[0].Params != stereo441.params() || runs[0].ParamsChanged {
		t.Fatalf("run 0 = %+v, want uniform 44.1 kHz stereo", runs[0])
	}
	if runs[1].Params != stereo441.params() || !runs[1].ParamsChanged {
		t.Fatalf("run 1 = %+v, want the first frame's params and a change", runs[1])
	}
	if total.Params != stereo441.params() || !total.ParamsChanged {
		t.Fatalf("total = %+v, want the file's first params and a change", total)
	}
	// The chunk boundary is the parameter change, not the run boundary: chunk 0
	// spans run 0 and the first two frames of run 1.
	if len(total.Chunks) != 2 {
		t.Fatalf("chunks = %+v, want 2", total.Chunks)
	}
	if c := total.Chunks[0]; c.Offset != 0 || c.Frames != 8 {
		t.Fatalf("chunk 0 = %+v, want 8 frames from offset 0", c)
	}
	if c := total.Chunks[1]; c.Params != mono48.params() || c.Frames != 3 {
		t.Fatalf("chunk 1 = %+v, want 3 mono 48 kHz frames", c)
	}
	checkPartition(t, total, int64(len(data)))

	// A uniform file yields exactly one chunk here too, and the runs agree.
	uniform := filepath.Join(t.TempDir(), "uniform.aac")
	if err := os.WriteFile(uniform, frames(9, 100, false), 0o644); err != nil {
		t.Fatal(err)
	}
	runs, total, err = ScanRuns(uniform, []int64{0, int64(3 * len(frame(100, false)))})
	if err != nil {
		t.Fatal(err)
	}
	if len(total.Chunks) != 1 || total.ParamsChanged {
		t.Fatalf("uniform total = %+v, want one chunk and no change", total)
	}
	for i, r := range runs {
		if r.Params != stereo441.params() || r.ParamsChanged {
			t.Fatalf("uniform run %d = %+v", i, r)
		}
	}
}

func TestParseHeaderExposesProfileAndChannelConfig(t *testing.T) {
	// 5.1 (channel_configuration 6) needs the bit that lives in byte 2, so a
	// wrong split of the field shows up here.
	cases := []frameOpts{
		stereo441,
		mono48,
		dualBlock,
		{profile: 0, sfi: 0, chanCfg: 6},  // Main, 96 kHz, 5.1
		{profile: 3, sfi: 12, chanCfg: 7}, // LTP, 7350 Hz, 7.1
		{profile: 2, sfi: 8, chanCfg: 0},  // SSR, 16 kHz, channels from a PCE
		{profile: 1, sfi: 4, chanCfg: 2, crc: true},
	}
	for _, o := range cases {
		h, ok := ParseHeader(frameWith(100, o))
		if !ok {
			t.Fatalf("%+v: ParseHeader failed", o)
		}
		if h.Params() != o.params() {
			t.Fatalf("%+v: params = %+v, want %+v", o, h.Params(), o.params())
		}
		if want := len(frameWith(100, o)); h.Length != want {
			t.Fatalf("%+v: length = %d, want %d", o, h.Length, want)
		}
	}
}

func TestParamsChannelsAndProfileName(t *testing.T) {
	cases := []struct {
		params   Params
		channels int
		name     string
	}{
		{Params{Profile: 0, ChannelConfig: 0}, 0, "Main"}, // 0 = defined by a PCE
		{Params{Profile: 1, ChannelConfig: 1}, 1, "LC"},
		{Params{Profile: 2, ChannelConfig: 2}, 2, "SSR"},
		{Params{Profile: 3, ChannelConfig: 3}, 3, "LTP"},
		{Params{Profile: 1, ChannelConfig: 4}, 4, "LC"},
		{Params{Profile: 1, ChannelConfig: 5}, 5, "LC"},
		{Params{Profile: 1, ChannelConfig: 6}, 6, "LC"},
		{Params{Profile: 1, ChannelConfig: 7}, 8, "LC"}, // 7 is 7.1: eight channels
	}
	for _, c := range cases {
		if got := c.params.Channels(); got != c.channels {
			t.Fatalf("%+v: channels = %d, want %d", c.params, got, c.channels)
		}
		if got := c.params.ProfileName(); got != c.name {
			t.Fatalf("%+v: profile name = %q, want %q", c.params, got, c.name)
		}
	}
}
