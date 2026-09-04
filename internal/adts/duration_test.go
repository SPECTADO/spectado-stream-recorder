package adts

import (
	"bufio"
	"bytes"
	"math"
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
