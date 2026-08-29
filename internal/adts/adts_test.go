package adts

import (
	"bytes"
	"errors"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// frame builds a valid ADTS frame: MPEG-4, profile LC (1), sampling index 4
// (44.1 kHz), channel config 2, correct frame_length, raw_data_blocks 0, plus
// deterministic payload bytes. Payload bytes are kept in 0x10..0xEF so they
// can never form an accidental sync word.
func frame(payloadLen int, crc bool) []byte {
	hdr := minHeader
	b1 := byte(0xF1)
	if crc {
		hdr = crcHeader
		b1 = 0xF0
	}
	fl := hdr + payloadLen
	if fl >= 1<<13 {
		panic("frame too long for 13-bit frame_length")
	}
	out := make([]byte, 0, fl)
	out = append(out,
		0xFF,
		b1,
		0x50,                   // profile LC(1)<<6 | sfi 4<<2 | channel_config(2)>>2
		0x80|byte(fl>>11)&0x03, // channel_config(2)&3 <<6 | frame_length>>11
		byte(fl>>3),            // frame_length>>3
		byte(fl&0x07)<<5|0x1F,  // frame_length<<5 | buffer_fullness(0x7FF)>>6
		0xFC,                   // buffer_fullness<<2 | raw_data_blocks 0
	)
	if crc {
		out = append(out, 0xAB, 0xCD)
	}
	for i := 0; i < payloadLen; i++ {
		out = append(out, byte(0x10+i%0xE0))
	}
	return out
}

// frames concatenates n frames of the same payload length.
func frames(n, payloadLen int, crc bool) []byte {
	var out []byte
	for i := 0; i < n; i++ {
		out = append(out, frame(payloadLen, crc)...)
	}
	return out
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// junk returns n pseudo-random bytes from a fixed seed.
func junk(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(r.UintN(256))
	}
	return out
}

// fakeSync is a byte sequence that looks like a valid 7-byte ADTS header
// (frame_length 10) and is planted inside payloads to test robustness.
var fakeSync = []byte{0xFF, 0xF1, 0x50, 0x80, 0x01, 0x5F, 0xFC}

// withFakeSync returns a frame whose payload contains fakeSync at offset off
// within the payload.
func withFakeSync(payloadLen int, off int) []byte {
	f := frame(payloadLen, false)
	copy(f[minHeader+off:], fakeSync)
	return f
}

func TestFrameHelper(t *testing.T) {
	f := frame(100, false)
	if got := len(f); got != 107 {
		t.Fatalf("len = %d, want 107", got)
	}
	fl, ok := parseHeader(f)
	if !ok || fl != 107 {
		t.Fatalf("parseHeader = %d,%v want 107,true", fl, ok)
	}
	c := frame(100, true)
	if got := len(c); got != 109 {
		t.Fatalf("crc len = %d, want 109", got)
	}
	fl, ok = parseHeader(c)
	if !ok || fl != 109 {
		t.Fatalf("crc parseHeader = %d,%v want 109,true", fl, ok)
	}
	if headerLen(c[1]) != crcHeader {
		t.Fatalf("headerLen(crc) = %d, want %d", headerLen(c[1]), crcHeader)
	}
	// payload must be free of sync words
	for i := minHeader; i+1 < len(f); i++ {
		if isSync(f[i], f[i+1]) {
			t.Fatalf("payload contains sync at %d", i)
		}
	}
	fl, ok = parseHeader(fakeSync)
	if !ok || fl != 10 {
		t.Fatalf("fakeSync parseHeader = %d,%v want 10,true", fl, ok)
	}
}

func TestScanTail(t *testing.T) {
	f100 := frame(100, false)
	three := frames(3, 100, false)
	ten := frames(10, 100, false)
	threeCRC := frames(3, 100, true)

	type tc struct {
		name string
		tail []byte
		// size is the absolute file size; 0 means len(tail).
		size int64
		// wantRemoved is the expected number of bytes removed from the end
		// from the end (0 == clean).
		wantRemoved int64
		wantOK      bool
	}

	big := frames(50, 100, false)
	bigPartial := concat(big, f100[:40])

	fake := concat(withFakeSync(100, 30), withFakeSync(100, 30), withFakeSync(100, 30), withFakeSync(100, 30))
	fakePartial := concat(fake, f100[:50])

	cases := []tc{
		{name: "empty", tail: nil, size: 0, wantOK: false},
		{name: "smaller than header", tail: f100[:5], wantOK: false},
		{name: "exactly header, no payload yet (partial only)", tail: f100[:7], wantOK: false},
		{name: "size smaller than tail is rejected", tail: three, size: 10, wantOK: false},

		{name: "clean 3 frames", tail: three, wantOK: true},
		{name: "clean 10 frames", tail: ten, wantOK: true},
		{name: "clean 1 frame whole file", tail: f100, wantOK: true},
		{name: "clean 2 frames whole file", tail: frames(2, 100, false), wantOK: true},
		{name: "clean 1 frame not whole file", tail: f100, size: int64(len(f100)) + 5000, wantOK: false},

		{name: "half frame after 3", tail: concat(three, f100[:50]), wantRemoved: 50, wantOK: true},
		{name: "3 header bytes after 3", tail: concat(three, f100[:3]), wantRemoved: 3, wantOK: true},
		{name: "1 header byte after 3", tail: concat(three, f100[:1]), wantRemoved: 1, wantOK: true},
		{name: "6 header bytes after 3", tail: concat(three, f100[:6]), wantRemoved: 6, wantOK: true},
		{name: "full header only after 3", tail: concat(three, f100[:7]), wantRemoved: 7, wantOK: true},
		{name: "all but last byte after 3", tail: concat(three, f100[:106]), wantRemoved: 106, wantOK: true},
		{name: "partial after 10", tail: concat(ten, f100[:99]), wantRemoved: 99, wantOK: true},

		{name: "1 complete + partial, whole file", tail: concat(f100, f100[:50]), wantRemoved: 50, wantOK: true},
		{name: "2 complete + partial, whole file", tail: concat(f100, f100, f100[:50]), wantRemoved: 50, wantOK: true},
		{name: "1 complete + truncated header, whole file", tail: concat(f100, f100[:3]), wantRemoved: 3, wantOK: true},
		{name: "1 complete + partial, not whole file", tail: concat(f100, f100[:50]), size: int64(157) + 1000, wantOK: false},
		{name: "2 complete + partial, not whole file", tail: concat(f100, f100, f100[:50]), size: int64(264) + 1000, wantOK: false},
		{name: "partial only, whole file", tail: f100[:80], wantOK: false},
		{name: "junk then 1 complete + partial, whole file", tail: concat([]byte{0x00, 0x01, 0x02}, f100, f100[:50]), wantOK: false},

		{name: "trailing garbage after 3 (not a header prefix)", tail: concat(three, []byte{0x00, 0x01}), wantOK: false},
		{name: "trailing garbage after 3 (bad layer bits)", tail: concat(three, []byte{0xFF, 0xF3}), wantOK: false},
		{name: "trailing garbage after 3 (reserved sfi)", tail: concat(three, []byte{0xFF, 0xF1, 0x74}), wantOK: false},
		{name: "trailing header with frame_length < header", tail: concat(three, []byte{0xFF, 0xF1, 0x50, 0x80, 0x00, 0x7F}), wantOK: false},
		{name: "trailing crc header with frame_length 8 (< 9)", tail: concat(three, []byte{0xFF, 0xF0, 0x50, 0x80, 0x01, 0x1F}), wantOK: false},

		{name: "random junk", tail: junk(4096, 1), wantOK: false},
		{name: "random junk 2", tail: junk(70000, 2), wantOK: false},
		{name: "repeated FF F1 (reserved sfi)", tail: bytes.Repeat([]byte{0xFF, 0xF1}, 512), wantOK: false},
		{name: "all FF", tail: bytes.Repeat([]byte{0xFF}, 512), wantOK: false},
		{name: "all zero", tail: make([]byte, 512), wantOK: false},

		{name: "3 frames + junk + 3 frames clean", tail: concat(three, make([]byte, 50), three), wantOK: true},
		{name: "3 frames + junk + 3 frames + partial", tail: concat(three, make([]byte, 50), three, f100[:20]), wantRemoved: 20, wantOK: true},
		{name: "3 frames + junk + 2 frames", tail: concat(three, make([]byte, 50), frames(2, 100, false)), wantOK: false},
		{name: "3 frames + junk + 2 frames + partial", tail: concat(three, make([]byte, 50), frames(2, 100, false), f100[:20]), wantOK: false},
		{name: "3 frames + reserved sfi header + 3 frames", tail: concat(three, []byte{0xFF, 0xF1, 0x74, 0x80, 0x0D, 0x7F, 0xFC}, three), wantOK: true},

		{name: "fake sync in payload, clean", tail: fake, wantOK: true},
		{name: "fake sync in payload, partial", tail: fakePartial, wantRemoved: 50, wantOK: true},
		{name: "fake sync in payload, partial contains fake", tail: concat(fake, withFakeSync(100, 30)[:60]), wantRemoved: 60, wantOK: true},
		{name: "fake sync, tail starts mid-frame before fake", tail: fakePartial[20:], size: int64(len(fakePartial)), wantRemoved: 50, wantOK: true},
		{name: "fake sync, tail starts at fake", tail: fakePartial[minHeader+30:], size: int64(len(fakePartial)), wantRemoved: 50, wantOK: true},
		{name: "fake sync, tail starts after fake", tail: fakePartial[minHeader+40:], size: int64(len(fakePartial)), wantRemoved: 50, wantOK: true},

		{name: "crc clean", tail: threeCRC, wantOK: true},
		{name: "crc partial", tail: concat(threeCRC, frame(100, true)[:60]), wantRemoved: 60, wantOK: true},
		{name: "crc 8 of 9 header bytes", tail: concat(threeCRC, frame(100, true)[:8]), wantRemoved: 8, wantOK: true},
		{name: "crc 2 header bytes", tail: concat(threeCRC, frame(100, true)[:2]), wantRemoved: 2, wantOK: true},
		{name: "crc exactly 9 header bytes", tail: concat(threeCRC, frame(100, true)[:9]), wantRemoved: 9, wantOK: true},
		{name: "mixed crc and non-crc, partial", tail: concat(frame(50, true), frame(70, false), frame(90, true), frame(200, false)[:100]), wantRemoved: 100, wantOK: true},

		{name: "varying sizes clean", tail: concat(frame(1, false), frame(8182, false), frame(400, false), frame(0, false)), wantOK: true},
		{name: "varying sizes partial", tail: concat(frame(1, false), frame(8182, false), frame(400, false), frame(0, true), frame(3000, false)[:2999]), wantRemoved: 2999, wantOK: true},
		{name: "max frame_length partial", tail: concat(three, frame(8184, false)[:100]), wantRemoved: 100, wantOK: true},

		{name: "big file, tail mid-frame, clean", tail: big[33:], size: int64(len(big)), wantOK: true},
		{name: "big file, tail mid-frame, partial", tail: bigPartial[33:], size: int64(len(bigPartial)), wantRemoved: 40, wantOK: true},
		{name: "big file, tail at frame boundary, partial", tail: bigPartial[107*10:], size: int64(len(bigPartial)), wantRemoved: 40, wantOK: true},
		{name: "big file, tail one byte into frame, partial", tail: bigPartial[107*10+1:], size: int64(len(bigPartial)), wantRemoved: 40, wantOK: true},
		{name: "big file, tail one byte before frame boundary, partial", tail: bigPartial[107*10-1:], size: int64(len(bigPartial)), wantRemoved: 40, wantOK: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			size := c.size
			if size == 0 {
				size = int64(len(c.tail))
			}
			got, ok := ScanTail(c.tail, size)
			if ok != c.wantOK {
				t.Fatalf("ScanTail ok = %v, want %v (truncateAt=%d size=%d)", ok, c.wantOK, got, size)
			}
			want := size
			if c.wantOK {
				want = size - c.wantRemoved
			}
			if got != want {
				t.Fatalf("ScanTail truncateAt = %d, want %d (size=%d, removed=%d)", got, want, size, size-got)
			}
			// Invariants.
			if got > size {
				t.Fatalf("truncateAt %d > size %d", got, size)
			}
			if got < size-int64(len(c.tail)) {
				t.Fatalf("truncateAt %d before tail start %d", got, size-int64(len(c.tail)))
			}
			if !ok && got != size {
				t.Fatalf("ok=false must return size (%d), got %d", size, got)
			}
		})
	}
}

// TestScanTailEveryCut cuts a multi-frame file at every possible byte offset
// and checks that the truncation point is always the last frame boundary.
func TestScanTailEveryCut(t *testing.T) {
	parts := [][]byte{frame(30, false), frame(500, true), frame(7, false), frame(0, false), frame(1234, true), frame(64, false)}
	file := concat(parts...)
	boundaries := []int{0}
	for _, p := range parts {
		boundaries = append(boundaries, boundaries[len(boundaries)-1]+len(p))
	}
	lastBoundary := func(cut int) int {
		b := 0
		for _, x := range boundaries {
			if x <= cut {
				b = x
			}
		}
		return b
	}
	for cut := 0; cut <= len(file); cut++ {
		tail := file[:cut]
		size := int64(cut)
		got, ok := ScanTail(tail, size)
		want := lastBoundary(cut)
		if want == 0 {
			// Nothing complete precedes the partial frame: leave untouched.
			if ok || got != size {
				t.Fatalf("cut %d: got (%d,%v), want (%d,false)", cut, got, ok, size)
			}
			continue
		}
		if !ok || got != int64(want) {
			t.Fatalf("cut %d: got (%d,%v), want (%d,true)", cut, got, ok, want)
		}
	}
}

func writeTemp(t *testing.T, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func statSize(t *testing.T, p string) int64 {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func TestTrimPartialTail(t *testing.T) {
	five := frames(5, 300, false)
	half := frame(300, false)[:150]

	t.Run("clean file untouched", func(t *testing.T) {
		p := writeTemp(t, "clean.aac", five)
		mtime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		removed, err := TrimPartialTail(p)
		if err != nil {
			t.Fatal(err)
		}
		if removed != 0 {
			t.Fatalf("removed = %d, want 0", removed)
		}
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() != int64(len(five)) {
			t.Fatalf("size = %d, want %d", fi.Size(), len(five))
		}
		if !fi.ModTime().Equal(mtime) {
			t.Fatalf("mtime changed: %v -> %v", mtime, fi.ModTime())
		}
		got, _ := os.ReadFile(p)
		if !bytes.Equal(got, five) {
			t.Fatal("content changed")
		}
	})

	t.Run("partial frame trimmed", func(t *testing.T) {
		p := writeTemp(t, "dirty.aac", concat(five, half))
		removed, err := TrimPartialTail(p)
		if err != nil {
			t.Fatal(err)
		}
		if removed != int64(len(half)) {
			t.Fatalf("removed = %d, want %d", removed, len(half))
		}
		if got := statSize(t, p); got != int64(len(five)) {
			t.Fatalf("on-disk size = %d, want %d", got, len(five))
		}
		got, _ := os.ReadFile(p)
		if !bytes.Equal(got, five) {
			t.Fatal("content after trim differs from the complete frames")
		}
		// Second call is a no-op.
		removed, err = TrimPartialTail(p)
		if err != nil || removed != 0 {
			t.Fatalf("second call = (%d,%v), want (0,nil)", removed, err)
		}
	})

	t.Run("truncated header trimmed", func(t *testing.T) {
		p := writeTemp(t, "hdr.aac", concat(five, half[:3]))
		removed, err := TrimPartialTail(p)
		if err != nil {
			t.Fatal(err)
		}
		if removed != 3 {
			t.Fatalf("removed = %d, want 3", removed)
		}
		if got := statSize(t, p); got != int64(len(five)) {
			t.Fatalf("on-disk size = %d, want %d", got, len(five))
		}
	})

	t.Run("single complete frame plus partial", func(t *testing.T) {
		one := frame(300, false)
		p := writeTemp(t, "one.aac", concat(one, half))
		removed, err := TrimPartialTail(p)
		if err != nil {
			t.Fatal(err)
		}
		if removed != int64(len(half)) {
			t.Fatalf("removed = %d, want %d", removed, len(half))
		}
		if got := statSize(t, p); got != int64(len(one)) {
			t.Fatalf("on-disk size = %d, want %d", got, len(one))
		}
	})

	t.Run("empty file", func(t *testing.T) {
		p := writeTemp(t, "empty.aac", nil)
		removed, err := TrimPartialTail(p)
		if err != nil || removed != 0 {
			t.Fatalf("got (%d,%v), want (0,nil)", removed, err)
		}
		if got := statSize(t, p); got != 0 {
			t.Fatalf("size = %d, want 0", got)
		}
	})

	t.Run("tiny file", func(t *testing.T) {
		p := writeTemp(t, "tiny.aac", []byte{0xFF, 0xF1, 0x50})
		removed, err := TrimPartialTail(p)
		if err != nil || removed != 0 {
			t.Fatalf("got (%d,%v), want (0,nil)", removed, err)
		}
		if got := statSize(t, p); got != 3 {
			t.Fatalf("size = %d, want 3", got)
		}
	})

	t.Run("junk file untouched", func(t *testing.T) {
		data := junk(10000, 7)
		p := writeTemp(t, "junk.aac", data)
		removed, err := TrimPartialTail(p)
		if err != nil || removed != 0 {
			t.Fatalf("got (%d,%v), want (0,nil)", removed, err)
		}
		got, _ := os.ReadFile(p)
		if !bytes.Equal(got, data) {
			t.Fatal("junk file was modified")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := TrimPartialTail(filepath.Join(t.TempDir(), "nope.aac"))
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("err = %v, want fs.ErrNotExist", err)
		}
	})

	t.Run("file larger than tail window, partial", func(t *testing.T) {
		// 1007-byte frames; 300 of them exceed tailWindow and the window
		// start falls mid-frame.
		body := frames(300, 1000, false)
		if len(body) <= tailWindow {
			t.Fatalf("test file %d bytes not larger than tailWindow %d", len(body), tailWindow)
		}
		if (len(body)-tailWindow)%1007 == 0 {
			t.Fatal("window start unexpectedly frame-aligned; pick another frame size")
		}
		partial := frame(1000, false)[:777]
		p := writeTemp(t, "big.aac", concat(body, partial))
		removed, err := TrimPartialTail(p)
		if err != nil {
			t.Fatal(err)
		}
		if removed != 777 {
			t.Fatalf("removed = %d, want 777", removed)
		}
		if got := statSize(t, p); got != int64(len(body)) {
			t.Fatalf("on-disk size = %d, want %d", got, len(body))
		}
		got, _ := os.ReadFile(p)
		if !bytes.Equal(got, body) {
			t.Fatal("content after trim differs")
		}
	})

	t.Run("file larger than tail window, clean", func(t *testing.T) {
		body := frames(300, 1000, false)
		p := writeTemp(t, "bigclean.aac", body)
		removed, err := TrimPartialTail(p)
		if err != nil || removed != 0 {
			t.Fatalf("got (%d,%v), want (0,nil)", removed, err)
		}
		if got := statSize(t, p); got != int64(len(body)) {
			t.Fatalf("size = %d, want %d", got, len(body))
		}
	})

	t.Run("window larger than file", func(t *testing.T) {
		body := frames(20, 100, false)
		if len(body) >= tailWindow {
			t.Fatal("body should be smaller than tailWindow")
		}
		p := writeTemp(t, "small.aac", concat(body, frame(100, false)[:33]))
		removed, err := TrimPartialTail(p)
		if err != nil {
			t.Fatal(err)
		}
		if removed != 33 {
			t.Fatalf("removed = %d, want 33", removed)
		}
		if got := statSize(t, p); got != int64(len(body)) {
			t.Fatalf("size = %d, want %d", got, len(body))
		}
	})
}
