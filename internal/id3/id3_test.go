package id3

import (
	"bytes"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestTagByteExact(t *testing.T) {
	got := Tag(TXXX("WALLCLOCK", "x"))
	want := []byte{
		'I', 'D', '3', 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x16, // header, body size = 22
		'T', 'X', 'X', 'X', 0x00, 0x00, 0x00, 0x0C, 0x00, 0x00, // frame TXXX, size = 12
		0x03, 'W', 'A', 'L', 'L', 'C', 'L', 'O', 'C', 'K', 0x00, 'x', // frame data
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Tag bytes:\n got %v\nwant %v", got, want)
	}
	// The header's syncsafe size must equal the body length.
	if n := desyncsafe(got[6:10]); n != len(got)-10 {
		t.Fatalf("header size = %d, want %d", n, len(got)-10)
	}
}

func TestRoundTrip(t *testing.T) {
	in := []Frame{
		TXXX("WALLCLOCK", "2026-09-17T10:00:00.500Z"),
		TXXX("WALLCLOCK-SOURCE", "hls-pdt"),
		AppleTimestamp(0x1_2345_6789),
		PRIV("owner", []byte{0, 1, 2, 3, 0xFF}),
	}
	raw := Tag(in...)
	if n, ok := TagLen(raw); !ok || n != len(raw) {
		t.Fatalf("TagLen = %d,%v want %d,true", n, ok, len(raw))
	}
	out, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("got %d frames, want %d", len(out), len(in))
	}
	for i := range in {
		if out[i].ID != in[i].ID || !bytes.Equal(out[i].Data, in[i].Data) {
			t.Fatalf("frame %d = %q %v, want %q %v", i, out[i].ID, out[i].Data, in[i].ID, in[i].Data)
		}
	}
}

func TestAppleTimestamp(t *testing.T) {
	const owner = "com.apple.streaming.transportStreamTimestamp"
	f := AppleTimestamp(90000)
	if f.ID != "PRIV" {
		t.Fatalf("id = %q", f.ID)
	}
	prefix := append([]byte(owner), 0x00)
	if !bytes.HasPrefix(f.Data, prefix) {
		t.Fatalf("owner not present: %v", f.Data)
	}
	payload := f.Data[len(prefix):]
	if len(payload) != 8 {
		t.Fatalf("payload len = %d, want 8", len(payload))
	}
	if got := binary.BigEndian.Uint64(payload); got != 90000 {
		t.Fatalf("payload = %d, want 90000", got)
	}
	// Values are masked to 33 bits.
	masked := AppleTimestamp(1 << 40)
	if got := binary.BigEndian.Uint64(masked.Data[len(prefix):]); got != 0 {
		t.Fatalf("1<<40 should mask to 0, got %d", got)
	}
	big := AppleTimestamp((1 << 33) - 1)
	if got := binary.BigEndian.Uint64(big.Data[len(prefix):]); got != (1<<33)-1 {
		t.Fatalf("max 33-bit = %d, want %d", got, uint64((1<<33)-1))
	}
}

func TestTagLenFooterAndVersions(t *testing.T) {
	// A v2.3 tag with the footer flag set adds 10 bytes.
	hdr := []byte{'I', 'D', '3', 0x04, 0x00, 0x10, 0x00, 0x00, 0x01, 0x00} // footer flag, size = 128
	n, ok := TagLen(hdr)
	if !ok || n != 10+128+10 {
		t.Fatalf("footer TagLen = %d,%v want %d,true", n, ok, 10+128+10)
	}
	// No footer.
	hdr[5] = 0x00
	if n, ok := TagLen(hdr); !ok || n != 10+128 {
		t.Fatalf("no-footer TagLen = %d,%v want %d,true", n, ok, 10+128)
	}
	// Version 2 and 3 are recognised for skipping.
	for _, v := range []byte{2, 3, 4} {
		hdr[3] = v
		if _, ok := TagLen(hdr); !ok {
			t.Fatalf("version %d should be recognised", v)
		}
	}
}

func TestTagLenAndParseRejectGarbage(t *testing.T) {
	bad := [][]byte{
		nil,
		[]byte("ID"),
		[]byte("ID3"),
		[]byte("XYZ\x04\x00\x00\x00\x00\x00\x00"),           // wrong magic
		[]byte("ID3\xff\x00\x00\x00\x00\x00\x00"),           // version 0xFF
		[]byte("ID3\x04\x00\x00\x80\x00\x00\x00"),           // non-syncsafe size byte
		bytes.Repeat([]byte{0xFF}, 32),                      // random
		append([]byte("ID3\x04\x00\x00"), 0x00, 0x00, 0x00), // too short (9 bytes)
	}
	for i, b := range bad {
		if _, ok := TagLen(b); ok {
			t.Errorf("TagLen(case %d) = ok, want false", i)
		}
		if _, err := Parse(b); err == nil {
			t.Errorf("Parse(case %d) = nil error, want error", i)
		}
	}
}

// TestParseV23 checks a v2.3 tag (plain big-endian frame sizes).
func TestParseV23(t *testing.T) {
	frameData := TXXX("K", "v").Data
	var body []byte
	body = append(body, "TXXX"...)
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(frameData)))
	body = append(body, size[:]...)
	body = append(body, 0x00, 0x00)
	body = append(body, frameData...)
	tag := append([]byte{'I', 'D', '3', 0x03, 0x00, 0x00}, syncsafe(len(body))...)
	tag = append(tag, body...)

	frames, err := Parse(tag)
	if err != nil {
		t.Fatalf("Parse v2.3: %v", err)
	}
	if len(frames) != 1 || frames[0].ID != "TXXX" || !bytes.Equal(frames[0].Data, frameData) {
		t.Fatalf("v2.3 frames = %+v", frames)
	}
}

// TestFFprobeReadsWallclock is an optional integration test: it writes a few
// real ADTS frames with an ID3 tag between them and checks that ffprobe reports
// TAG:WALLCLOCK. It is skipped when ffprobe is not on PATH.
func TestFFprobeReadsWallclock(t *testing.T) {
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe not on PATH")
	}
	tag := Tag(
		AppleTimestamp(90000),
		TXXX("WALLCLOCK", "2026-09-17T10:00:00.000Z"),
		TXXX("WALLCLOCK-SOURCE", "hls-pdt"),
	)
	var file []byte
	file = append(file, adtsFrame()...)
	file = append(file, tag...)
	for i := 0; i < 20; i++ {
		file = append(file, adtsFrame()...)
	}
	p := filepath.Join(t.TempDir(), "tagged.aac")
	if err := os.WriteFile(p, file, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(ffprobe, "-v", "error", "-show_format", p).CombinedOutput()
	if err != nil {
		t.Fatalf("ffprobe: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte("TAG:WALLCLOCK=")) {
		t.Fatalf("ffprobe did not report the WALLCLOCK tag:\n%s", out)
	}
}

// adtsFrame builds a small, silent-ish ADTS frame (AAC-LC, 44.1 kHz, stereo)
// with a 32-byte payload. It is only meant to be structurally valid for the
// demuxer, not to decode to meaningful audio.
func adtsFrame() []byte {
	const payload = 32
	fl := 7 + payload
	out := []byte{
		0xFF, 0xF1,
		0x50,                     // profile LC(1)<<6 | sfi 4<<2 | ...
		0x80 | byte(fl>>11&0x03), // channel_config(2)&3 <<6 | frame_length>>11
		byte(fl >> 3),
		byte(fl&0x07)<<5 | 0x1F,
		0xFC,
	}
	for i := 0; i < payload; i++ {
		out = append(out, byte(0x10+i%0xE0))
	}
	return out
}
