// Package id3 writes and parses the minimal subset of ID3v2 the recorder used
// to embed a wall clock in the ADTS/AAC stream between frames.
//
// Before 1.1.0, the recorder inserted small ID3v2.4.0 tags between ADTS
// frames: an Apple PRIV "transportStreamTimestamp" frame (what hls.js turned
// into basePTS) plus two TXXX frames carrying the human-readable wall clock
// and its source. These were removed in 1.1.0 because the in-band tags
// perturbed raw-ADTS duration estimation and HLS players read the Apple PRIV
// tag as a PTS base. TagLen/Parse remain in use: internal/adts uses them to
// skip legacy tags in captures that were started before the upgrade and
// resumed across it. Tag/AppleTimestamp/TXXX remain only to build
// legacy-format fixtures in tests (internal/remux and internal/recorder tests
// use them); do not use them for new writes.
//
// ID3v2 tag layout used:
//
//	"ID3" 0x04 0x00 flags(0x00) size(syncsafe, 4 bytes)   // 10-byte header
//	<frame> ...                                            // no extended header,
//	                                                       // no footer, no unsync
//
// ID3v2.4 frame layout:
//
//	ID(4 bytes) size(syncsafe, 4 bytes) flags(0x00 0x00) data
package id3

import (
	"errors"
	"fmt"
)

// appleTimestampOwner is the PRIV owner string Apple defined for the 33-bit
// 90 kHz transport-stream timestamp carried in packed-audio HLS segments.
const appleTimestampOwner = "com.apple.streaming.transportStreamTimestamp"

// Frame is one ID3v2 frame: a four-character ID and its raw frame data (the
// bytes after the frame header, i.e. what a frame's size field counts).
type Frame struct {
	ID   string
	Data []byte
}

// TXXX builds a user-defined text frame: encoding byte 0x03 (UTF-8), the
// description, a 0x00 terminator, then the value. Per the spec the value has no
// trailing terminator (it runs to the end of the frame).
func TXXX(description, value string) Frame {
	data := make([]byte, 0, 1+len(description)+1+len(value))
	data = append(data, 0x03) // UTF-8
	data = append(data, description...)
	data = append(data, 0x00)
	data = append(data, value...)
	return Frame{ID: "TXXX", Data: data}
}

// PRIV builds a private frame: the owner identifier, a 0x00 terminator, then the
// binary payload.
func PRIV(owner string, payload []byte) Frame {
	data := make([]byte, 0, len(owner)+1+len(payload))
	data = append(data, owner...)
	data = append(data, 0x00)
	data = append(data, payload...)
	return Frame{ID: "PRIV", Data: data}
}

// AppleTimestamp builds the Apple PRIV timestamp frame: an 8-byte big-endian
// payload holding the 33-bit 90 kHz PTS.
func AppleTimestamp(pts90k uint64) Frame {
	pts90k &= (1 << 33) - 1
	var buf [8]byte
	for i := 7; i >= 0; i-- {
		buf[i] = byte(pts90k)
		pts90k >>= 8
	}
	return PRIV(appleTimestampOwner, buf[:])
}

// Tag renders a complete ID3v2.4.0 tag holding the given frames.
func Tag(frames ...Frame) []byte {
	var body []byte
	for _, f := range frames {
		id := f.ID
		if len(id) != 4 {
			// Pad/truncate defensively; callers use 4-char IDs.
			id = (id + "\x00\x00\x00\x00")[:4]
		}
		body = append(body, id...)
		body = append(body, syncsafe(len(f.Data))...)
		body = append(body, 0x00, 0x00) // frame flags
		body = append(body, f.Data...)
	}
	out := make([]byte, 0, 10+len(body))
	out = append(out, 'I', 'D', '3', 0x04, 0x00, 0x00)
	out = append(out, syncsafe(len(body))...)
	out = append(out, body...)
	return out
}

// TagLen recognises an ID3v2 tag header at the start of b and returns the total
// tag length (header + body, plus a footer when present). ok is false when b
// does not begin with a valid ID3v2 (2..4) header.
func TagLen(b []byte) (n int, ok bool) {
	if len(b) < 10 {
		return 0, false
	}
	if b[0] != 'I' || b[1] != 'D' || b[2] != '3' {
		return 0, false
	}
	if b[3] < 2 || b[3] > 4 || b[4] == 0xFF {
		return 0, false
	}
	// The size is a 28-bit syncsafe integer: every size byte has its high bit 0.
	if b[6]&0x80 != 0 || b[7]&0x80 != 0 || b[8]&0x80 != 0 || b[9]&0x80 != 0 {
		return 0, false
	}
	size := desyncsafe(b[6:10])
	total := 10 + size
	if b[5]&0x10 != 0 { // footer present (only valid in v2.4)
		total += 10
	}
	return total, true
}

// Parse extracts the frames of an ID3v2 tag. It handles v2.4 (syncsafe frame
// sizes) and v2.3 (plain big-endian frame sizes); it is intentionally minimal
// (no unsynchronisation, no extended header, no compression). Padding (a frame
// whose ID begins with a zero byte) ends the frame list.
func Parse(b []byte) ([]Frame, error) {
	if len(b) < 10 || b[0] != 'I' || b[1] != 'D' || b[2] != '3' {
		return nil, errors.New("id3: not an ID3v2 tag")
	}
	major := b[3]
	if major < 2 || major > 4 || b[4] == 0xFF {
		return nil, fmt.Errorf("id3: unsupported version %d", major)
	}
	if major == 2 {
		// v2.2 uses 3-byte frame IDs; not emitted by this package and not needed.
		return nil, errors.New("id3: v2.2 frames not supported")
	}
	total, ok := TagLen(b)
	if !ok {
		return nil, errors.New("id3: bad tag header")
	}
	if total > len(b) {
		total = len(b)
	}
	syncsafeFrames := major >= 4
	var frames []Frame
	p := 10
	for p+10 <= total {
		if b[p] == 0 { // padding
			break
		}
		id := string(b[p : p+4])
		var size int
		if syncsafeFrames {
			if b[p+4]&0x80 != 0 || b[p+5]&0x80 != 0 || b[p+6]&0x80 != 0 || b[p+7]&0x80 != 0 {
				return frames, fmt.Errorf("id3: frame %q has a non-syncsafe size", id)
			}
			size = desyncsafe(b[p+4 : p+8])
		} else {
			size = int(b[p+4])<<24 | int(b[p+5])<<16 | int(b[p+6])<<8 | int(b[p+7])
		}
		p += 10 // frame header
		if size < 0 || p+size > total {
			return frames, fmt.Errorf("id3: frame %q size %d overflows tag", id, size)
		}
		data := make([]byte, size)
		copy(data, b[p:p+size])
		frames = append(frames, Frame{ID: id, Data: data})
		p += size
	}
	return frames, nil
}

// syncsafe encodes n as a 4-byte syncsafe integer (7 bits per byte, high bit 0).
func syncsafe(n int) []byte {
	return []byte{
		byte(n >> 21 & 0x7F),
		byte(n >> 14 & 0x7F),
		byte(n >> 7 & 0x7F),
		byte(n & 0x7F),
	}
}

// desyncsafe decodes a 4-byte syncsafe integer.
func desyncsafe(b []byte) int {
	return int(b[0]&0x7F)<<21 | int(b[1]&0x7F)<<14 | int(b[2]&0x7F)<<7 | int(b[3]&0x7F)
}
