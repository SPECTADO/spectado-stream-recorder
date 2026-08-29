// Package adts repairs the tail of ADTS (AAC) files.
//
// The recorder appends ADTS frames coming from ffmpeg's stdout to a file. If
// the recorder is killed mid-write the file may end with a partial frame.
// Before appends resume, TrimPartialTail cuts the file back to the end of the
// last complete frame so the file stays a clean concatenation of frames.
//
// ADTS header layout (7 bytes, or 9 when a CRC is present):
//
//	byte 0      : 0xFF                      sync word (high 8 bits)
//	byte 1      : 0xF0 | ID<<3 | layer<<1 | protection_absent
//	byte 2      : profile<<6 | sampling_frequency_index<<2 | private<<1 | channel_config>>2
//	byte 3      : channel_config<<6 | orig<<5 | home<<4 | cid_bit<<3 | cid_start<<2 | frame_length>>11
//	byte 4      : frame_length>>3
//	byte 5      : frame_length<<5 | buffer_fullness>>6
//	byte 6      : buffer_fullness<<2 | number_of_raw_data_blocks
//	bytes 7..8  : CRC (only when protection_absent == 0)
//
// frame_length includes the header.
package adts

import (
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	// tailWindow is the maximum number of bytes inspected at the end of a
	// file. ADTS frames are at most 8191 bytes, so a full window always holds
	// dozens of frames.
	tailWindow = 256 << 10

	// minHeader is the size of an ADTS header without CRC.
	minHeader = 7
	// crcHeader is the size of an ADTS header with CRC.
	crcHeader = 9

	// maxSamplingIndex is the largest valid sampling_frequency_index; 13 and
	// 14 are reserved and 15 is the escape value, none of which ffmpeg emits.
	maxSamplingIndex = 12

	// minChain is the number of consecutive complete frames a candidate
	// chain must contain before it is trusted when the tail does not cover
	// the whole file.
	minChain = 3
)

// TrimPartialTail inspects the end of the ADTS file at path and, if the file
// ends with an incomplete frame, truncates it to the end of the last complete
// frame. It returns the number of bytes removed (0 when the file was clean or
// too small/unrecognisable to judge). It never grows the file and never
// removes more than tailWindow bytes; if no valid frame chain can be found in
// the tail window the file is left untouched and removed == 0, err == nil.
//
// Errors from opening, stating, reading or truncating the file are returned
// as-is (a missing file yields an error satisfying errors.Is(err, fs.ErrNotExist)).
func TrimPartialTail(path string) (removed int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := fi.Size()
	if size < minHeader {
		return 0, nil
	}

	n := size
	if n > tailWindow {
		n = tailWindow
	}
	buf := make([]byte, n)
	m, err := f.ReadAt(buf, size-n)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if int64(m) != n {
		return 0, fmt.Errorf("adts: short read of %s: got %d of %d bytes", path, m, n)
	}

	truncateAt, ok := ScanTail(buf, size)
	if !ok || truncateAt >= size {
		return 0, nil
	}

	// Release the read handle before truncating; harmless on POSIX, required
	// on platforms that refuse to truncate an open file.
	if err := f.Close(); err != nil {
		return 0, err
	}
	if err := os.Truncate(path, truncateAt); err != nil {
		return 0, err
	}
	return size - truncateAt, nil
}

// ScanTail is the pure function behind TrimPartialTail: given the last bytes
// of a file (tail) and the absolute file size, it returns the absolute offset
// at which the file should be truncated (== size when the tail is clean) and
// ok=false when no trustworthy frame chain was found.
//
// A chain of frames is trusted when it contains at least minChain complete
// frames, or when it starts at offset 0 of the file (the tail covers the whole
// file) and contains at least one complete frame. Candidates are tried from
// the earliest offset, so the longest chain wins. The result always satisfies
// size-len(tail) <= truncateAt <= size.
func ScanTail(tail []byte, size int64) (truncateAt int64, ok bool) {
	n := len(tail)
	if size < minHeader || n < minHeader || int64(n) > size {
		return size, false
	}
	wholeFile := int64(n) == size

	for p := 0; p+1 < n; p++ {
		if !isSync(tail[p], tail[p+1]) {
			continue
		}
		complete, partialStart, valid := walk(tail, p)
		if !valid {
			continue
		}
		trusted := complete >= minChain || (wholeFile && p == 0 && complete >= 1)
		if !trusted {
			continue
		}
		if partialStart < 0 {
			return size, true
		}
		return size - int64(n-partialStart), true
	}
	return size, false
}

// walk follows consecutive frames in tail starting at start. It returns the
// number of complete frames seen and the offset of a trailing partial frame
// (-1 when the chain ends exactly at the end of tail). valid is false when a
// byte sequence that cannot be an ADTS header is encountered before the end
// of tail, i.e. the chain is broken.
func walk(tail []byte, start int) (complete int, partialStart int, valid bool) {
	q := start
	for q < len(tail) {
		rest := tail[q:]
		if len(rest) < minHeader {
			// Truncated header: a partial frame if the visible bytes are
			// consistent with an ADTS header, otherwise garbage.
			if !headerPrefixValid(rest) {
				return 0, -1, false
			}
			return complete, q, true
		}
		frameLen, ok := parseHeader(rest)
		if !ok {
			return 0, -1, false
		}
		if frameLen > len(rest) {
			return complete, q, true
		}
		complete++
		q += frameLen
	}
	return complete, -1, true
}

// isSync reports whether b0,b1 carry the 12-bit sync word 0xFFF followed by
// layer bits 00.
func isSync(b0, b1 byte) bool {
	return b0 == 0xFF && b1&0xF6 == 0xF0
}

// headerLen returns the header size implied by byte 1 (protection_absent).
func headerLen(b1 byte) int {
	if b1&0x01 == 0 {
		return crcHeader
	}
	return minHeader
}

// frameLength decodes the 13-bit frame_length field from bytes 3..5.
func frameLength(b []byte) int {
	return int(b[3]&0x03)<<11 | int(b[4])<<3 | int(b[5])>>5
}

// parseHeader validates a full ADTS header at the start of b (len(b) >= 7)
// and returns its frame_length (header included).
func parseHeader(b []byte) (frameLen int, ok bool) {
	if !isSync(b[0], b[1]) {
		return 0, false
	}
	if (b[2]>>2)&0x0F > maxSamplingIndex {
		return 0, false
	}
	frameLen = frameLength(b)
	if frameLen < headerLen(b[1]) {
		return 0, false
	}
	return frameLen, true
}

// headerPrefixValid reports whether b (1 to 6 bytes) could be the beginning
// of a valid ADTS header, checking only the fields that are present.
func headerPrefixValid(b []byte) bool {
	if len(b) == 0 || b[0] != 0xFF {
		return false
	}
	if len(b) >= 2 && b[1]&0xF6 != 0xF0 {
		return false
	}
	if len(b) >= 3 && (b[2]>>2)&0x0F > maxSamplingIndex {
		return false
	}
	if len(b) >= 6 && frameLength(b) < headerLen(b[1]) {
		return false
	}
	return true
}
