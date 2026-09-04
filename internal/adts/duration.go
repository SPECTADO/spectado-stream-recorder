package adts

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"time"
)

// sampleRates maps sampling_frequency_index to Hz (indexes 13..15 are
// reserved/escape and rejected by parseHeader).
var sampleRates = [...]int{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}

// Info describes the frames found in an ADTS file.
type Info struct {
	Frames   int           // complete frames
	Duration time.Duration // playback time of those frames
	Junk     int64         // bytes that were not part of a complete frame
}

// Scan walks the whole file and derives its playback duration from the frame
// headers (1024 samples per raw data block at the header's sampling rate).
// ADTS carries no timestamps, so this is the only exact way to know how long
// a recording is; the file is read once sequentially.
//
// A leading ID3v2 tag is skipped. Bytes that do not form a valid frame are
// skipped one at a time and counted in Junk; after such a gap a candidate
// frame is only trusted when the frame following it is valid too.
func Scan(path string) (Info, error) {
	f, err := os.Open(path)
	if err != nil {
		return Info{}, err
	}
	defer f.Close()
	return scan(bufio.NewReaderSize(f, 256<<10))
}

func scan(r *bufio.Reader) (Info, error) {
	var info Info
	if hdr, err := r.Peek(10); err == nil && bytes.HasPrefix(hdr, []byte("ID3")) {
		size := int(hdr[6]&0x7F)<<21 | int(hdr[7]&0x7F)<<14 | int(hdr[8]&0x7F)<<7 | int(hdr[9]&0x7F)
		size += 10
		if hdr[5]&0x10 != 0 { // footer present
			size += 10
		}
		if n, err := r.Discard(size); err != nil {
			info.Junk += int64(n)
			return info, nil
		}
	}

	seconds := 0.0
	synced := false
	for {
		hdr, err := r.Peek(minHeader)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return info, err
			}
			info.Junk += int64(len(hdr)) // partial trailing header
			break
		}
		frameLen, ok := parseHeader(hdr)
		if !ok {
			skip(r, &info)
			synced = false
			continue
		}
		frame, err := r.Peek(frameLen)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return info, err
			}
			// Truncated final frame (or junk that looked like a header).
			info.Junk += int64(len(frame))
			break
		}
		if !synced {
			next, err := r.Peek(frameLen + minHeader)
			if err == nil {
				if _, ok := parseHeader(next[frameLen:]); !ok {
					skip(r, &info)
					continue
				}
			} else if !errors.Is(err, io.EOF) {
				return info, err
			} else if len(next) != frameLen {
				// Followed by a few stray bytes: cannot be a frame boundary.
				skip(r, &info)
				continue
			}
			synced = true
		}
		rate := sampleRates[(hdr[2]>>2)&0x0F]
		blocks := int(hdr[6]&0x03) + 1
		seconds += float64(1024*blocks) / float64(rate)
		info.Frames++
		if _, err := r.Discard(frameLen); err != nil {
			return info, err
		}
	}
	info.Duration = time.Duration(seconds * float64(time.Second))
	return info, nil
}

func skip(r *bufio.Reader, info *Info) {
	if n, _ := r.Discard(1); n > 0 {
		info.Junk += int64(n)
	}
}
