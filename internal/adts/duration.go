package adts

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"time"

	"github.com/spectado/stream-recorder/internal/id3"
)

// sampleRates maps sampling_frequency_index to Hz (indexes 13..15 are
// reserved/escape and rejected by parseHeader).
var sampleRates = [...]int{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}

// Info describes the frames found in an ADTS file.
type Info struct {
	Frames   int           // complete frames
	Duration time.Duration // playback time of those frames
	Junk     int64         // bytes that were not part of a complete frame or tag
	Tags     int           // well-formed ID3v2 tags skipped
	TagBytes int64         // bytes those tags occupied
}

// Scan walks the whole file and derives its playback duration from the frame
// headers (1024 samples per raw data block at the header's sampling rate).
// ADTS carries no timestamps, so this is the only exact way to know how long
// a recording is; the file is read once sequentially.
//
// Well-formed ID3v2 tags (which the recorder writes between frames) are skipped
// and counted in Tags/TagBytes, not Junk. Bytes that do not form a valid frame
// are skipped one at a time and counted in Junk; after such a gap a candidate
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
	seconds := 0.0
	synced := false
	for {
		// An ID3v2 tag may appear anywhere (at the start of the file and between
		// frames). Skip it and count it separately from junk. A tag breaks frame
		// adjacency, so re-sync afterwards.
		if hdr, err := r.Peek(10); err == nil && bytes.HasPrefix(hdr, []byte("ID3")) {
			if n, ok := id3.TagLen(hdr); ok {
				d, derr := r.Discard(n)
				info.Tags++
				info.TagBytes += int64(d)
				if derr != nil {
					if !errors.Is(derr, io.EOF) {
						return info, derr
					}
					break // truncated tag at EOF
				}
				synced = false
				continue
			}
		}

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
		// Peek the whole frame, plus the following header when resyncing. A
		// Peek may slide the buffer and invalidate earlier slices (hdr!), so
		// from here on only buf is read.
		want := frameLen
		if !synced {
			want += minHeader
		}
		buf, err := r.Peek(want)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return info, err
			}
			if len(buf) < frameLen {
				// Truncated final frame (or junk that looked like a header).
				info.Junk += int64(len(buf))
				break
			}
			if len(buf) != frameLen {
				// Followed by a few stray bytes: cannot be a frame boundary.
				skip(r, &info)
				continue
			}
			// Exactly one frame left: a clean end of file.
		} else if !synced {
			if _, ok := parseHeader(buf[frameLen:]); !ok {
				skip(r, &info)
				continue
			}
		}
		synced = true
		hdr = buf[:minHeader]
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

// ScanRuns walks the file once and attributes every frame, tag and junk byte to
// the run whose byte range contains that element's first byte. offsets holds the
// first byte of each run (ascending); run i covers [offsets[i], offsets[i+1]),
// the last run extends to EOF, and any bytes before offsets[0] fall in run 0. It
// returns one Info per run plus the totals. Used at finalize to give every
// #EXT-X-BYTERANGE segment its authoritative frame count and duration.
func ScanRuns(path string, offsets []int64) (runs []Info, total Info, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, Info{}, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 256<<10)

	n := len(offsets)
	if n == 0 {
		n = 1 // a single implicit run covering the whole file
	}
	runs = make([]Info, n)
	runSeconds := make([]float64, n)

	cur := 0
	idxFor := func(pos int64) int {
		for cur+1 < len(offsets) && pos >= offsets[cur+1] {
			cur++
		}
		return cur
	}

	var pos int64
	synced := false
	for {
		if hdr, e := r.Peek(10); e == nil && bytes.HasPrefix(hdr, []byte("ID3")) {
			if tl, ok := id3.TagLen(hdr); ok {
				idx := idxFor(pos)
				d, derr := r.Discard(tl)
				runs[idx].Tags++
				runs[idx].TagBytes += int64(d)
				pos += int64(d)
				if derr != nil {
					if !errors.Is(derr, io.EOF) {
						return runs, Info{}, derr
					}
					break
				}
				synced = false
				continue
			}
		}

		hdr, e := r.Peek(minHeader)
		if e != nil {
			if !errors.Is(e, io.EOF) {
				return runs, Info{}, e
			}
			runs[idxFor(pos)].Junk += int64(len(hdr))
			pos += int64(len(hdr))
			break
		}
		frameLen, ok := parseHeader(hdr)
		if !ok {
			idx := idxFor(pos)
			if d, _ := r.Discard(1); d > 0 {
				runs[idx].Junk += int64(d)
				pos += int64(d)
			}
			synced = false
			continue
		}
		want := frameLen
		if !synced {
			want += minHeader
		}
		buf, e := r.Peek(want)
		if e != nil {
			if !errors.Is(e, io.EOF) {
				return runs, Info{}, e
			}
			if len(buf) < frameLen {
				runs[idxFor(pos)].Junk += int64(len(buf))
				pos += int64(len(buf))
				break
			}
			if len(buf) != frameLen {
				idx := idxFor(pos)
				if d, _ := r.Discard(1); d > 0 {
					runs[idx].Junk += int64(d)
					pos += int64(d)
				}
				continue
			}
		} else if !synced {
			if _, ok := parseHeader(buf[frameLen:]); !ok {
				idx := idxFor(pos)
				if d, _ := r.Discard(1); d > 0 {
					runs[idx].Junk += int64(d)
					pos += int64(d)
				}
				continue
			}
		}
		synced = true
		idx := idxFor(pos)
		rate := sampleRates[(buf[2]>>2)&0x0F]
		blocks := int(buf[6]&0x03) + 1
		runSeconds[idx] += float64(1024*blocks) / float64(rate)
		runs[idx].Frames++
		d, derr := r.Discard(frameLen)
		pos += int64(d)
		if derr != nil {
			return runs, Info{}, derr
		}
	}

	var totalSeconds float64
	for i := range runs {
		runs[i].Duration = time.Duration(runSeconds[i] * float64(time.Second))
		total.Frames += runs[i].Frames
		total.Junk += runs[i].Junk
		total.Tags += runs[i].Tags
		total.TagBytes += runs[i].TagBytes
		totalSeconds += runSeconds[i]
	}
	total.Duration = time.Duration(totalSeconds * float64(time.Second))
	return runs, total, nil
}
