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

// headerParams decodes the stream parameters of an ADTS header that parseHeader
// has already validated (len(b) >= minHeader). It is the allocation-free path
// the scanners take for every frame; ParseHeader is the validating public
// equivalent and shares this decoding.
func headerParams(b []byte) Params {
	idx := int(b[2]>>2) & 0x0F
	return Params{
		Profile:         int(b[2] >> 6),
		SampleRateIndex: idx,
		SampleRate:      sampleRates[idx],
		ChannelConfig:   int(b[2]&0x01)<<2 | int(b[3]>>6),
		Blocks:          int(b[6]&0x03) + 1,
	}
}

// Params are the stream parameters an MP4 sample description would have to
// carry. Two frames with different Params cannot share one MP4 track under a
// stream copy (see package remux), so the scanners report where they change.
type Params struct {
	Profile         int // profile_ObjectType: 0 Main, 1 LC, 2 SSR, 3 LTP
	SampleRateIndex int // sampling_frequency_index (0..12)
	SampleRate      int // Hz
	ChannelConfig   int // channel_configuration (0 = in-band PCE)
	Blocks          int // number_of_raw_data_blocks + 1 (ffmpeg emits 1; >1 breaks the MP4 sample math)
}

// Channels maps channel_configuration to a channel count: 1..6 → 1..6, 7 → 8,
// 0 → 0 (defined by a PCE; unknown without decoding).
func (p Params) Channels() int {
	switch {
	case p.ChannelConfig == 0:
		return 0
	case p.ChannelConfig <= 6:
		return p.ChannelConfig
	default:
		return 8
	}
}

// ProfileName renders profile_ObjectType the way ffprobe does.
func (p Params) ProfileName() string {
	switch p.Profile {
	case 0:
		return "Main"
	case 1:
		return "LC"
	case 2:
		return "SSR"
	default:
		return "LTP"
	}
}

// Chunk is a byte range of the file whose frames all share the same Params.
// Chunks partition the scanned region: a new chunk starts at the first frame
// whose Params differ from the current chunk's. ID3 tags and junk bytes belong
// to the chunk they sit in (bytes before the first frame belong to chunk 0).
type Chunk struct {
	Offset   int64
	Length   int64
	Params   Params
	Frames   int
	Duration time.Duration
}

// chunkList accumulates the homogeneous byte ranges of one scan. It is cut at
// frame starts only, which is what makes tags and junk belong to the chunk they
// sit in, and it appends at most once per parameter change (never per frame) so
// scanning a uniform file allocates nothing extra.
type chunkList struct {
	chunks  []Chunk
	seconds float64 // playback time of the open chunk, summed like the rest of the scan
}

// frame records a frame with parameters p starting at absolute offset off and
// lasting secs seconds.
func (c *chunkList) frame(off int64, p Params, secs float64) {
	switch {
	case len(c.chunks) == 0:
		// Whatever precedes the first frame (a leading ID3 tag, junk) is part
		// of chunk 0, so the first chunk starts at the start of the region.
		c.chunks = append(c.chunks, Chunk{Params: p})
	case p != c.chunks[len(c.chunks)-1].Params:
		c.finish(off)
		c.chunks = append(c.chunks, Chunk{Offset: off, Params: p})
	}
	cur := &c.chunks[len(c.chunks)-1]
	cur.Frames++
	c.seconds += secs
}

// finish closes the open chunk at absolute offset end.
func (c *chunkList) finish(end int64) {
	cur := &c.chunks[len(c.chunks)-1]
	cur.Length = end - cur.Offset
	cur.Duration = time.Duration(c.seconds * float64(time.Second))
	c.seconds = 0
}

// done closes the last chunk at end (the end of the scanned region, so the
// chunks partition it) and returns them; nil when no frame was seen.
func (c *chunkList) done(end int64) []Chunk {
	if len(c.chunks) == 0 {
		return nil
	}
	c.finish(end)
	return c.chunks
}

// Info describes the frames found in an ADTS file.
type Info struct {
	Frames   int           // complete frames
	Duration time.Duration // playback time of those frames
	Junk     int64         // bytes that were not part of a complete frame or tag
	Tags     int           // well-formed ID3v2 tags skipped
	TagBytes int64         // bytes those tags occupied

	// Params of the first frame; ParamsChanged is true when any later frame
	// differs (sample rate, channel config, profile or raw-data-block count),
	// in which case Chunks holds the homogeneous byte ranges. Chunks is nil
	// for a file with no frames and has exactly one entry for a uniform file.
	Params        Params
	ParamsChanged bool
	Chunks        []Chunk
}

// Scan walks the whole file and derives its playback duration from the frame
// headers (1024 samples per raw data block at the header's sampling rate).
// ADTS carries no timestamps, so this is the only exact way to know how long
// a recording is; the file is read once sequentially.
//
// Well-formed ID3v2 tags (files captured before 1.1.0 may contain in-band ID3
// tags; the scanner still skips them) are skipped and counted in Tags/TagBytes,
// not Junk. Bytes that do not form a valid frame
// are skipped one at a time and counted in Junk; after such a gap a candidate
// frame is only trusted when the frame following it is valid too.
//
// The same pass records the stream parameters (Params/ParamsChanged/Chunks):
// package remux needs them to decide whether the file can be stream-copied into
// a single MP4 track or has to be re-encoded chunk by chunk.
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
	var chunks chunkList
	seconds := 0.0
	synced := false
	var pos int64 // bytes consumed so far == offset of the element at hand
	for {
		// An ID3v2 tag may appear anywhere (at the start of the file and between
		// frames). Skip it and count it separately from junk. A tag breaks frame
		// adjacency, so re-sync afterwards.
		if hdr, err := r.Peek(10); err == nil && bytes.HasPrefix(hdr, []byte("ID3")) {
			if n, ok := id3.TagLen(hdr); ok {
				d, derr := r.Discard(n)
				info.Tags++
				info.TagBytes += int64(d)
				pos += int64(d)
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
			pos += int64(len(hdr))
			break
		}
		frameLen, ok := parseHeader(hdr)
		if !ok {
			pos += skip(r, &info)
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
				pos += int64(len(buf))
				break
			}
			if len(buf) != frameLen {
				// Followed by a few stray bytes: cannot be a frame boundary.
				pos += skip(r, &info)
				continue
			}
			// Exactly one frame left: a clean end of file.
		} else if !synced {
			if _, ok := parseHeader(buf[frameLen:]); !ok {
				pos += skip(r, &info)
				continue
			}
		}
		synced = true
		p := headerParams(buf)
		frameSeconds := float64(1024*p.Blocks) / float64(p.SampleRate)
		seconds += frameSeconds
		chunks.frame(pos, p, frameSeconds)
		info.Frames++
		if _, err := r.Discard(frameLen); err != nil {
			return info, err
		}
		pos += int64(frameLen)
	}
	info.Duration = time.Duration(seconds * float64(time.Second))
	info.Chunks = chunks.done(pos)
	if len(info.Chunks) > 0 {
		// A later frame that differs opens a new chunk, so more than one chunk
		// is exactly the condition ParamsChanged describes.
		info.Params = info.Chunks[0].Params
		info.ParamsChanged = len(info.Chunks) > 1
	}
	return info, nil
}

// skip discards one unusable byte and returns how many bytes were consumed, so
// the caller can keep its absolute position in step with the reader.
func skip(r *bufio.Reader, info *Info) int64 {
	n, _ := r.Discard(1)
	info.Junk += int64(n)
	return int64(n)
}

// ScanRuns walks the file once and attributes every frame, tag and junk byte to
// the run whose byte range contains that element's first byte. offsets holds the
// first byte of each run (ascending); run i covers [offsets[i], offsets[i+1]),
// the last run extends to EOF, and any bytes before offsets[0] fall in run 0. It
// returns one Info per run plus the totals. Used at finalize to give every run
// record its authoritative frame count and duration.
//
// Each run's Info carries the Params of that run's first frame and a
// ParamsChanged that only looks at that run. Chunks are a property of the whole
// file (a run boundary is not a parameter change, so a chunk may span runs) and
// are therefore reported on total only, together with the file's Params and
// ParamsChanged.
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
	var chunks chunkList

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
		p := headerParams(buf)
		frameSeconds := float64(1024*p.Blocks) / float64(p.SampleRate)
		runSeconds[idx] += frameSeconds
		chunks.frame(pos, p, frameSeconds)
		if runs[idx].Frames == 0 {
			runs[idx].Params = p
		} else if p != runs[idx].Params {
			runs[idx].ParamsChanged = true
		}
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
	total.Chunks = chunks.done(pos)
	if len(total.Chunks) > 0 {
		total.Params = total.Chunks[0].Params
		total.ParamsChanged = len(total.Chunks) > 1
	}
	return runs, total, nil
}
