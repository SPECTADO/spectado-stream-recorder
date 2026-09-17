// Package hls renders and parses the small VOD media playlists (index.m3u8)
// the recorder publishes next to each recording, and parses live source
// playlists to read their #EXT-X-PROGRAM-DATE-TIME (the clock anchor of a run).
//
// Every recording folder in the bucket holds one playlist that lists all
// media files of that folder (normally one; several after a rotation or when
// a show was extended after it ended). Segments are raw ADTS/AAC files, so
// the playlist is a plain "packed audio" playlist without an init section.
// Each ffmpeg run of a file is one #EXT-X-BYTERANGE segment carrying its own
// #EXT-X-PROGRAM-DATE-TIME.
package hls

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

// pdtLayout is RFC3339 with millisecond precision in UTC, used for
// #EXT-X-PROGRAM-DATE-TIME rendering.
const pdtLayout = "2006-01-02T15:04:05.000Z07:00"

// Segment is one media entry of the playlist. URI is relative to the playlist
// (a bare file name for files in the same folder). Offset/Length describe an
// #EXT-X-BYTERANGE into URI (Length 0 = the whole file); ProgramDateTime is the
// wall-clock time of the segment's first sample (zero = none).
type Segment struct {
	URI             string
	Duration        float64 // seconds
	Offset          int64
	Length          int64
	ProgramDateTime time.Time
}

// ErrNotPlaylist is returned by Parse when the input does not start with
// #EXTM3U.
var ErrNotPlaylist = errors.New("not an M3U8 playlist")

// Parse extracts the media segments of a playlist. It understands #EXTINF,
// #EXT-X-BYTERANGE (n[@o]; a missing offset continues after the previous
// byterange of the same URI per RFC 8216) and #EXT-X-PROGRAM-DATE-TIME (applies
// to the next segment). Other tags are ignored, so playlists written by other
// tools merge losslessly as long as they are M3U8. An empty segment list is not
// an error.
func Parse(data []byte) ([]Segment, error) {
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf")) // UTF-8 BOM
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	first := true
	var segs []Segment
	pending := false
	var dur float64
	var pdt time.Time
	var rangeLen, rangeOff int64
	var haveRange, haveOff bool
	nextOff := map[string]int64{} // URI -> default offset for a byterange without @o
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if first {
			if !strings.HasPrefix(line, "#EXTM3U") {
				return nil, ErrNotPlaylist
			}
			first = false
			continue
		}
		switch {
		case strings.HasPrefix(line, "#EXTINF:"):
			v := strings.TrimPrefix(line, "#EXTINF:")
			if i := strings.IndexByte(v, ','); i >= 0 {
				v = v[:i]
			}
			d, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil || d < 0 || math.IsInf(d, 0) || math.IsNaN(d) {
				d = 0
			}
			dur = d
			pending = true
		case strings.HasPrefix(line, "#EXT-X-BYTERANGE:"):
			rangeLen, rangeOff, haveOff = parseByteRange(strings.TrimPrefix(line, "#EXT-X-BYTERANGE:"))
			haveRange = true
		case strings.HasPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:"):
			if t, err := parseTime(strings.TrimPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:")); err == nil {
				pdt = t
			}
		case strings.HasPrefix(line, "#"):
			// ignore other tags
		default:
			if pending {
				seg := Segment{URI: line, Duration: dur, ProgramDateTime: pdt}
				if haveRange {
					off := rangeOff
					if !haveOff {
						off = nextOff[line]
					}
					seg.Offset = off
					seg.Length = rangeLen
					nextOff[line] = off + rangeLen
				}
				segs = append(segs, seg)
			}
			pending, haveRange, haveOff = false, false, false
			dur, rangeLen, rangeOff = 0, 0, 0
			pdt = time.Time{}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if first {
		return nil, ErrNotPlaylist
	}
	return segs, nil
}

// parseByteRange parses "n[@o]".
func parseByteRange(v string) (length, offset int64, haveOffset bool) {
	v = strings.TrimSpace(v)
	nStr, oStr, hasAt := strings.Cut(v, "@")
	n, err := strconv.ParseInt(strings.TrimSpace(nStr), 10, 64)
	if err != nil || n < 0 {
		n = 0
	}
	if hasAt {
		if o, err := strconv.ParseInt(strings.TrimSpace(oStr), 10, 64); err == nil && o >= 0 {
			return n, o, true
		}
	}
	return n, 0, false
}

// Merge returns the union of the two segment lists keyed by URI+"@"+Offset.
// Every existing entry of a URI that also appears in fresh is dropped (the
// recorder knows the true byte layout of the file it just uploaded), then the
// fresh entries are added. The result is sorted by file name without its
// extension, then URI, then Offset, which orders the recorder's session-stamped
// names chronologically ("x_<stamp>.aac" before "x_<stamp>-2.aac") and the byte
// ranges within one file front to back.
func Merge(existing, fresh []Segment) []Segment {
	freshURI := make(map[string]bool, len(fresh))
	for _, s := range fresh {
		if s.URI != "" {
			freshURI[s.URI] = true
		}
	}
	key := func(s Segment) string { return s.URI + "@" + strconv.FormatInt(s.Offset, 10) }
	byKey := make(map[string]Segment, len(existing)+len(fresh))
	for _, s := range existing {
		if s.URI == "" || freshURI[s.URI] {
			continue // replaced wholesale by the fresh entries of this URI
		}
		byKey[key(s)] = s
	}
	for _, s := range fresh {
		if s.URI != "" {
			byKey[key(s)] = s
		}
	}
	out := make([]Segment, 0, len(byKey))
	for _, s := range byKey {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := sortKey(out[i].URI), sortKey(out[j].URI)
		if a != b {
			return a < b
		}
		if out[i].URI != out[j].URI {
			return out[i].URI < out[j].URI
		}
		return out[i].Offset < out[j].Offset
	})
	return out
}

func sortKey(uri string) string { return strings.TrimSuffix(uri, path.Ext(uri)) }

// Render produces a complete VOD playlist. It is #EXT-X-VERSION:4 as soon as any
// segment carries a byte range (#EXT-X-BYTERANGE needs v4), else v3 so legacy
// whole-file folders render byte-for-byte as before. Consecutive segments come
// from separate ffmpeg runs, so every boundary is a discontinuity.
func Render(segs []Segment) []byte {
	version := 3
	for _, s := range segs {
		if s.Length > 0 {
			version = 4
			break
		}
	}
	target := 1.0
	for _, s := range segs {
		if s.Duration > target {
			target = s.Duration
		}
	}
	var b bytes.Buffer
	b.WriteString("#EXTM3U\n")
	fmt.Fprintf(&b, "#EXT-X-VERSION:%d\n", version)
	b.WriteString("#EXT-X-PLAYLIST-TYPE:VOD\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", int(math.Ceil(target)))
	b.WriteString("#EXT-X-MEDIA-SEQUENCE:0\n")
	for i, s := range segs {
		if i > 0 {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if !s.ProgramDateTime.IsZero() {
			fmt.Fprintf(&b, "#EXT-X-PROGRAM-DATE-TIME:%s\n", s.ProgramDateTime.UTC().Format(pdtLayout))
		}
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n", s.Duration)
		if s.Length > 0 {
			fmt.Fprintf(&b, "#EXT-X-BYTERANGE:%d@%d\n", s.Length, s.Offset)
		}
		b.WriteString(s.URI)
		b.WriteByte('\n')
	}
	b.WriteString("#EXT-X-ENDLIST\n")
	return b.Bytes()
}

// parseTime parses an RFC3339 timestamp with or without fractional seconds.
func parseTime(v string) (time.Time, error) {
	v = strings.TrimSpace(v)
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, v)
}
