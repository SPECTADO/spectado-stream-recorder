// Package hls renders and parses the small VOD media playlists (index.m3u8)
// the recorder publishes next to each recording.
//
// Every recording folder in the bucket holds one playlist that lists all
// media files of that folder (normally one; several after a rotation or when
// a show was extended after it ended). Segments are raw ADTS/AAC files, so
// the playlist is a plain "packed audio" playlist without an init section.
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
)

// Segment is one media file of the playlist. URI is relative to the playlist
// (a bare file name for files in the same folder).
type Segment struct {
	URI      string
	Duration float64 // seconds
}

// ErrNotPlaylist is returned by Parse when the input does not start with
// #EXTM3U.
var ErrNotPlaylist = errors.New("not an M3U8 playlist")

// Parse extracts the media segments of a playlist. Tags other than #EXTINF
// are ignored, so playlists written by other tools are tolerated as long as
// they are M3U8. An empty segment list is not an error.
func Parse(data []byte) ([]Segment, error) {
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf")) // UTF-8 BOM
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	first := true
	var segs []Segment
	pending := false
	var dur float64
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
		if strings.HasPrefix(line, "#EXTINF:") {
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
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		if pending {
			segs = append(segs, Segment{URI: line, Duration: dur})
			pending = false
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

// Merge returns the union of the two segment lists keyed by URI. Entries in
// fresh replace entries with the same URI in existing (the recorder knows the
// exact duration of what it just uploaded). The result is sorted by file name
// without its extension, which orders the recorder's session-stamped names
// chronologically ("x_<stamp>.aac" before "x_<stamp>-2.aac").
func Merge(existing, fresh []Segment) []Segment {
	byURI := make(map[string]Segment, len(existing)+len(fresh))
	for _, s := range existing {
		if s.URI != "" {
			byURI[s.URI] = s
		}
	}
	for _, s := range fresh {
		if s.URI != "" {
			byURI[s.URI] = s
		}
	}
	out := make([]Segment, 0, len(byURI))
	for _, s := range byURI {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := sortKey(out[i].URI), sortKey(out[j].URI)
		if a != b {
			return a < b
		}
		return out[i].URI < out[j].URI
	})
	return out
}

func sortKey(uri string) string { return strings.TrimSuffix(uri, path.Ext(uri)) }

// Render produces a complete VOD playlist. Consecutive segments come from
// separate ffmpeg runs, so every boundary is marked as a discontinuity.
func Render(segs []Segment) []byte {
	target := 1.0
	for _, s := range segs {
		if s.Duration > target {
			target = s.Duration
		}
	}
	var b bytes.Buffer
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:3\n")
	b.WriteString("#EXT-X-PLAYLIST-TYPE:VOD\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", int(math.Ceil(target)))
	b.WriteString("#EXT-X-MEDIA-SEQUENCE:0\n")
	for i, s := range segs {
		if i > 0 {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n%s\n", s.Duration, s.URI)
	}
	b.WriteString("#EXT-X-ENDLIST\n")
	return b.Bytes()
}
