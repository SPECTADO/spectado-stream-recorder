package hls

import (
	"bufio"
	"bytes"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// LiveSegment is one media segment of a live source playlist.
type LiveSegment struct {
	URI             string
	Duration        float64
	ProgramDateTime time.Time // zero when neither declared nor inheritable
}

// LivePlaylist is the parsed view of a source HLS playlist used to anchor a run
// to the source's wall clock. For a master playlist Master is true and Variant
// holds the first variant/audio-rendition URI (resolved against the base URL).
type LivePlaylist struct {
	Master   bool
	Variant  string
	Segments []LiveSegment
	EndList  bool
}

// ParseLive parses a live source playlist. For a media playlist it returns the
// segments with their durations and program-date-times: a PDT applies to the
// segment that follows it, and a segment without its own PDT inherits
// prev.PDT + prev.Duration. For a master playlist (#EXT-X-STREAM-INF, or
// #EXT-X-MEDIA:TYPE=AUDIO with a URI) it returns Master=true with the first
// variant URI resolved against base. #EXT-X-ENDLIST is reported in EndList.
func ParseLive(data []byte, base *url.URL) (LivePlaylist, error) {
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64*1024), 1024*1024)

	var lp LivePlaylist
	first := true
	var dur float64
	var pdt time.Time
	var havePDT bool
	var prevEnd time.Time
	var haveInherit bool
	var variantURI, audioURI string
	expectVariant := false

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if first {
			if !strings.HasPrefix(line, "#EXTM3U") {
				return LivePlaylist{}, ErrNotPlaylist
			}
			first = false
			continue
		}
		switch {
		case strings.HasPrefix(line, "#EXT-X-ENDLIST"):
			lp.EndList = true
		case strings.HasPrefix(line, "#EXT-X-STREAM-INF"):
			expectVariant = true
		case strings.HasPrefix(line, "#EXT-X-MEDIA:"):
			if audioURI == "" && strings.Contains(strings.ToUpper(line), "TYPE=AUDIO") {
				if u := attr(line, "URI"); u != "" {
					audioURI = u
				}
			}
		case strings.HasPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:"):
			if t, err := parseTime(strings.TrimPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:")); err == nil {
				pdt, havePDT = t, true
			}
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
		case strings.HasPrefix(line, "#"):
			// ignore other tags
		default:
			if expectVariant {
				if variantURI == "" {
					variantURI = line
				}
				expectVariant = false
				continue
			}
			seg := LiveSegment{URI: line, Duration: dur}
			switch {
			case havePDT:
				seg.ProgramDateTime = pdt
			case haveInherit:
				seg.ProgramDateTime = prevEnd
			}
			lp.Segments = append(lp.Segments, seg)
			if !seg.ProgramDateTime.IsZero() {
				prevEnd = seg.ProgramDateTime.Add(time.Duration(seg.Duration * float64(time.Second)))
				haveInherit = true
			} else {
				haveInherit = false
			}
			dur = 0
			havePDT = false
		}
	}
	if err := sc.Err(); err != nil {
		return LivePlaylist{}, err
	}
	if first {
		return LivePlaylist{}, ErrNotPlaylist
	}

	// A playlist with no media segments but a variant/audio URI is a master.
	if len(lp.Segments) == 0 {
		uri := variantURI
		if uri == "" {
			uri = audioURI
		}
		if uri != "" {
			lp.Master = true
			lp.Variant = resolve(base, uri)
		}
	}
	return lp, nil
}

// attr returns the value of the attribute named key in an HLS attribute list
// (e.g. URI="a.m3u8"); the value may be quoted or bare. It returns "" when the
// attribute is absent.
func attr(line, key string) string {
	up := strings.ToUpper(key) + "="
	rest := line
	for {
		i := strings.Index(strings.ToUpper(rest), up)
		if i < 0 {
			return ""
		}
		// Ensure the match is at an attribute boundary (start, comma or space).
		if i > 0 {
			prev := rest[i-1]
			if prev != ',' && prev != ' ' && prev != ':' {
				rest = rest[i+len(up):]
				continue
			}
		}
		v := rest[i+len(up):]
		if len(v) > 0 && v[0] == '"' {
			if j := strings.IndexByte(v[1:], '"'); j >= 0 {
				return v[1 : 1+j]
			}
			return strings.TrimSpace(v[1:])
		}
		if j := strings.IndexByte(v, ','); j >= 0 {
			return strings.TrimSpace(v[:j])
		}
		return strings.TrimSpace(v)
	}
}

// resolve resolves a possibly-relative URI against base (base may be nil).
func resolve(base *url.URL, uri string) string {
	if base == nil {
		return uri
	}
	u, err := url.Parse(uri)
	if err != nil {
		return uri
	}
	return base.ResolveReference(u).String()
}
