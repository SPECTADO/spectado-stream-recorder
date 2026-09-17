package hls

import (
	"net/url"
	"testing"
	"time"
)

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestParseLiveMediaWithPDTAndInheritance(t *testing.T) {
	in := "#EXTM3U\n#EXT-X-VERSION:4\n#EXT-X-TARGETDURATION:5\n#EXT-X-MEDIA-SEQUENCE:10\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2026-09-17T10:00:00.000Z\n#EXTINF:5.000,\nseg10.ts\n" +
		"#EXTINF:5.000,\nseg11.ts\n" + // inherits 10:00:05
		"#EXTINF:4.000,\nseg12.ts\n" // inherits 10:00:10
	lp, err := ParseLive([]byte(in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if lp.Master || lp.EndList {
		t.Fatalf("should be a media playlist, no endlist: %+v", lp)
	}
	if len(lp.Segments) != 3 {
		t.Fatalf("segments = %d", len(lp.Segments))
	}
	want := []time.Time{
		time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 17, 10, 0, 5, 0, time.UTC),
		time.Date(2026, 9, 17, 10, 0, 10, 0, time.UTC),
	}
	for i, w := range want {
		if !lp.Segments[i].ProgramDateTime.Equal(w) {
			t.Fatalf("seg %d PDT = %v, want %v", i, lp.Segments[i].ProgramDateTime, w)
		}
	}
}

func TestParseLiveEndListAndMissingPDT(t *testing.T) {
	in := "#EXTM3U\n#EXTINF:5.000,\na.ts\n#EXTINF:5.000,\nb.ts\n#EXT-X-ENDLIST\n"
	lp, err := ParseLive([]byte(in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !lp.EndList || len(lp.Segments) != 2 {
		t.Fatalf("lp = %+v", lp)
	}
	for i, s := range lp.Segments {
		if !s.ProgramDateTime.IsZero() {
			t.Fatalf("seg %d should have no PDT: %v", i, s.ProgramDateTime)
		}
	}
}

func TestParseLivePerSegmentPDT(t *testing.T) {
	// Each segment carries its own PDT (no inheritance needed).
	in := "#EXTM3U\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2026-09-17T10:00:00Z\n#EXTINF:5,\na.ts\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2026-09-17T10:00:06Z\n#EXTINF:5,\nb.ts\n"
	lp, err := ParseLive([]byte(in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !lp.Segments[1].ProgramDateTime.Equal(time.Date(2026, 9, 17, 10, 0, 6, 0, time.UTC)) {
		t.Fatalf("seg1 PDT = %v", lp.Segments[1].ProgramDateTime)
	}
}

func TestParseLiveMaster(t *testing.T) {
	base := mustURL(t, "https://cdn.example.com/live/master.m3u8")
	in := "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS=\"mp4a.40.2\"\naudio/hi.m3u8\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=64000\naudio/lo.m3u8\n"
	lp, err := ParseLive([]byte(in), base)
	if err != nil {
		t.Fatal(err)
	}
	if !lp.Master {
		t.Fatalf("should be a master: %+v", lp)
	}
	if lp.Variant != "https://cdn.example.com/live/audio/hi.m3u8" {
		t.Fatalf("variant = %q", lp.Variant)
	}
}

func TestParseLiveMasterAudioRendition(t *testing.T) {
	base := mustURL(t, "https://cdn.example.com/live/master.m3u8")
	in := "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",NAME=\"en\",URI=\"audio/en.m3u8\"\n"
	lp, err := ParseLive([]byte(in), base)
	if err != nil {
		t.Fatal(err)
	}
	if !lp.Master || lp.Variant != "https://cdn.example.com/live/audio/en.m3u8" {
		t.Fatalf("audio rendition master: %+v", lp)
	}
}

func TestParseLiveRejectsNonPlaylist(t *testing.T) {
	if _, err := ParseLive([]byte("not a playlist"), nil); err == nil {
		t.Fatal("want error for non-playlist")
	}
}

func TestAttr(t *testing.T) {
	line := `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a",NAME="en",URI="audio/en.m3u8",DEFAULT=YES`
	if got := attr(line, "URI"); got != "audio/en.m3u8" {
		t.Fatalf("URI = %q", got)
	}
	if got := attr(line, "TYPE"); got != "AUDIO" {
		t.Fatalf("TYPE = %q", got)
	}
	if got := attr(line, "MISSING"); got != "" {
		t.Fatalf("MISSING = %q", got)
	}
	// GROUP-ID contains "URI"-free text; ensure NAME boundary matching works.
	if got := attr(line, "NAME"); got != "en" {
		t.Fatalf("NAME = %q", got)
	}
}
