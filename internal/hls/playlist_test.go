package hls

import (
	"errors"
	"strings"
	"testing"
)

func TestRenderParseRoundTrip(t *testing.T) {
	segs := []Segment{
		{URI: "a_20260903T184400Z.aac", Duration: 7200.533},
		{URI: "a_20260903T204500Z.aac", Duration: 12.5},
	}
	out := Render(segs)
	text := string(out)
	for _, want := range []string{
		"#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION:7201\n#EXT-X-MEDIA-SEQUENCE:0\n",
		"#EXTINF:7200.533,\na_20260903T184400Z.aac\n#EXT-X-DISCONTINUITY\n#EXTINF:12.500,\na_20260903T204500Z.aac\n#EXT-X-ENDLIST\n",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("rendered playlist lacks %q:\n%s", want, text)
		}
	}
	if strings.Count(text, "#EXT-X-DISCONTINUITY") != 1 {
		t.Fatalf("one discontinuity between two segments expected:\n%s", text)
	}
	got, err := Parse(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != segs[0] || got[1].URI != segs[1].URI || got[1].Duration != 12.5 {
		t.Fatalf("round trip = %+v, want %+v", got, segs)
	}
}

func TestRenderEmpty(t *testing.T) {
	out := string(Render(nil))
	if !strings.HasPrefix(out, "#EXTM3U\n") || !strings.HasSuffix(out, "#EXT-X-ENDLIST\n") || !strings.Contains(out, "TARGETDURATION:1\n") {
		t.Fatalf("empty playlist = %q", out)
	}
	if strings.Contains(out, "#EXTINF") || strings.Contains(out, "DISCONTINUITY") {
		t.Fatalf("empty playlist must not list segments: %q", out)
	}
}

func TestParseTolerant(t *testing.T) {
	in := "\xef\xbb\xbf#EXTM3U\r\n#EXT-X-VERSION:3\r\n\r\n#EXT-X-FOO:bar\r\n#EXTINF:10.25, some title\r\nfirst.aac\r\n" +
		"#EXTINF:garbage,\r\nsecond.aac\r\n#EXT-X-DISCONTINUITY\r\n#EXTINF:3\r\nthird.aac\r\norphan-line-without-extinf.aac\r\n#EXT-X-ENDLIST\r\n"
	segs, err := Parse([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	want := []Segment{{"first.aac", 10.25}, {"second.aac", 0}, {"third.aac", 3}}
	if len(segs) != len(want) {
		t.Fatalf("segments = %+v, want %+v", segs, want)
	}
	for i := range want {
		if segs[i] != want[i] {
			t.Fatalf("segment %d = %+v, want %+v", i, segs[i], want[i])
		}
	}
}

func TestParseRejectsNonPlaylist(t *testing.T) {
	for _, in := range []string{"", "   \n\n", "<html>", "#EXTINF:1,\nx.aac\n"} {
		if _, err := Parse([]byte(in)); !errors.Is(err, ErrNotPlaylist) {
			t.Errorf("Parse(%q) error = %v, want ErrNotPlaylist", in, err)
		}
	}
	segs, err := Parse([]byte("#EXTM3U\n#EXT-X-ENDLIST\n"))
	if err != nil || len(segs) != 0 {
		t.Fatalf("empty playlist: segs=%v err=%v", segs, err)
	}
}

func TestMerge(t *testing.T) {
	existing := []Segment{{"b_20260903T190000Z.aac", 5}, {"b_20260903T184400Z.aac", 1}, {"", 9}}
	fresh := []Segment{{"b_20260903T184400Z.aac", 1.75}, {"b_20260903T184400Z-2.aac", 2}}
	got := Merge(existing, fresh)
	want := []Segment{
		{"b_20260903T184400Z.aac", 1.75}, // fresh duration wins
		{"b_20260903T184400Z-2.aac", 2},  // same second, started later
		{"b_20260903T190000Z.aac", 5},
	}
	if len(got) != len(want) {
		t.Fatalf("merge = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("merge[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
	if len(Merge(nil, nil)) != 0 {
		t.Fatal("merge of nothing must be empty")
	}
}
