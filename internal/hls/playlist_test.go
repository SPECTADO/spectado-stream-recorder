package hls

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRenderParseRoundTripLegacy(t *testing.T) {
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
	// A legacy folder (no byteranges) must stay VERSION 3 and carry no BYTERANGE.
	if strings.Contains(text, "BYTERANGE") || strings.Contains(text, "PROGRAM-DATE-TIME") {
		t.Fatalf("legacy playlist should have no byterange/PDT:\n%s", text)
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

func TestRenderByterangeAndPDT(t *testing.T) {
	t0 := time.Date(2026, 9, 3, 18, 44, 0, 500*int(time.Millisecond), time.UTC)
	t1 := time.Date(2026, 9, 3, 18, 44, 30, 0, time.UTC)
	segs := []Segment{
		{URI: "x.aac", Duration: 30.0, Offset: 0, Length: 12345, ProgramDateTime: t0},
		{URI: "x.aac", Duration: 15.25, Offset: 12345, Length: 6789, ProgramDateTime: t1},
	}
	out := string(Render(segs))
	for _, want := range []string{
		"#EXT-X-VERSION:4\n",
		"#EXT-X-PROGRAM-DATE-TIME:2026-09-03T18:44:00.500Z\n#EXTINF:30.000,\n#EXT-X-BYTERANGE:12345@0\nx.aac\n",
		"#EXT-X-DISCONTINUITY\n#EXT-X-PROGRAM-DATE-TIME:2026-09-03T18:44:30.000Z\n#EXTINF:15.250,\n#EXT-X-BYTERANGE:6789@12345\nx.aac\n",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("rendered playlist lacks %q:\n%s", want, out)
		}
	}
	// Parse round trip preserves offset/length/PDT.
	got, err := Parse([]byte(out))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("segments = %d", len(got))
	}
	if got[0].Offset != 0 || got[0].Length != 12345 || !got[0].ProgramDateTime.Equal(t0) {
		t.Fatalf("seg0 = %+v", got[0])
	}
	if got[1].Offset != 12345 || got[1].Length != 6789 || !got[1].ProgramDateTime.Equal(t1) {
		t.Fatalf("seg1 = %+v", got[1])
	}
}

func TestParseByterangeWithoutOffset(t *testing.T) {
	// A byterange without @offset continues after the previous byterange of the
	// same URI (RFC 8216).
	in := "#EXTM3U\n#EXT-X-VERSION:4\n" +
		"#EXTINF:10.000,\n#EXT-X-BYTERANGE:100@0\nx.aac\n" +
		"#EXTINF:10.000,\n#EXT-X-BYTERANGE:200\nx.aac\n" +
		"#EXTINF:10.000,\n#EXT-X-BYTERANGE:50\nx.aac\n#EXT-X-ENDLIST\n"
	segs, err := Parse([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	wantOff := []int64{0, 100, 300}
	wantLen := []int64{100, 200, 50}
	if len(segs) != 3 {
		t.Fatalf("segments = %d", len(segs))
	}
	for i := range segs {
		if segs[i].Offset != wantOff[i] || segs[i].Length != wantLen[i] {
			t.Fatalf("seg %d = off %d len %d, want off %d len %d", i, segs[i].Offset, segs[i].Length, wantOff[i], wantLen[i])
		}
	}
}

func TestRenderEmpty(t *testing.T) {
	out := string(Render(nil))
	if !strings.HasPrefix(out, "#EXTM3U\n") || !strings.HasSuffix(out, "#EXT-X-ENDLIST\n") || !strings.Contains(out, "TARGETDURATION:1\n") {
		t.Fatalf("empty playlist = %q", out)
	}
	if !strings.Contains(out, "#EXT-X-VERSION:3\n") {
		t.Fatalf("empty playlist should be VERSION 3: %q", out)
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
	want := []Segment{{URI: "first.aac", Duration: 10.25}, {URI: "second.aac", Duration: 0}, {URI: "third.aac", Duration: 3}}
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

func TestMergeLegacy(t *testing.T) {
	existing := []Segment{
		{URI: "b_20260903T190000Z.aac", Duration: 5},
		{URI: "b_20260903T184400Z.aac", Duration: 1},
		{URI: "", Duration: 9},
	}
	fresh := []Segment{
		{URI: "b_20260903T184400Z.aac", Duration: 1.75},
		{URI: "b_20260903T184400Z-2.aac", Duration: 2},
	}
	got := Merge(existing, fresh)
	want := []Segment{
		{URI: "b_20260903T184400Z.aac", Duration: 1.75}, // fresh duration wins
		{URI: "b_20260903T184400Z-2.aac", Duration: 2},  // same second, started later
		{URI: "b_20260903T190000Z.aac", Duration: 5},
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

// TestMergeReplacesURIWithByteRanges covers the recorder re-uploading a file it
// previously listed as one whole-file entry, now split into byte ranges: every
// old entry of that URI is replaced by the fresh ranges, while other files
// (including legacy whole-file entries) are kept.
func TestMergeReplacesURIWithByteRanges(t *testing.T) {
	// A legacy whole-file entry for another file, plus a stale whole-file entry
	// for the file being re-uploaded.
	existing := []Segment{
		{URI: "old_20260101T000000Z.aac", Duration: 100},
		{URI: "x_20260903T184400Z.aac", Duration: 40}, // stale whole-file, must be replaced
	}
	fresh := []Segment{
		{URI: "x_20260903T184400Z.aac", Duration: 30, Offset: 0, Length: 1000},
		{URI: "x_20260903T184400Z.aac", Duration: 10, Offset: 1000, Length: 400},
	}
	got := Merge(existing, fresh)
	if len(got) != 3 {
		t.Fatalf("merge = %+v, want 3 entries", got)
	}
	// The stale whole-file entry (Length 0) must be gone.
	for _, s := range got {
		if s.URI == "x_20260903T184400Z.aac" && s.Length == 0 {
			t.Fatalf("stale whole-file entry not replaced: %+v", got)
		}
	}
	// Order: old file first (sortKey), then the two byte ranges front to back.
	if got[0].URI != "old_20260101T000000Z.aac" {
		t.Fatalf("first should be the other file: %+v", got)
	}
	if got[1].Offset != 0 || got[2].Offset != 1000 {
		t.Fatalf("byte ranges out of order: %+v", got[1:])
	}
	// Rendering the merged list is VERSION 4 (a byterange is present).
	if out := string(Render(got)); !strings.Contains(out, "#EXT-X-VERSION:4\n") {
		t.Fatalf("merged render should be v4:\n%s", out)
	}
}
