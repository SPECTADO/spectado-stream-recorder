package recorder

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spectado/stream-recorder/internal/adts"
	"github.com/spectado/stream-recorder/internal/config"
	"github.com/spectado/stream-recorder/internal/id3"
	"github.com/spectado/stream-recorder/internal/metrics"
)

// adtsFrame builds a structurally valid ADTS frame (AAC-LC, 44.1 kHz, stereo)
// with a deterministic payload free of sync words.
func adtsFrame(payload int) []byte {
	fl := 7 + payload
	out := []byte{
		0xFF, 0xF1,
		0x50,
		0x80 | byte(fl>>11&0x03),
		byte(fl >> 3),
		byte(fl&0x07)<<5 | 0x1F,
		0xFC,
	}
	for i := 0; i < payload; i++ {
		out = append(out, byte(0x10+i%0xE0))
	}
	return out
}

// fakeFFmpegADTS writes a fake ffmpeg that streams real ADTS frames: a frames
// file is cat'd on a loop until SIGINT/SIGTERM.
func fakeFFmpegADTS(t *testing.T, dir string) string {
	t.Helper()
	var block []byte
	for i := 0; i < 60; i++ {
		block = append(block, adtsFrame(200)...)
	}
	framesPath := filepath.Join(dir, "frames.bin")
	if err := os.WriteFile(framesPath, block, 0o644); err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`#!/bin/sh
trap 'exit 0' INT TERM
while :; do
  cat %q || exit 0
  sleep 0.05
done
`, framesPath)
	p := filepath.Join(dir, "ffmpeg-adts.sh")
	if err := os.WriteFile(p, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestRunRecordsAndByterangePlaylist exercises the frame-aware writer end to
// end: a real ADTS stream produces a run record, in-band ID3 tags, and the
// upload publishes a VERSION 4 byterange playlist with a program-date-time plus
// the clock object metadata.
func TestRunRecordsAndByterangePlaylist(t *testing.T) {
	up := &fakeUploader{}
	dir := t.TempDir()
	ff := fakeFFmpegADTS(t, dir)
	cfg := testConfig(dir, ff)
	cfg.ClockID3Interval = 50 * time.Millisecond // media-time cadence: a tag every few frames
	cfg.ClockPDTLookup = false                   // no source playlist in this test: wallclock anchor
	m := NewManager(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), up, "test")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})
	now := time.Now()
	m.SetSchedule(sched(item("a", now.Add(-time.Second), now.Add(time.Hour))))
	ar := m.activeFor("a")
	if ar == nil {
		t.Fatal("not started")
	}
	// Wait until a run record exists and some audio has been written.
	waitFor(t, 5*time.Second, "run record with bytes", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(ar.s.Runs) == 1 && ar.bytes.Load() > 5000
	})

	// The active view exposes the current run's clock anchor and source.
	views := m.Recordings()
	if len(views) != 1 || views[0].ClockSource != "wallclock" || views[0].ClockAnchor == nil {
		t.Fatalf("active clock view = %+v", views[0])
	}
	if len(views[0].Runs) != 1 {
		t.Fatalf("view runs = %+v", views[0].Runs)
	}

	sid := ar.s.SessionID
	m.Reconcile(now.Add(2 * time.Hour)) // end
	m.settle()
	if st, _ := m.sessionState(sid); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	if up.count() != 1 {
		t.Fatalf("uploads = %d", up.count())
	}

	// Run record is closed with an authoritative frame count and duration.
	m.mu.Lock()
	runs := append([]Run(nil), m.sessions[sid].Runs...)
	m.mu.Unlock()
	if len(runs) != 1 {
		t.Fatalf("runs = %d, want 1", len(runs))
	}
	r := runs[0]
	if r.Offset != 0 || r.Frames <= 0 || r.DurationSeconds <= 0 || r.EndedAt == nil {
		t.Fatalf("run = %+v", r)
	}
	if r.AnchorSource != "wallclock" || r.Anchor.IsZero() {
		t.Fatalf("run anchor = %v source = %q", r.Anchor, r.AnchorSource)
	}

	// Object metadata carries the clock provenance.
	meta := up.calls[0].meta
	if meta["ffmpeg-runs"] != "1" || meta["clock-source"] != "wallclock" || meta["first-sample-time"] == "" {
		t.Fatalf("clock metadata = %v", meta)
	}

	// The published playlist is a VERSION 4 byterange playlist with a PDT.
	pl := up.object(playlistKey(ar.s.Key))
	for _, want := range []string{"#EXT-X-VERSION:4\n", "#EXT-X-BYTERANGE:", "#EXT-X-PROGRAM-DATE-TIME:", "\n" + sid + ".aac\n"} {
		if !strings.Contains(pl, want) {
			t.Fatalf("playlist lacks %q:\n%s", want, pl)
		}
	}
}

// TestWriterInsertsID3Tags checks the writer inserts ID3 wall-clock tags between
// frames and that the file remains a faithful, decodable ADTS concatenation
// (frame count via ScanRuns, tags counted separately from junk).
func TestWriterInsertsID3Tags(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir, "ffmpeg")
	cfg.ClockID3Interval = 40 * time.Millisecond
	m := NewManager(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), nil, "test")

	s := &Session{
		ID: "w", SafeID: "w", SessionID: "w_20260101T000000Z", Source: "http://x/live.m3u8",
		Key: "w/w_20260101T000000Z.aac", State: StateRecording, Codec: "aac",
		dir: filepath.Join(dir, "recordings", "w"),
	}
	m.mu.Lock()
	m.sessions[s.SessionID] = s
	m.mu.Unlock()
	ar, err := m.openActive(s, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ar.file.Close()

	anchor := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	ar.beginRun(anchor, "hls-pdt", 0)
	w := &captureWriter{ar: ar, m: m}

	const nframes = 100
	var stream []byte
	for i := 0; i < nframes; i++ {
		stream = append(stream, adtsFrame(180)...)
	}
	// Feed in awkward chunks to exercise partial-frame buffering.
	for i := 0; i < len(stream); i += 13 {
		end := i + 13
		if end > len(stream) {
			end = len(stream)
		}
		if _, err := w.Write(stream[i:end]); err != nil {
			t.Fatal(err)
		}
	}
	m.endRun(ar)
	_ = ar.file.Sync()

	// A run record was created and closed.
	m.mu.Lock()
	runs := append([]Run(nil), s.Runs...)
	m.mu.Unlock()
	if len(runs) != 1 || runs[0].EndedAt == nil || runs[0].AnchorSource != "hls-pdt" || !runs[0].Anchor.Equal(anchor) {
		t.Fatalf("run = %+v", runs)
	}

	// The file scans back to the same frame count, with the inserted tags counted
	// as tags (not junk).
	info, err := adts.Scan(s.FilePath())
	if err != nil {
		t.Fatal(err)
	}
	// The last frame may be flushed as junk (never confirmed), so allow one less.
	if info.Frames < nframes-1 || info.Junk != 0 {
		t.Fatalf("scan = %+v, want ~%d frames and no junk", info, nframes)
	}
	if info.Tags == 0 {
		t.Fatal("no ID3 tags were inserted")
	}
	// The first tag is at the very start of the run (frame 0).
	data, _ := os.ReadFile(s.FilePath())
	if n, ok := id3.TagLen(data); !ok || n <= 0 {
		t.Fatalf("file does not start with an ID3 tag: %v", data[:min(16, len(data))])
	}
	// ScanRuns attributes everything to the single run.
	rinfos, total, err := adts.ScanRuns(s.FilePath(), []int64{0})
	if err != nil {
		t.Fatal(err)
	}
	if len(rinfos) != 1 || rinfos[0].Frames != info.Frames || total.Tags != info.Tags {
		t.Fatalf("scanRuns = %+v total = %+v vs scan %+v", rinfos, total, info)
	}
}

// TestMultiRunContiguousByteRanges checks that after a restart that left a
// partial frame (trimmed before the next run), the run byte ranges stay exactly
// contiguous with no overlap: run0.Offset+run0.Bytes == run1.Offset.
func TestMultiRunContiguousByteRanges(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir, "ffmpeg")
	cfg.ClockID3Interval = 0 // keep byte arithmetic exact (no inserted tags)
	m := NewManager(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), nil, "test")

	s := &Session{
		ID: "m", SafeID: "m", SessionID: "m_20260101T000000Z", Source: "http://x/live.m3u8",
		Key: "m/m_20260101T000000Z.aac", State: StateRecording, Codec: "aac",
		dir: filepath.Join(dir, "recordings", "m"),
	}
	m.mu.Lock()
	m.sessions[s.SessionID] = s
	m.mu.Unlock()
	ar, err := m.openActive(s, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ar.file.Close()

	frame := adtsFrame(180) // 187 bytes
	fl := len(frame)
	full := func(n int) []byte {
		var b []byte
		for i := 0; i < n; i++ {
			b = append(b, frame...)
		}
		return b
	}

	// Run 0: 10 complete frames plus a 50-byte partial frame.
	ar.beginRun(time.Unix(1000, 0), "wallclock", 0)
	w0 := &captureWriter{ar: ar, m: m}
	if _, err := w0.Write(append(full(10), frame[:50]...)); err != nil {
		t.Fatal(err)
	}
	m.endRun(ar)
	m.trimTail(ar) // supervise trims the flushed partial before the next run

	// Run 1: 8 complete frames.
	ar.beginRun(time.Unix(2000, 0), "wallclock", 0)
	w1 := &captureWriter{ar: ar, m: m}
	if _, err := w1.Write(full(8)); err != nil {
		t.Fatal(err)
	}
	m.endRun(ar)
	_ = ar.file.Sync()

	m.mu.Lock()
	runs := append([]Run(nil), s.Runs...)
	m.mu.Unlock()
	if len(runs) != 2 {
		t.Fatalf("runs = %d, want 2", len(runs))
	}
	if runs[0].Offset != 0 || runs[0].Bytes != int64(10*fl) {
		t.Fatalf("run0 = offset %d bytes %d, want 0 / %d", runs[0].Offset, runs[0].Bytes, 10*fl)
	}
	if runs[1].Offset != int64(10*fl) {
		t.Fatalf("run1 offset = %d, want %d (contiguous, no overlap)", runs[1].Offset, 10*fl)
	}
	if runs[0].Offset+runs[0].Bytes != runs[1].Offset {
		t.Fatalf("byte ranges overlap/gap: run0 ends at %d, run1 starts at %d", runs[0].Offset+runs[0].Bytes, runs[1].Offset)
	}
	// The on-disk file is exactly the two runs' frames, contiguous.
	fi, err := os.Stat(s.FilePath())
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != int64(18*fl) {
		t.Fatalf("file size = %d, want %d", fi.Size(), 18*fl)
	}
	// ScanRuns attributes 10 and 8 frames respectively.
	rinfos, _, err := adts.ScanRuns(s.FilePath(), []int64{runs[0].Offset, runs[1].Offset})
	if err != nil {
		t.Fatal(err)
	}
	if rinfos[0].Frames != 10 || rinfos[1].Frames != 8 {
		t.Fatalf("scanRuns frames = %d / %d, want 10 / 8", rinfos[0].Frames, rinfos[1].Frames)
	}
}

// --- anchor lookup ------------------------------------------------------------

func anchorManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	cfg := testConfig(dir, "ffmpeg")
	cfg.ClockPDTLookup = true
	cfg.FFmpegUserAgent = "test-agent"
	return NewManager(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), nil, "test")
}

func mediaPlaylist(startPDT time.Time, n int, segDur float64) string {
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:4\n#EXT-X-TARGETDURATION:5\n#EXT-X-MEDIA-SEQUENCE:0\n")
	for i := 0; i < n; i++ {
		if i == 0 {
			fmt.Fprintf(&b, "#EXT-X-PROGRAM-DATE-TIME:%s\n", startPDT.UTC().Format("2006-01-02T15:04:05.000Z07:00"))
		}
		fmt.Fprintf(&b, "#EXTINF:%.3f,\nseg%d.ts\n", segDur, i)
	}
	return b.String()
}

func TestLookupAnchorFreshVsRestart(t *testing.T) {
	m := anchorManager(t)
	start := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != "test-agent" {
			t.Errorf("user-agent = %q", r.Header.Get("User-Agent"))
		}
		_, _ = io.WriteString(w, mediaPlaylist(start, 5, 5.0))
	}))
	defer srv.Close()
	s := &Session{Source: srv.URL, Type: "hls"}

	// Fresh start: ffmpeg begins at max(n-3,0) = index 2 -> start + 10s.
	at, src := m.lookupAnchor(context.Background(), s, false)
	if src != "hls-pdt" || !at.Equal(start.Add(10*time.Second)) {
		t.Fatalf("fresh = %v %q, want %v hls-pdt", at, src, start.Add(10*time.Second))
	}
	// Restart: newest segment, index 4 -> start + 20s.
	at, src = m.lookupAnchor(context.Background(), s, true)
	if src != "hls-pdt" || !at.Equal(start.Add(20*time.Second)) {
		t.Fatalf("restart = %v %q, want %v hls-pdt", at, src, start.Add(20*time.Second))
	}
}

func TestLookupAnchorEndListAndInheritance(t *testing.T) {
	m := anchorManager(t)
	start := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, mediaPlaylist(start, 4, 6.0)+"#EXT-X-ENDLIST\n")
	}))
	defer srv.Close()
	s := &Session{Source: srv.URL, Type: "hls"}
	// ENDLIST -> index 0, and the anchor is the declared PDT of segment 0.
	at, src := m.lookupAnchor(context.Background(), s, true)
	if src != "hls-pdt" || !at.Equal(start) {
		t.Fatalf("endlist = %v %q, want %v", at, src, start)
	}
}

func TestLookupAnchorMasterFollow(t *testing.T) {
	m := anchorManager(t)
	start := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	mux := http.NewServeMux()
	mux.HandleFunc("/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=128000\nmedia.m3u8\n")
	})
	mux.HandleFunc("/media.m3u8", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, mediaPlaylist(start, 5, 5.0))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	s := &Session{Source: srv.URL + "/master.m3u8", Type: "hls"}
	at, src := m.lookupAnchor(context.Background(), s, false)
	if src != "hls-pdt" || !at.Equal(start.Add(10*time.Second)) {
		t.Fatalf("master follow = %v %q, want %v", at, src, start.Add(10*time.Second))
	}
}

func TestLookupAnchorMissingPDT(t *testing.T) {
	m := anchorManager(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "#EXTM3U\n#EXTINF:5,\na.ts\n#EXTINF:5,\nb.ts\n")
	}))
	defer srv.Close()
	s := &Session{Source: srv.URL, Type: "hls"}
	at, src := m.lookupAnchor(context.Background(), s, true)
	if src != "wallclock" || !at.IsZero() {
		t.Fatalf("missing PDT = %v %q, want zero wallclock", at, src)
	}
}

func TestLookupAnchorServerError(t *testing.T) {
	m := anchorManager(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	s := &Session{Source: srv.URL, Type: "hls"}
	at, src := m.lookupAnchor(context.Background(), s, false)
	if src != "wallclock" || !at.IsZero() {
		t.Fatalf("500 = %v %q, want zero wallclock", at, src)
	}
}

func TestLookupAnchorTimeout(t *testing.T) {
	m := anchorManager(t)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	s := &Session{Source: srv.URL, Type: "hls"}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	at, src := m.lookupAnchor(ctx, s, false)
	if src != "wallclock" || !at.IsZero() {
		t.Fatalf("timeout = %v %q, want zero wallclock", at, src)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("timeout took too long: %s", time.Since(start))
	}
}

func TestLookupAnchorDisabledOrNonHLS(t *testing.T) {
	m := anchorManager(t)
	// Non-HLS source: no lookup, wallclock now.
	at, src := m.lookupAnchor(context.Background(), &Session{Source: "http://x/stream", Type: "icecast"}, false)
	if src != "wallclock" || at.IsZero() {
		t.Fatalf("non-hls = %v %q, want now wallclock", at, src)
	}
	// Lookup disabled.
	m.cfg = func() *config.Config { c := *m.cfg; c.ClockPDTLookup = false; return &c }()
	at, src = m.lookupAnchor(context.Background(), &Session{Source: "http://x/a.m3u8", Type: "hls"}, false)
	if src != "wallclock" || at.IsZero() {
		t.Fatalf("disabled = %v %q, want now wallclock", at, src)
	}
}
