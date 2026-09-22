package recorder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spectado/stream-recorder/internal/metrics"
	"github.com/spectado/stream-recorder/internal/schedule"
)

// seedFinalized writes a finished session (sidecar + ADTS capture) into a data
// directory so Recover() picks it up exactly as it would after a restart.
func seedFinalized(t *testing.T, root, id, sid string, start time.Time, key string, frames int) *Session {
	t.Helper()
	safe := schedule.SafeID(id)
	end := start.Add(time.Hour)
	s := &Session{
		ID: id, SafeID: safe, SessionID: sid, Name: "Test " + id,
		Source: "http://example.invalid/" + id + ".m3u8",
		Start:  start, End: end, SessionStart: start, SessionEnd: &end,
		State: StateFinalized, FinishReason: ReasonEnded, Codec: "aac", ResolvedCodec: "aac",
		dir: filepath.Join(root, "recordings", safe),
	}
	s.setKey(key)
	data := adtsStream(frames)
	s.Bytes, s.Size = int64(len(data)), int64(len(data))
	s.Runs = []Run{{
		StartedAt: start, EndedAt: &end, Anchor: start, AnchorSource: "wallclock",
		Offset: 0, Bytes: int64(len(data)), Frames: frames,
	}}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.FilePath(), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	return s
}

// newRecoverManager builds a manager over dir with a fake remuxer, without the
// fake ffmpeg (nothing is captured in these tests).
func newRecoverManager(t *testing.T, dir string, up Uploader) (*Manager, *fakeRemuxer) {
	t.Helper()
	m := NewManager(testConfig(dir, "ffmpeg"), slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics.New("test"), up, "test")
	rx := newFakeRemuxer()
	m.SetRemuxer(rx)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})
	return m, rx
}

// shortDefer makes the "a sibling is still recording" wait test-sized.
func shortDefer(t *testing.T, d time.Duration) {
	t.Helper()
	old := deferInterval
	deferInterval = d
	t.Cleanup(func() { deferInterval = old })
}

// TestRotationDefersAndMergesLocally covers the MAX_SESSION_DURATION case: the
// first part's upload waits while the second part is still capturing, and both
// are then merged into ONE object by a single upload.
func TestRotationDefersAndMergesLocally(t *testing.T) {
	shortDefer(t, 50*time.Millisecond)
	up := &fakeUploader{}
	m, _ := newTestManager(t, up)
	m.cfg.MaxSessionDuration = time.Minute
	now := time.Now()
	it := item("a", now.Add(-time.Second), now.Add(time.Hour))
	m.SetSchedule(sched(it))
	first := m.activeFor("a")
	if first == nil {
		t.Fatal("not started")
	}
	waitFor(t, 5*time.Second, "bytes", func() bool { return first.bytes.Load() > 0 })

	m.Reconcile(now.Add(2 * time.Minute)) // rotate
	second := m.activeFor("a")
	if second == nil || second == first {
		t.Fatal("rotation should start a second session")
	}
	if second.s.Key != first.s.Key {
		t.Fatalf("rotated session key = %q, want the pinned %q", second.s.Key, first.s.Key)
	}
	waitFor(t, 5*time.Second, "bytes of the second part", func() bool { return second.bytes.Load() > 0 })
	// While the second part records, the first part's upload must not run.
	if up.count() != 0 {
		t.Fatalf("uploaded %d objects while a sibling was still recording", up.count())
	}

	m.Reconcile(now.Add(2 * time.Hour))
	m.settle()

	if up.count() != 1 {
		t.Fatalf("uploads = %d, want exactly one merged object: %+v", up.count(), up.calls)
	}
	call := up.calls[0]
	if call.meta["parts"] != "2" || call.replace != "" {
		t.Fatalf("merged upload = %+v", call)
	}
	want := sessionDigest(first.s.SessionID) + "," + sessionDigest(second.s.SessionID)
	if call.meta["sessions"] != want {
		t.Fatalf("sessions = %q, want %q (capture order)", call.meta["sessions"], want)
	}
	for _, sid := range []string{first.s.SessionID, second.s.SessionID} {
		if st, _ := m.sessionState(sid); st != StateUploaded {
			t.Fatalf("session %s state = %s", sid, st)
		}
	}
	// Both captures are gone; a local merge never leaves one behind.
	if up.downloads != 0 {
		t.Fatalf("a local merge must not download anything (downloads=%d)", up.downloads)
	}
}

// TestRemoteMergeIsIdempotent covers the crash-between-upload-and-sidecar case:
// the object already lists this session, so the attempt completes it without
// uploading anything again.
func TestRemoteMergeIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
	up.seed(key, adtsStream(30), map[string]string{
		"recording-id":       "a",
		"sessions":           sessionDigest(s.SessionID),
		"parts":              "1",
		"last-session-start": start.UTC().Format(time.RFC3339),
	})
	m, rx := newRecoverManager(t, dir, up)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	if up.count() != 0 {
		t.Fatalf("session already in the object must not be uploaded again: %+v", up.calls)
	}
	if rx.builds != 0 {
		t.Fatalf("nothing to add means nothing to remux (builds=%d)", rx.builds)
	}
	if _, err := os.Stat(s.FilePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("capture should be removed once the object holds it: %v", err)
	}
}

// TestInconsistentManifestBlocksUpload covers HEAD case (c): a manifest that
// contradicts itself must never be appended to, and the capture stays.
func TestInconsistentManifestBlocksUpload(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
	up.seed(key, adtsStream(30), map[string]string{
		"recording-id": "a",
		"sessions":     "aaaaaaaa,bbbbbbbb",
		"parts":        "1", // contradicts the two digests
	})
	m, _ := newRecoverManager(t, dir, up)
	// The blocked loop never finishes on its own; cut the shutdown wait short
	// (cleanups run last-registered-first, so this one wins).
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		m.Shutdown(ctx)
	})
	m.Recover()

	waitFor(t, 5*time.Second, "upload blocked", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.sessions[s.SessionID] != nil && m.sessions[s.SessionID].UploadBlocked
	})
	m.mu.Lock()
	st, lastErr := m.sessions[s.SessionID].State, m.sessions[s.SessionID].LastError
	m.mu.Unlock()
	if st != StateFinalized || !strings.Contains(lastErr, "manifest") {
		t.Fatalf("state=%s lastError=%q", st, lastErr)
	}
	if up.count() != 0 {
		t.Fatalf("nothing may be written over an unreadable manifest: %+v", up.calls)
	}
	if _, err := os.Stat(s.FilePath()); err != nil {
		t.Fatalf("capture must stay while the upload is blocked: %v", err)
	}
}

// TestOutOfOrderSessionGetsNewKey covers HEAD case (d): a session older than
// the object's last part cannot be appended (the timeline would run backwards),
// so it goes to its own object.
func TestOutOfOrderSessionGetsNewKey(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-4 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
	up.seed(key, adtsStream(30), map[string]string{
		"recording-id":       "a",
		"sessions":           "aaaaaaaa",
		"parts":              "1",
		"last-session-start": start.Add(time.Hour).UTC().Format(time.RFC3339),
	})
	m, _ := newRecoverManager(t, dir, up)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	want := strings.TrimSuffix(key, ".m4a") + "-2.m4a"
	if up.count() != 1 || up.calls[0].key != want {
		t.Fatalf("calls = %+v, want one upload to %s", up.calls, want)
	}
	if n := counterValue(t, m, "recorder_object_key_renames_total", map[string]string{"reason": "out-of-order"}); n != 1 {
		t.Fatalf("rename metric = %v, want 1", n)
	}
}

// TestNoConditionalWritesRefusesMerge: without a store that enforces If-Match a
// merge could silently drop the part that is already there, so the new session
// gets its own object instead.
func TestNoConditionalWritesRefusesMerge(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{noCond: true}
	up.seed(key, adtsStream(30), map[string]string{
		"recording-id":       "a",
		"sessions":           "aaaaaaaa",
		"parts":              "1",
		"last-session-start": start.Add(-time.Hour).UTC().Format(time.RFC3339),
	})
	m, _ := newRecoverManager(t, dir, up)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	want := strings.TrimSuffix(key, ".m4a") + "-2.m4a"
	if up.count() != 1 || up.calls[0].key != want {
		t.Fatalf("calls = %+v, want one upload to %s", up.calls, want)
	}
	if n := counterValue(t, m, "recorder_object_key_renames_total", map[string]string{"reason": "no-conditional-writes"}); n != 1 {
		t.Fatalf("rename metric = %v, want 1", n)
	}
}

// TestRemuxFailuresFallBackToRawADTS: recorded audio must never stay out of the
// bucket because ffmpeg cannot digest it. After three failed remuxes the raw
// capture is stored, clearly marked.
func TestRemuxFailuresFallBackToRawADTS(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
	m, rx := newRecoverManager(t, dir, up)
	boom := errors.New("remux: ffmpeg exited 1: Invalid data found when processing input")
	rx.setErrs(boom, boom, boom)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	wantKey := strings.TrimSuffix(key, ".m4a") + ".aac"
	if up.count() != 1 || up.calls[0].key != wantKey {
		t.Fatalf("calls = %+v, want one upload to %s", up.calls, wantKey)
	}
	call := up.calls[0]
	if call.contentType != "audio/aac" || call.meta["remux-failed"] != "true" {
		t.Fatalf("fallback upload = %+v", call)
	}
	// The fallback object carries the same manifest as an .m4a would: the next
	// attempt recognises its own capture there instead of overwriting it.
	if call.meta["recording-id"] != "a" || call.meta["parts"] != "1" ||
		call.meta["sessions"] != sessionDigest(s.SessionID) || call.meta["last-session-start"] == "" {
		t.Fatalf("fallback object still needs its manifest: %v", call.meta)
	}
	if n := counterValue(t, m, "recorder_upload_fallback_total", nil); n != 1 {
		t.Fatalf("fallback metric = %v, want 1", n)
	}
	if n := counterValue(t, m, "recorder_remux_total", map[string]string{"result": "failure"}); n != 3 {
		t.Fatalf("remux failure metric = %v, want 3", n)
	}
	if _, err := os.Stat(s.FilePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("capture should be removed after the fallback upload: %v", err)
	}
}

// TestNoDiskSpaceRetriesWithoutRemuxFailure: too little free disk is a
// transient condition, not a broken recording — it must not count toward the
// remux-failure budget that leads to the raw-ADTS fallback.
func TestNoDiskSpaceRetriesWithoutRemuxFailure(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
	m, rx := newRecoverManager(t, dir, up)
	var full testFlag
	full.set(true)
	m.SetDiskFree(func() (uint64, bool) {
		if full.get() {
			return 10, true
		}
		return 1 << 40, true
	})
	m.Recover()

	waitFor(t, 5*time.Second, "nospace retry", func() bool {
		return counterValue(t, m, "recorder_remux_total", map[string]string{"result": "nospace"}) > 0
	})
	if up.count() != 0 || rx.builds != 0 {
		t.Fatalf("nothing may be built or uploaded without disk: uploads=%d builds=%d", up.count(), rx.builds)
	}
	m.mu.Lock()
	failures := m.sessions[s.SessionID].RemuxFailures
	m.mu.Unlock()
	if failures != 0 {
		t.Fatalf("remux failures = %d, want 0 (a full disk is not a remux failure)", failures)
	}
	if _, err := os.Stat(s.FilePath()); err != nil {
		t.Fatalf("capture must stay: %v", err)
	}

	full.set(false)
	waitFor(t, 10*time.Second, "upload after the disk recovered", func() bool { return up.count() == 1 })
	m.settle()
	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
}

// TestKeptModeRemuxesAndRecoverReRuns covers UPLOAD_DISABLED=true: the capture
// is remuxed to a local .m4a and only then deleted, and a crash before the
// remux finished is repaired on the next start.
func TestKeptModeRemuxesAndRecoverReRuns(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)
	capture := s.FilePath()
	kept := s.OutputPath()
	original, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}

	m, rx := newRecoverManager(t, dir, nil)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateKept {
		t.Fatalf("state = %s", st)
	}
	if rx.builds != 1 {
		t.Fatalf("builds = %d, want 1", rx.builds)
	}
	if _, err := os.Stat(kept); err != nil {
		t.Fatalf("kept .m4a missing: %v", err)
	}
	if _, err := os.Stat(capture); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("capture should be gone once the remux succeeded: %v", err)
	}
	views := m.Recordings()
	if len(views) != 1 || views[0].OutputFile != kept || views[0].File != "" {
		t.Fatalf("view = %+v", views[0])
	}

	// A crash between the remux and the sidecar leaves the .aac without a .m4a;
	// the next start must remux it again.
	if err := os.Remove(kept); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(capture, original, 0o644); err != nil {
		t.Fatal(err)
	}
	m2, rx2 := newRecoverManager(t, dir, nil)
	m2.Recover()
	m2.settle()
	if rx2.builds != 1 {
		t.Fatalf("recover builds = %d, want the kept remux to run again", rx2.builds)
	}
	if _, err := os.Stat(kept); err != nil {
		t.Fatalf("kept .m4a missing after recover: %v", err)
	}
}

// TestKeyPinningAcrossMidnight: a show whose start is moved past midnight keeps
// the object of the session that is already recording it — a recording must
// never be split across two date folders.
func TestKeyPinningAcrossMidnight(t *testing.T) {
	up := &fakeUploader{}
	m, _ := newTestManager(t, up)
	night := time.Date(2026, 9, 22, 23, 30, 0, 0, time.UTC)
	it := item("a", night, night.Add(40*time.Minute)) // ends 00:10 the next day
	m.SetSchedule(sched(it))
	m.Reconcile(night.Add(time.Minute))
	first := m.activeFor("a")
	if first == nil {
		t.Fatal("not started")
	}
	if first.s.Key != "2026-09-22/a.m4a" {
		t.Fatalf("key = %q", first.s.Key)
	}
	waitFor(t, 5*time.Second, "bytes", func() bool { return first.bytes.Load() > 0 })

	// The show ends, then the producer re-announces it for after midnight.
	m.Reconcile(night.Add(time.Hour)) // 00:30, past the end
	waitFor(t, 5*time.Second, "first session finalized", func() bool {
		st, _ := m.sessionState(first.s.SessionID)
		return st != StateRecording
	})
	moved := item("a", night.Add(35*time.Minute), night.Add(2*time.Hour)) // 00:05 -> 01:30
	m.SetSchedule(sched(moved))
	m.Reconcile(night.Add(time.Hour))
	second := m.activeFor("a")
	if second == nil {
		t.Fatal("re-announced show should start a new session")
	}
	if second.s.Key != first.s.Key {
		t.Fatalf("second session key = %q, want the pinned %q (one object per recording)", second.s.Key, first.s.Key)
	}
}

// testFlag is a tiny helper for a flag flipped by the test goroutine and read
// by the manager's.
type testFlag struct {
	mu sync.Mutex
	v  bool
}

func (a *testFlag) set(v bool) { a.mu.Lock(); a.v = v; a.mu.Unlock() }
func (a *testFlag) get() bool  { a.mu.Lock(); defer a.mu.Unlock(); return a.v }

// adtsFrame48 is adtsFrame with sampling_frequency_index 3 (48 kHz) instead of
// 4 (44.1 kHz): a capture that switches between them cannot be described by one
// MP4 sample description.
func adtsFrame48(payload int) []byte {
	f := adtsFrame(payload)
	f[2] = 0x4C
	return f
}

// TestHeterogeneousCaptureIsTranscoded: when the origin's encoder changed
// parameters mid-capture a stream copy would silently produce a short, pitched
// file, so the homogeneous chunks are re-encoded instead and the object says so.
func TestHeterogeneousCaptureIsTranscoded(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 20)

	// Append 20 frames at a different sample rate.
	f, err := os.OpenFile(s.FilePath(), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := f.Write(adtsFrame48(200)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	up := &fakeUploader{}
	m, rx := newRecoverManager(t, dir, up)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	if rx.transcodes != 1 || rx.builds != 0 {
		t.Fatalf("transcodes=%d builds=%d, want the re-encoding path", rx.transcodes, rx.builds)
	}
	meta := up.calls[0].meta
	if meta["transcoded"] != "true" || meta["stream-params-changed"] != "true" {
		t.Fatalf("object metadata must record the re-encode: %v", meta)
	}
	if n := counterValue(t, m, "recorder_remux_transcoded_total", nil); n != 1 {
		t.Fatalf("transcode metric = %v, want 1", n)
	}
	views := m.Recordings()
	if len(views) != 1 || !views[0].Transcoded {
		t.Fatalf("view = %+v", views)
	}
	// No chunk files left behind.
	entries, _ := os.ReadDir(s.Dir())
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Fatalf("temporary file left behind: %s", e.Name())
		}
	}
}

// TestCapRunTableCoalescesOldestEntries: the position -> wall-clock map may not
// grow without bound, but it must stay continuous, so the oldest entries are
// merged rather than dropped.
func TestCapRunTableCoalescesOldestEntries(t *testing.T) {
	var entries []runEntry
	off := 0.0
	for i := 0; i < 10; i++ {
		sid := "s1"
		if i >= 5 {
			sid = "s2"
		}
		entries = append(entries, runEntry{SID: sid, Off: off, Dur: 10})
		off += 10
	}
	got := capRunTable(entries, 6)
	if len(got) != 6 {
		t.Fatalf("len = %d, want 6", len(got))
	}
	if got[0].SID != "s1" || got[0].Off != 0 || got[0].Dur != 50 {
		t.Fatalf("oldest entry = %+v, want one s1 entry spanning 50 s", got[0])
	}
	if got[1].SID != "s2" || got[1].Off != 50 {
		t.Fatalf("second entry = %+v", got[1])
	}
	var total float64
	for _, e := range got {
		total += e.Dur
	}
	if total != 100 {
		t.Fatalf("coalescing lost time: %v of 100 s", total)
	}
	// Nothing to do when the table already fits.
	if n := len(capRunTable(entries, 20)); n != 10 {
		t.Fatalf("len = %d, want the table unchanged", n)
	}
}

// TestFallbackNeverOverwritesAForeignObject: the raw-ADTS fallback key is an
// object key like any other. When something else already sits there the capture
// goes to a new key instead of over the top of it — before the fix the PUT was
// unconditional, so a second session of the same recording silently destroyed
// the first one's fallback audio.
func TestFallbackNeverOverwritesAForeignObject(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	fbKey := strings.TrimSuffix(key, ".m4a") + ".aac"
	seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
	up.seed(fbKey, []byte("someone else's audio"), map[string]string{"foo": "bar"})
	m, rx := newRecoverManager(t, dir, up)
	boom := errors.New("remux: ffmpeg exited 1: Invalid data found when processing input")
	rx.setErrs(boom, boom, boom)
	m.Recover()
	m.settle()

	if got := string(up.object(fbKey).body); got != "someone else's audio" {
		t.Fatalf("the fallback object was overwritten: %q", got)
	}
	for _, c := range up.calls {
		if c.key == fbKey {
			t.Fatalf("nothing may be written to an occupied fallback key: %+v", c)
		}
	}
	want := strings.TrimSuffix(key, ".m4a") + "-2.m4a"
	if up.count() != 1 || up.calls[0].key != want {
		t.Fatalf("calls = %+v, want one upload to %s", up.calls, want)
	}
	if n := counterValue(t, m, "recorder_object_key_renames_total", map[string]string{"reason": "remux-failed"}); n != 1 {
		t.Fatalf("rename metric = %v, want 1", n)
	}
}

// TestFallbackIsIdempotent: a crash between the fallback upload and the sidecar
// writes leaves our own capture under the fallback key. The next attempt must
// recognise it and converge instead of uploading (or renaming) again.
func TestFallbackIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	fbKey := strings.TrimSuffix(key, ".m4a") + ".aac"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
	up.seed(fbKey, adtsStream(30), map[string]string{
		"recording-id":       "a",
		"sessions":           sessionDigest(s.SessionID),
		"parts":              "1",
		"last-session-start": start.UTC().Format(time.RFC3339),
		"remux-failed":       "true",
	})
	m, rx := newRecoverManager(t, dir, up)
	boom := errors.New("remux: ffmpeg exited 1: Invalid data found when processing input")
	rx.setErrs(boom, boom, boom)
	m.Recover()
	m.settle()

	if up.count() != 0 {
		t.Fatalf("a capture the fallback object already holds must not be uploaded again: %+v", up.calls)
	}
	if n := counterValue(t, m, "recorder_object_key_renames_total", nil); n != 0 {
		t.Fatalf("rename metric = %v, want 0 (the object is ours and complete)", n)
	}
	m.mu.Lock()
	state, gotKey := m.sessions[s.SessionID].State, m.sessions[s.SessionID].Key
	m.mu.Unlock()
	if state != StateUploaded || gotKey != fbKey {
		t.Fatalf("state=%s key=%q, want uploaded under %s", state, gotKey, fbKey)
	}
	if _, err := os.Stat(s.FilePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("capture should be removed once the object holds it: %v", err)
	}
}

// TestImmediateRetriesAreBounded: "the object moved under us, look again" is
// normally self-limiting, but a store that keeps answering the same way would
// otherwise spin the claim/scan/remux/PUT cycle with no delay and no visible
// attempt. After a few in a row the loop has to back off like any other failure.
func TestImmediateRetriesAreBounded(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
	errs := make([]error, 40)
	for i := range errs {
		errs[i] = ErrObjectExists
	}
	up.setErrs(errs...)
	m, _ := newRecoverManager(t, dir, up)
	m.Recover()

	var attempts int
	var next *time.Time
	waitFor(t, 10*time.Second, "the upload loop to back off", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		sess := m.sessions[s.SessionID]
		attempts, next = sess.UploadAttempts, sess.NextUploadAt
		return attempts >= 2 && next != nil
	})
	// Bounded: a spinning loop would have HEADed the key hundreds of times in
	// the time these two counted attempts took.
	if heads := up.headCount(); heads > (attempts+1)*(maxImmediateRetries+1) {
		t.Fatalf("%d attempts after %d HEADs: the loop is spinning instead of backing off", attempts, heads)
	}
}

// TestPartsCapGetsNewKey covers HEAD case (d): an object that already holds the
// maximum number of parts cannot take another one (the `sessions` manifest has
// to stay inside the metadata budget), so the session goes to its own object.
func TestPartsCapGetsNewKey(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	digests := make([]string, maxPartsPerObject)
	for i := range digests {
		digests[i] = sessionDigest(fmt.Sprintf("a_part%02d", i))
	}
	up := &fakeUploader{}
	up.seed(key, adtsStream(30), map[string]string{
		"recording-id":       "a",
		"sessions":           strings.Join(digests, ","),
		"parts":              strconv.Itoa(maxPartsPerObject),
		"last-session-start": start.Add(-time.Hour).UTC().Format(time.RFC3339),
	})
	m, _ := newRecoverManager(t, dir, up)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	want := strings.TrimSuffix(key, ".m4a") + "-2.m4a"
	if up.count() != 1 || up.calls[0].key != want {
		t.Fatalf("calls = %+v, want one upload to %s", up.calls, want)
	}
	if n := counterValue(t, m, "recorder_object_key_renames_total", map[string]string{"reason": "parts-cap"}); n != 1 {
		t.Fatalf("rename metric = %v, want 1", n)
	}
	if up.downloads != 0 {
		t.Fatalf("a full object must not be downloaded (downloads=%d)", up.downloads)
	}
}

// TestMergeKeepsTheLocalName: storage RFC-2047-encodes a non-ASCII name, so the
// value a HEAD returns is not the name. A merge must take the name from the
// local sidecar, or every merged recording would end up titled "=?utf-8?b?…?=".
func TestMergeKeepsTheLocalName(t *testing.T) {
	const name = "Rádio Jedna"
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)
	s.Name = name
	if err := s.save(); err != nil {
		t.Fatal(err)
	}

	up := &fakeUploader{}
	up.seed(key, adtsStream(30), map[string]string{
		"recording-id":       "a",
		"sessions":           "aaaaaaaa",
		"parts":              "1",
		"last-session-start": start.Add(-time.Hour).UTC().Format(time.RFC3339),
		"name":               mime.BEncoding.Encode("utf-8", name),
	})
	m, rx := newRecoverManager(t, dir, up)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	if up.count() != 1 || up.calls[0].replace == "" {
		t.Fatalf("calls = %+v, want one merging replace", up.calls)
	}
	if got := up.calls[0].meta["name"]; got != name {
		t.Fatalf("object name = %q, want %q", got, name)
	}
	meta, ok := rx.lastMeta()
	if !ok || meta.Title != name {
		t.Fatalf("mp4 title = %q (set=%v), want %q", meta.Title, ok, name)
	}
}

// TestDecodeMetaName: an encoded word becomes the name again, anything else is
// passed through unchanged.
func TestDecodeMetaName(t *testing.T) {
	if got := decodeMetaName(mime.BEncoding.Encode("utf-8", "Rádio Jedna")); got != "Rádio Jedna" {
		t.Errorf("decoded = %q", got)
	}
	if got := decodeMetaName("Plain Name"); got != "Plain Name" {
		t.Errorf("plain = %q", got)
	}
	if got := decodeMetaName("=?utf-8?b?not base64!?="); got != "=?utf-8?b?not base64!?=" {
		t.Errorf("undecodable = %q, want it verbatim", got)
	}
	if got := decodeMetaName("  "); got != "" {
		t.Errorf("empty = %q", got)
	}
}
