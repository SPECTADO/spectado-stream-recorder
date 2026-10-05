package recorder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spectado/stream-recorder/internal/metrics"
	"github.com/spectado/stream-recorder/internal/remux"
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

// ---------------------------------------------------------------------------
// video companion (RECORD_VIDEO)
// ---------------------------------------------------------------------------

// fakeVideoRemuxer adds the video builders to fakeRemuxer without ffmpeg: a
// "video" is the audio input's bytes behind a marker, so its fake Probe still
// counts the audio frames. Every build is recorded with its kind and inputs.
type fakeVideoRemuxer struct {
	*fakeRemuxer
	vmu    sync.Mutex
	kinds  []string
	inputs [][]string
	verrs  []error
	info   remux.VideoInfo // what ProbeVideo reports for every TS file
}

func newFakeVideoRemuxer() *fakeVideoRemuxer {
	return &fakeVideoRemuxer{fakeRemuxer: newFakeRemuxer(), info: remux.VideoInfo{
		HasVideo: true, VideoCodec: "h264", VideoProfile: "High", PixFmt: "yuv420p", Width: 320, Height: 240, FPS: 25,
		AudioCodec: "aac", SampleRate: 48000, Channels: 1,
	}}
}

func (f *fakeVideoRemuxer) setVideoErrs(errs ...error) {
	f.vmu.Lock()
	defer f.vmu.Unlock()
	f.verrs = errs
}

func (f *fakeVideoRemuxer) builds() []string {
	f.vmu.Lock()
	defer f.vmu.Unlock()
	return append([]string(nil), f.kinds...)
}

// record notes one build and pops the next scripted error.
func (f *fakeVideoRemuxer) record(kind string, inputs []string) error {
	f.vmu.Lock()
	defer f.vmu.Unlock()
	f.kinds = append(f.kinds, kind)
	f.inputs = append(f.inputs, inputs)
	if len(f.verrs) == 0 {
		return nil
	}
	err := f.verrs[0]
	f.verrs = f.verrs[1:]
	return err
}

func (f *fakeVideoRemuxer) ProbeVideo(ctx context.Context, path string) (remux.VideoInfo, error) {
	return f.info, nil
}

func (f *fakeVideoRemuxer) fakeVideo(ctx context.Context, audio []string, out string, w, h int) (remux.Result, error) {
	var body []byte
	for _, p := range audio {
		data, err := os.ReadFile(p)
		if err != nil {
			return remux.Result{}, err
		}
		body = append(body, data...)
	}
	if err := os.WriteFile(out, body, 0o644); err != nil {
		return remux.Result{}, err
	}
	res, err := f.Probe(ctx, out)
	if err != nil {
		return remux.Result{}, err
	}
	res.Format = "mov,mp4,m4a,3gp,3g2,mj2"
	res.Video = remux.VideoInfo{HasVideo: true, VideoCodec: "h264", Width: w, Height: h}
	return res, nil
}

func (f *fakeVideoRemuxer) BuildBlackVideo(ctx context.Context, audioM4A, out string, meta remux.Metadata) (remux.Result, error) {
	if err := f.record("black", []string{audioM4A}); err != nil {
		return remux.Result{}, err
	}
	return f.fakeVideo(ctx, []string{audioM4A}, out, remux.BlackVideoWidth, remux.BlackVideoHeight)
}

func (f *fakeVideoRemuxer) BuildVideoConcat(ctx context.Context, tsFiles []string, out string, meta remux.Metadata) (remux.Result, error) {
	if err := f.record("copy", tsFiles); err != nil {
		return remux.Result{}, err
	}
	return f.fakeVideo(ctx, tsFiles, out, f.info.Width, f.info.Height)
}

func (f *fakeVideoRemuxer) ExtractVideoTS(ctx context.Context, mp4In, tsOut string) error {
	if err := f.record("extract", []string{mp4In}); err != nil {
		return err
	}
	data, err := os.ReadFile(mp4In)
	if err != nil {
		return err
	}
	return os.WriteFile(tsOut, data, 0o644)
}

func (f *fakeVideoRemuxer) BuildVideoTranscode(ctx context.Context, segs []remux.VideoSegment, out string, w, h int, fps float64, bitrate string, meta remux.Metadata) (remux.Result, error) {
	var in []string
	for _, s := range segs {
		in = append(in, s.TS+s.Audio)
	}
	if err := f.record("transcode", in); err != nil {
		return remux.Result{}, err
	}
	return f.fakeVideo(ctx, in, out, w, h)
}

// newVideoRecoverManager is newRecoverManager with the fake video remuxer.
func newVideoRecoverManager(t *testing.T, dir string, up Uploader) (*Manager, *fakeVideoRemuxer) {
	t.Helper()
	m, _ := newRecoverManager(t, dir, up)
	rx := newFakeVideoRemuxer()
	m.SetRemuxer(rx)
	return m, rx
}

// markVideo turns a seeded session into one created with RECORD_VIDEO=true.
func markVideo(t *testing.T, s *Session, source string) {
	t.Helper()
	s.Video, s.VideoSource = true, source
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
}

// videoFields reads a session's video outcome under the lock.
func videoFields(m *Manager, sid string) (uploaded bool, videoErr, outVideo string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[sid]
	if s == nil {
		return false, "", ""
	}
	return s.VideoUploaded, s.VideoError, s.OutputVideoFile
}

func (f *fakeUploader) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	for i, c := range f.calls {
		out[i] = c.key
	}
	return out
}

// TestVideoOffUploadsNoCompanion: with the switch off (the default, and every
// session from an older sidecar) the upload is exactly what it was: one HEAD,
// one .m4a, no video remuxer call.
func TestVideoOffUploadsNoCompanion(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
	m, rx := newVideoRecoverManager(t, dir, up)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	if got := up.keys(); !slices.Equal(got, []string{key}) {
		t.Fatalf("uploads = %q, want only the .m4a", got)
	}
	if up.headCount() != 1 {
		t.Fatalf("heads = %d, want 1 (the .mp4 key must not even be looked at)", up.headCount())
	}
	if b := rx.builds(); len(b) != 0 {
		t.Fatalf("video builds = %q, want none", b)
	}
	call := up.calls[0]
	for _, k := range []string{"media", "synthetic-video", "video-kind", "video-codec", "width", "height"} {
		if _, ok := call.meta[k]; ok {
			t.Errorf("audio-only object carries video metadata %q", k)
		}
	}
	if ok, verr, _ := videoFields(m, s.SessionID); ok || verr != "" {
		t.Fatalf("videoUploaded=%v videoError=%q on a session without video", ok, verr)
	}
}

// TestVideoAudioOnlySessionStoresBlackCompanionFirst: a Video session that
// captured no picture (audio-only source) gets a black .mp4 built from the
// finished .m4a, stored BEFORE the .m4a and with the same manifest.
func TestVideoAudioOnlySessionStoresBlackCompanionFirst(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	vkey := strings.TrimSuffix(key, ".m4a") + ".mp4"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)
	markVideo(t, s, "none")

	up := &fakeUploader{}
	m, rx := newVideoRecoverManager(t, dir, up)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	if got := up.keys(); !slices.Equal(got, []string{vkey, key}) {
		t.Fatalf("uploads = %q, want the .mp4 and then the .m4a", got)
	}
	if b := rx.builds(); !slices.Equal(b, []string{"black"}) {
		t.Fatalf("video builds = %q, want one black", b)
	}
	if in := rx.inputs[0][0]; !strings.HasPrefix(filepath.Base(in), ".tmp-upload-") || filepath.Ext(in) != ".m4a" {
		t.Fatalf("black video built from %q, want the .m4a of this attempt", in)
	}
	v, a := up.calls[0], up.calls[1]
	if v.contentType != "video/mp4" || a.contentType != "audio/mp4" {
		t.Fatalf("content types = %q, %q", v.contentType, a.contentType)
	}
	want := map[string]string{
		"media": "video", "synthetic-video": "true", "video-kind": "black", "video-codec": "h264",
		"width": "16", "height": "16",
	}
	for k, w := range want {
		if v.meta[k] != w {
			t.Errorf("mp4 metadata %s = %q, want %q", k, v.meta[k], w)
		}
	}
	for _, k := range []string{"recording-id", "sessions", "parts", "last-session-start", "name", "frames"} {
		if v.meta[k] == "" || v.meta[k] != a.meta[k] {
			t.Errorf("mp4 metadata %s = %q, the .m4a says %q", k, v.meta[k], a.meta[k])
		}
	}
	if _, ok := a.meta["media"]; ok {
		t.Errorf("the .m4a must not change: it carries media=%q", a.meta["media"])
	}
	if ok, verr, _ := videoFields(m, s.SessionID); !ok || verr != "" {
		t.Fatalf("videoUploaded=%v videoError=%q", ok, verr)
	}
	if n := counterValue(t, m, "recorder_video_builds_total", map[string]string{"kind": "black", "result": "success"}); n != 1 {
		t.Fatalf("video build metric = %v, want 1", n)
	}
}

// TestVideoForeignCompanionRenamesGroup: an object under the derived .mp4 key
// that is not this recording's is a conflict exactly like one under the .m4a
// key — the group moves on, nothing is overwritten.
func TestVideoForeignCompanionRenamesGroup(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	vkey := strings.TrimSuffix(key, ".m4a") + ".mp4"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)
	markVideo(t, s, "none")

	up := &fakeUploader{}
	up.seed(vkey, []byte("someone else's video"), map[string]string{"title": "not ours"})
	m, _ := newVideoRecoverManager(t, dir, up)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	base := strings.TrimSuffix(key, ".m4a")
	if got := up.keys(); !slices.Equal(got, []string{base + "-2.mp4", base + "-2.m4a"}) {
		t.Fatalf("uploads = %q, want both objects under the -2 key", got)
	}
	if o := up.object(vkey); string(o.body) != "someone else's video" {
		t.Fatal("the foreign object was overwritten")
	}
	if n := counterValue(t, m, "recorder_object_key_renames_total", map[string]string{"reason": "conflict"}); n != 1 {
		t.Fatalf("rename metric = %v, want 1", n)
	}
}

// TestVideoBuildFailuresStoreAudioWithoutCompanion: a companion that cannot be
// built never costs the audio. After maxRemuxFailures failed builds the .m4a
// is stored without it and the session says why.
func TestVideoBuildFailuresStoreAudioWithoutCompanion(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)
	markVideo(t, s, "none")
	ts := s.VideoPath(s.videoFileName(1)) // a leftover with no run: still removed
	if err := os.WriteFile(ts, []byte("ts"), 0o644); err != nil {
		t.Fatal(err)
	}

	up := &fakeUploader{}
	m, rx := newVideoRecoverManager(t, dir, up)
	boom := errors.New("remux: ffmpeg exited 1: libx264 exploded")
	rx.setVideoErrs(boom, boom, boom)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	if got := up.keys(); !slices.Equal(got, []string{key}) {
		t.Fatalf("uploads = %q, want only the .m4a", got)
	}
	if b := rx.builds(); len(b) != maxRemuxFailures {
		t.Fatalf("video builds = %q, want %d attempts", b, maxRemuxFailures)
	}
	ok, verr, _ := videoFields(m, s.SessionID)
	if ok || !strings.Contains(verr, "libx264 exploded") {
		t.Fatalf("videoUploaded=%v videoError=%q", ok, verr)
	}
	if n := counterValue(t, m, "recorder_video_builds_total", map[string]string{"kind": "black", "result": "failure"}); n != 3 {
		t.Fatalf("video failure metric = %v, want 3", n)
	}
	if _, err := os.Stat(ts); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("TS file should be removed with the session: %v", err)
	}
	m.mu.Lock()
	left := len(m.videoFailures)
	m.mu.Unlock()
	if left != 0 {
		t.Fatalf("video failure count not forgotten after the upload: %d keys", left)
	}
}

// TestVideoRawFallbackDropsCompanion: when even the .m4a cannot be built the
// raw ADTS goes up alone — there is nothing to build a video from.
func TestVideoRawFallbackDropsCompanion(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)
	markVideo(t, s, "none")

	up := &fakeUploader{}
	m, rx := newVideoRecoverManager(t, dir, up)
	boom := errors.New("remux: ffmpeg exited 1: Invalid data found when processing input")
	rx.setErrs(boom, boom, boom)
	m.Recover()
	m.settle()

	if got := up.keys(); !slices.Equal(got, []string{strings.TrimSuffix(key, ".m4a") + ".aac"}) {
		t.Fatalf("uploads = %q, want only the raw fallback", got)
	}
	if b := rx.builds(); len(b) != 0 {
		t.Fatalf("video builds = %q, want none", b)
	}
	if ok, verr, _ := videoFields(m, s.SessionID); ok || !strings.Contains(verr, "raw ADTS") {
		t.Fatalf("videoUploaded=%v videoError=%q", ok, verr)
	}
}

// ---------------------------------------------------------------------------
// video companion against the real binaries
// ---------------------------------------------------------------------------

// videoRun describes one ffmpeg run of a seeded Video session: picture=true
// writes an A/V MPEG-TS (testsrc2 + sine, H.264 + AAC) and appends its audio
// to the .aac exactly as the capture's two outputs would; picture=false is an
// audio-only run (no TS file).
type videoRun struct {
	picture bool
	w, h    int
	seconds float64
}

// seedVideoSession is seedFinalized with real media: an AAC capture and, per
// picture run, its TS file listed in the run.
func seedVideoSession(t *testing.T, ffmpeg, root, id, sid string, start time.Time, key string, runs ...videoRun) *Session {
	t.Helper()
	safe := schedule.SafeID(id)
	end := start.Add(time.Hour)
	s := &Session{
		ID: id, SafeID: safe, SessionID: sid, Name: "Test " + id,
		Source: "http://example.invalid/" + id + ".m3u8",
		Start:  start, End: end, SessionStart: start, SessionEnd: &end,
		State: StateFinalized, FinishReason: ReasonEnded, Codec: "aac", ResolvedCodec: "copy",
		Video: true, VideoSource: "copy", VideoMap: "0:v:0",
		dir: filepath.Join(root, "recordings", safe),
	}
	s.setKey(key)
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		t.Fatal(err)
	}
	ff := func(args ...string) {
		t.Helper()
		args = append([]string{"-nostdin", "-hide_banner", "-v", "error", "-y"}, args...)
		if out, err := exec.Command(ffmpeg, args...).CombinedOutput(); err != nil {
			t.Fatalf("ffmpeg %q: %v\n%s", args, err, out)
		}
	}
	var capture []byte
	for i, r := range runs {
		adtsPath := filepath.Join(s.dir, fmt.Sprintf(".seed-%d.aac", i))
		sine := fmt.Sprintf("sine=frequency=%d:sample_rate=48000:duration=%g", 440+110*i, r.seconds)
		run := Run{StartedAt: start, EndedAt: &end, Anchor: start.Add(time.Duration(i) * time.Minute),
			AnchorSource: "wallclock", Offset: int64(len(capture))}
		if r.picture {
			s.VideoSeq++
			name := s.videoFileName(s.VideoSeq)
			ts := s.VideoPath(name)
			ff("-f", "lavfi", "-i", fmt.Sprintf("testsrc2=s=%dx%d:r=25:d=%g", r.w, r.h, r.seconds),
				"-f", "lavfi", "-i", sine,
				"-c:v", "libx264", "-preset", "ultrafast", "-g", "25", "-pix_fmt", "yuv420p",
				"-ac", "1", "-c:a", "aac", "-b:a", "96k", "-f", "mpegts", ts)
			ff("-i", ts, "-map", "0:a:0", "-c", "copy", "-f", "adts", adtsPath)
			run.VideoFiles = []string{name}
		} else {
			ff("-f", "lavfi", "-i", sine, "-ac", "1", "-c:a", "aac", "-b:a", "96k", "-f", "adts", adtsPath)
		}
		data, err := os.ReadFile(adtsPath)
		if err != nil {
			t.Fatal(err)
		}
		_ = os.Remove(adtsPath)
		capture = append(capture, data...)
		run.Bytes = int64(len(data))
		s.Runs = append(s.Runs, run)
	}
	s.Bytes, s.Size = int64(len(capture)), int64(len(capture))
	if err := os.WriteFile(s.FilePath(), capture, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	return s
}

// newRealVideoManager runs the upload path on the real ffmpeg-backed remuxer.
func newRealVideoManager(t *testing.T, dir string, up Uploader) *Manager {
	t.Helper()
	ffmpeg, ffprobe := realFFmpeg(t)
	cfg := testConfig(dir, ffmpeg)
	cfg.FFprobePath = ffprobe
	cfg.AudioBitrate = "96k"
	m := NewManager(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), up, "test")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})
	return m
}

// settleLong waits for the real-ffmpeg uploads (slower than the fakes).
func settleLong(t *testing.T, m *Manager) {
	t.Helper()
	waitFor(t, 2*time.Minute, "uploads to settle", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(m.uploadLoops) == 0
	})
}

// probeObject writes a stored object to disk and probes it.
func probeObject(t *testing.T, m *Manager, up *fakeUploader, key string) remux.VideoInfo {
	t.Helper()
	o := up.object(key)
	if o == nil {
		t.Fatalf("no object %s", key)
	}
	p := filepath.Join(t.TempDir(), "probe"+filepath.Ext(key))
	if err := os.WriteFile(p, o.body, 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := m.remuxer().(VideoRemuxer).ProbeVideo(context.Background(), p)
	if err != nil {
		t.Fatalf("probe %s: %v", key, err)
	}
	return info
}

func assertTSGone(t *testing.T, s *Session) {
	t.Helper()
	left, _ := filepath.Glob(filepath.Join(s.dir, s.SessionID+".v*.ts"))
	if len(left) != 0 {
		t.Fatalf("TS files left after completion: %q", left)
	}
}

// TestVideoRealCopyAndTranscode: a session whose every run captured the same
// picture is stream-copied; one with an audio-only run in between is
// re-encoded at the captured size. Both keep the TS files only until the
// session is stored.
func TestVideoRealCopyAndTranscode(t *testing.T) {
	cases := []struct {
		name string
		runs []videoRun
		kind string
	}{
		{"copy", []videoRun{{true, 320, 240, 2}, {true, 320, 240, 1.5}}, "copy"},
		{"transcode", []videoRun{{true, 320, 240, 2}, {false, 0, 0, 1.5}}, "transcode"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ffmpeg, _ := realFFmpeg(t)
			dir := t.TempDir()
			start := time.Now().Add(-2 * time.Hour)
			key := start.UTC().Format("2006-01-02") + "/a.m4a"
			vkey := strings.TrimSuffix(key, ".m4a") + ".mp4"
			s := seedVideoSession(t, ffmpeg, dir, "a", "a_20260101T000000Z", start, key, tc.runs...)

			up := &fakeUploader{}
			m := newRealVideoManager(t, dir, up)
			m.Recover()
			settleLong(t, m)

			if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
				m.mu.Lock()
				t.Fatalf("state = %s (%s)", st, m.sessions[s.SessionID].LastError)
			}
			if got := up.keys(); !slices.Equal(got, []string{vkey, key}) {
				t.Fatalf("uploads = %q", got)
			}
			v := up.calls[0]
			if v.meta["video-kind"] != tc.kind || v.meta["synthetic-video"] != "false" ||
				v.meta["width"] != "320" || v.meta["height"] != "240" || v.meta["video-codec"] != "h264" {
				t.Fatalf("mp4 metadata = %v", v.meta)
			}
			info := probeObject(t, m, up, vkey)
			if !info.HasVideo || info.VideoCodec != "h264" || info.Width != 320 || info.Height != 240 || info.AudioCodec != "aac" {
				t.Fatalf("stored mp4 = %+v", info)
			}
			if d := info.Duration.Seconds(); d < 3 || d > 4.5 {
				t.Fatalf("stored mp4 lasts %.2fs, want ~3.5s", d)
			}
			if ok, verr, _ := videoFields(m, s.SessionID); !ok || verr != "" {
				t.Fatalf("videoUploaded=%v videoError=%q", ok, verr)
			}
			assertTSGone(t, s)
		})
	}
}

// TestVideoRealRemoteMerge: a later session of the recording merges into the
// stored pair. A stored picture whose manifest matches the .m4a's is reused
// (extracted and stream-copied); one that is out of sync, or missing, is
// replaced by the stored audio under black, which forces a re-encode.
func TestVideoRealRemoteMerge(t *testing.T) {
	cases := []struct {
		name      string
		tamper    func(up *fakeUploader, vkey string)
		kind      string
		downloads int // the .m4a, plus the .mp4 only when it is reused
	}{
		{"in-sync", func(*fakeUploader, string) {}, "copy", 2},
		{"out-of-sync", func(up *fakeUploader, vkey string) {
			up.mu.Lock()
			up.objects[vkey].meta["sessions"] = "deadbeef"
			up.mu.Unlock()
		}, "transcode", 1},
		{"missing", func(up *fakeUploader, vkey string) {
			up.mu.Lock()
			delete(up.objects, vkey)
			up.mu.Unlock()
		}, "transcode", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ffmpeg, _ := realFFmpeg(t)
			dir := t.TempDir()
			start := time.Now().Add(-3 * time.Hour)
			key := start.UTC().Format("2006-01-02") + "/a.m4a"
			vkey := strings.TrimSuffix(key, ".m4a") + ".mp4"
			first := seedVideoSession(t, ffmpeg, dir, "a", "a_20260101T000000Z", start, key, videoRun{true, 320, 240, 2})

			up := &fakeUploader{}
			m1 := newRealVideoManager(t, dir, up)
			m1.Recover()
			settleLong(t, m1)
			if got := up.keys(); !slices.Equal(got, []string{vkey, key}) {
				t.Fatalf("first uploads = %q", got)
			}
			tc.tamper(up, vkey)

			second := seedVideoSession(t, ffmpeg, dir, "a", "a_20260101T010000Z", start.Add(time.Hour), key,
				videoRun{true, 320, 240, 1.5})
			m2 := newRealVideoManager(t, dir, up)
			m2.Recover()
			settleLong(t, m2)

			if st, _ := m2.sessionState(second.SessionID); st != StateUploaded {
				m2.mu.Lock()
				t.Fatalf("state = %s (%s)", st, m2.sessions[second.SessionID].LastError)
			}
			calls := up.calls[2:]
			if len(calls) != 2 || calls[0].key != vkey || calls[1].key != key {
				t.Fatalf("merge uploads = %+v", calls)
			}
			v, a := calls[0], calls[1]
			wantSessions := sessionDigest(first.SessionID) + "," + sessionDigest(second.SessionID)
			if a.meta["sessions"] != wantSessions || v.meta["sessions"] != wantSessions || v.meta["parts"] != "2" {
				t.Fatalf("manifests: m4a %q, mp4 %q (parts %s)", a.meta["sessions"], v.meta["sessions"], v.meta["parts"])
			}
			if v.meta["video-kind"] != tc.kind {
				t.Fatalf("video-kind = %q, want %q", v.meta["video-kind"], tc.kind)
			}
			up.mu.Lock()
			downloads := up.downloads
			up.mu.Unlock()
			if downloads != tc.downloads {
				t.Fatalf("downloads = %d, want %d", downloads, tc.downloads)
			}
			info := probeObject(t, m2, up, vkey)
			if !info.HasVideo || info.Width != 320 || info.Height != 240 {
				t.Fatalf("merged mp4 = %+v", info)
			}
			if d := info.Duration.Seconds(); d < 3 || d > 4.5 {
				t.Fatalf("merged mp4 lasts %.2fs, want ~3.5s", d)
			}
			assertTSGone(t, second)
		})
	}
}

// TestVideoRealKeptMode: with uploads disabled a Video session also leaves
// <session>.mp4 next to its .m4a, and the TS files go once both exist.
func TestVideoRealKeptMode(t *testing.T) {
	ffmpeg, _ := realFFmpeg(t)
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedVideoSession(t, ffmpeg, dir, "a", "a_20260101T000000Z", start, key,
		videoRun{true, 320, 240, 2}, videoRun{true, 320, 240, 1})

	m := newRealVideoManager(t, dir, nil)
	m.Recover()
	settleLong(t, m)

	if st, _ := m.sessionState(s.SessionID); st != StateKept {
		t.Fatalf("state = %s", st)
	}
	_, verr, out := videoFields(m, s.SessionID)
	if out != s.OutputVideoPath() || verr != "" {
		t.Fatalf("outputVideoFile=%q videoError=%q", out, verr)
	}
	info, err := m.remuxer().(VideoRemuxer).ProbeVideo(context.Background(), out)
	if err != nil {
		t.Fatal(err)
	}
	if !info.HasVideo || info.Width != 320 || info.Height != 240 || info.AudioCodec != "aac" {
		t.Fatalf("kept mp4 = %+v", info)
	}
	if _, err := os.Stat(s.OutputPath()); err != nil {
		t.Fatalf("kept .m4a missing: %v", err)
	}
	if _, err := os.Stat(s.FilePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("capture should be gone: %v", err)
	}
	assertTSGone(t, s)
	views := m.Recordings()
	if len(views) != 1 || views[0].OutputVideoFile != out || !views[0].Video {
		t.Fatalf("view = %+v", views[0])
	}
}
