package recorder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spectado/stream-recorder/internal/config"
	"github.com/spectado/stream-recorder/internal/metrics"
	"github.com/spectado/stream-recorder/internal/schedule"
)

// fakeFFmpeg writes a shell script that behaves like ffmpeg for our purposes:
// it streams bytes to stdout until it receives SIGINT/SIGTERM.
func fakeFFmpeg(t *testing.T, dir string, fail bool) string {
	t.Helper()
	body := `#!/bin/sh
trap 'exit 0' INT TERM
while :; do
  printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
  sleep 0.05
done
`
	if fail {
		body = "#!/bin/sh\necho '[error] fake ffmpeg failure' >&2\nexit 1\n"
	}
	name := "ffmpeg-ok.sh"
	if fail {
		name = "ffmpeg-fail.sh"
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

type uploadCall struct {
	path, key string
	size      int64
	meta      map[string]string
}

// fakeUploader records calls and returns queued errors first.
type fakeUploader struct {
	mu    sync.Mutex
	calls []uploadCall
	errs  []error
	block chan struct{} // when non-nil, Upload blocks until closed or ctx done
}

func (f *fakeUploader) Upload(ctx context.Context, path, key, contentType string, meta map[string]string) (string, error) {
	f.mu.Lock()
	var err error
	if len(f.errs) > 0 {
		err, f.errs = f.errs[0], f.errs[1:]
	}
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if err != nil {
		return "", err
	}
	st, serr := os.Stat(path)
	if serr != nil {
		return "", serr
	}
	f.mu.Lock()
	f.calls = append(f.calls, uploadCall{path: path, key: key, size: st.Size(), meta: meta})
	f.mu.Unlock()
	return "etag-" + key, nil
}

func (f *fakeUploader) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeUploader) setErrs(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = errs
}

func testConfig(dir, ffmpeg string) *config.Config {
	return &config.Config{
		DataDir:                 dir,
		RetentionUploaded:       time.Hour,
		MinFreeDiskBytes:        1,
		SchedulePollInterval:    time.Minute,
		ScheduleFetchTimeout:    5 * time.Second,
		ScheduleLocation:        time.UTC,
		RecordStartEarly:        0,
		RecordStopLate:          0,
		FFmpegPath:              ffmpeg,
		FFprobePath:             ffmpeg,
		FFprobeTimeout:          5 * time.Second,
		ProbeConcurrency:        4,
		AudioCodec:              "aac",
		AudioBitrate:            "64k",
		FFmpegUserAgent:         "test",
		FFmpegRWTimeout:         5 * time.Second,
		FFmpegStallTimeout:      10 * time.Second,
		FFmpegRestartBackoffMin: 50 * time.Millisecond,
		FFmpegRestartBackoffMax: 200 * time.Millisecond,
		FFmpegStopGrace:         2 * time.Second,
		FFmpegStderrLog:         "off",
		UploadConcurrency:       2,
		UploadBackoffMax:        200 * time.Millisecond,
		UploadMinThroughput:     128 * 1024,
	}
}

func newTestManager(t *testing.T, up Uploader) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	ff := fakeFFmpeg(t, dir, false)
	m := NewManager(testConfig(dir, ff), slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), up, "test")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})
	return m, dir
}

func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}

func item(id string, start, end time.Time) schedule.Item {
	return schedule.Item{ID: id, Name: "Test " + id, Source: "http://example.invalid/" + id + ".m3u8", Start: start, End: end}
}

func sched(items ...schedule.Item) *schedule.Schedule {
	return &schedule.Schedule{Items: items, FetchedAt: time.Now(), Origin: "url"}
}

func (m *Manager) activeFor(id string) *activeRecording {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active[id]
}

func (m *Manager) sessionList() []*Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		out = append(out, s)
	}
	return out
}

func (m *Manager) sessionState(sid string) (State, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[sid]
	if s == nil {
		return "", ""
	}
	return s.State, s.FinishReason
}

// settle waits for pending stop/finalize and upload goroutines.
func (m *Manager) settle() {
	m.stopWG.Wait()
	m.uploadWG.Wait()
}

func TestStartRecordEndUpload(t *testing.T) {
	up := &fakeUploader{}
	m, dir := newTestManager(t, up)
	now := time.Now()
	it := item("a", now.Add(-time.Second), now.Add(time.Hour))
	m.SetSchedule(sched(it))

	ar := m.activeFor("a")
	if ar == nil {
		t.Fatal("recording not started")
	}
	waitFor(t, 5*time.Second, "bytes captured", func() bool { return ar.bytes.Load() > 0 })
	if !ar.running.Load() {
		t.Fatal("ffmpeg should be running")
	}
	if m.FFmpegProcessCount() != 1 {
		t.Fatalf("process count = %d", m.FFmpegProcessCount())
	}
	sid := ar.s.SessionID
	wantKey := fmt.Sprintf("a/%s/a_%s.aac", now.UTC().Format("2006-01-02"), now.UTC().Format("20060102T150405Z"))
	if ar.s.Key != wantKey {
		t.Fatalf("key = %q, want %q", ar.s.Key, wantKey)
	}

	// End reached (even though wall clock has not moved): stop + upload.
	m.Reconcile(now.Add(2 * time.Hour))
	if m.activeFor("a") != nil {
		t.Fatal("recording should have been removed from active set")
	}
	m.settle()
	st, reason := m.sessionState(sid)
	if st != StateUploaded || reason != ReasonEnded {
		t.Fatalf("state=%s reason=%s", st, reason)
	}
	if up.count() != 1 || up.calls[0].key != wantKey || up.calls[0].size == 0 {
		t.Fatalf("upload calls: %+v", up.calls)
	}
	if _, err := os.Stat(filepath.Join(dir, "recordings", "a", sid+".aac")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("audio file should be deleted after upload, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "recordings", "a", sid+".json")); err != nil {
		t.Fatalf("sidecar should remain: %v", err)
	}
	if m.FFmpegProcessCount() != 0 {
		t.Fatalf("process count after stop = %d", m.FFmpegProcessCount())
	}
	// Coverage: the window is still "open" from the point of view of a stepped-back clock.
	m.Reconcile(now.Add(time.Minute))
	if m.activeFor("a") != nil {
		t.Fatal("ended session must cover the window; no re-record on clock step-back")
	}
}

func TestRemovedItemStops(t *testing.T) {
	up := &fakeUploader{}
	m, _ := newTestManager(t, up)
	now := time.Now()
	m.SetSchedule(sched(item("a", now.Add(-time.Second), now.Add(time.Hour))))
	ar := m.activeFor("a")
	if ar == nil {
		t.Fatal("not started")
	}
	waitFor(t, 5*time.Second, "bytes", func() bool { return ar.bytes.Load() > 0 })

	m.SetSchedule(sched()) // empty, successfully fetched schedule
	m.settle()
	st, reason := m.sessionState(ar.s.SessionID)
	if st != StateUploaded || reason != ReasonRemoved {
		t.Fatalf("state=%s reason=%s", st, reason)
	}
	// Re-added while still in window: a removed session never covers -> record again.
	m.SetSchedule(sched(item("a", now.Add(-time.Second), now.Add(time.Hour))))
	ar2 := m.activeFor("a")
	if ar2 == nil || ar2.s.SessionID == ar.s.SessionID {
		t.Fatal("re-added item should start a new session")
	}
}

func TestInvalidItemKeepsRecording(t *testing.T) {
	m, _ := newTestManager(t, &fakeUploader{})
	now := time.Now()
	m.SetSchedule(sched(item("a", now.Add(-time.Second), now.Add(time.Hour))))
	ar := m.activeFor("a")
	if ar == nil {
		t.Fatal("not started")
	}
	// A later fetch where the item is present but invalid must not stop it.
	bad := sched()
	bad.Invalid = []schedule.InvalidItem{{Index: 0, ID: "a", Reason: "end must be after start"}}
	m.SetSchedule(bad)
	if m.activeFor("a") != ar {
		t.Fatal("recording was stopped although the item is still present (invalid)")
	}
	m.mu.Lock()
	note := ar.s.ScheduleNote
	m.mu.Unlock()
	if !strings.Contains(note, "invalid") {
		t.Fatalf("expected schedule note, got %q", note)
	}
	// Valid again: note cleared.
	m.SetSchedule(sched(item("a", now.Add(-time.Second), now.Add(time.Hour))))
	m.mu.Lock()
	note = ar.s.ScheduleNote
	m.mu.Unlock()
	if note != "" {
		t.Fatalf("note should be cleared, got %q", note)
	}
}

func TestEndChangesApplyToActiveRecording(t *testing.T) {
	m, _ := newTestManager(t, &fakeUploader{})
	now := time.Now()
	m.SetSchedule(sched(item("a", now.Add(-time.Second), now.Add(time.Hour))))
	ar := m.activeFor("a")
	if ar == nil {
		t.Fatal("not started")
	}
	// Extend the end; a moved start must not matter.
	ext := item("a", now.Add(30*time.Minute), now.Add(3*time.Hour))
	m.SetSchedule(sched(ext))
	m.Reconcile(now.Add(2 * time.Hour))
	if m.activeFor("a") != ar {
		t.Fatal("extended recording must continue past the old end")
	}
	// Shorten the end into the past: stops now.
	m.SetSchedule(sched(item("a", now.Add(-time.Hour), now.Add(-time.Minute))))
	m.settle()
	if m.activeFor("a") != nil {
		t.Fatal("recording should stop once the (shortened) end has passed")
	}
	st, reason := m.sessionState(ar.s.SessionID)
	if reason != ReasonEnded || st == StateRecording {
		t.Fatalf("state=%s reason=%s", st, reason)
	}
}

func TestExtensionAfterEndStartsNewPart(t *testing.T) {
	up := &fakeUploader{}
	m, _ := newTestManager(t, up)
	now := time.Now()
	it := item("a", now.Add(-time.Second), now.Add(time.Hour))
	it.Key = "shows/morning.aac"
	m.SetSchedule(sched(it))
	first := m.activeFor("a")
	if first == nil {
		t.Fatal("not started")
	}
	if first.s.Key != "shows/morning.aac" {
		t.Fatalf("explicit key not used: %q", first.s.Key)
	}
	m.Reconcile(now.Add(90 * time.Minute)) // ended
	m.settle()

	// The producer extends the show after it already ended: new session, new key.
	it.End = now.Add(3 * time.Hour)
	m.SetSchedule(sched(it))
	later := now.Add(91 * time.Minute)
	m.Reconcile(later)
	second := m.activeFor("a")
	if second == nil {
		t.Fatal("extension after end should start a new session")
	}
	if second.s.SessionID == first.s.SessionID {
		t.Fatal("new session expected")
	}
	if !strings.HasPrefix(second.s.Key, "shows/morning_") || !strings.HasSuffix(second.s.Key, ".aac") {
		t.Fatalf("second part key = %q", second.s.Key)
	}
}

func TestMaxSessionDurationRotates(t *testing.T) {
	m, _ := newTestManager(t, &fakeUploader{})
	m.cfg.MaxSessionDuration = time.Minute
	now := time.Now()
	m.SetSchedule(sched(item("a", now.Add(-time.Second), now.Add(time.Hour))))
	first := m.activeFor("a")
	if first == nil {
		t.Fatal("not started")
	}
	m.Reconcile(now.Add(2 * time.Minute))
	second := m.activeFor("a")
	if second == nil || second == first {
		t.Fatal("rotation should stop the first session and start a second one in the same tick")
	}
	m.settle()
	_, reason := m.sessionState(first.s.SessionID)
	if reason != ReasonRotated {
		t.Fatalf("first session reason = %q", reason)
	}
}

func TestDiskLowRefusesNewRecordings(t *testing.T) {
	m, _ := newTestManager(t, &fakeUploader{})
	m.SetDiskFree(func() (uint64, bool) { return 0, true })
	now := time.Now()
	m.SetSchedule(sched(item("a", now.Add(-time.Second), now.Add(time.Hour))))
	if m.activeFor("a") != nil {
		t.Fatal("must not start when disk is low")
	}
	if ok, reason := m.Ready(); ok || !strings.Contains(reason, "disk") {
		t.Fatalf("ready=%v reason=%q", ok, reason)
	}
	m.SetDiskFree(func() (uint64, bool) { return 1 << 40, true })
	m.Reconcile(time.Now())
	if m.activeFor("a") == nil {
		t.Fatal("should start once disk recovered")
	}
}

func TestResumeFromSidecarWithoutSchedule(t *testing.T) {
	up := &fakeUploader{}
	dir := t.TempDir()
	ff := fakeFFmpeg(t, dir, false)
	cfg := testConfig(dir, ff)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// A previous run left an unfinished session behind.
	now := time.Now()
	s := &Session{
		ID: "radio", SafeID: "radio", SessionID: "radio_20260101T000000Z", Source: "http://example.invalid/x",
		Start: now.Add(-time.Hour), End: now.Add(time.Hour), Key: "radio/x.aac", Codec: "aac", ResolvedCodec: "aac",
		SessionStart: now.Add(-time.Hour), State: StateRecording, Bytes: 5,
		dir: filepath.Join(dir, "recordings", "radio"),
	}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.FilePath(), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	m := NewManager(cfg, log, metrics.New("test"), up, "test")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})
	m.Recover()
	ar := m.activeFor("radio")
	if ar == nil {
		t.Fatal("unfinished session not recovered")
	}
	if !ar.recovered || ar.bytes.Load() != 5 {
		t.Fatalf("recovered=%v bytes=%d", ar.recovered, ar.bytes.Load())
	}
	// No schedule loaded yet: the sidecar is the last known state -> resume.
	m.Reconcile(time.Now())
	waitFor(t, 5*time.Second, "appended bytes", func() bool { return ar.bytes.Load() > 5 })
	data, _ := os.ReadFile(s.FilePath())
	if !strings.HasPrefix(string(data), "hello") {
		t.Fatal("resume must append, not overwrite")
	}
	// Its own end passes -> finalize and upload.
	m.Reconcile(now.Add(2 * time.Hour))
	m.settle()
	st, reason := m.sessionState(s.SessionID)
	if st != StateUploaded || reason != ReasonEnded {
		t.Fatalf("state=%s reason=%s", st, reason)
	}
	if up.count() != 1 || up.calls[0].key != "radio/x.aac" {
		t.Fatalf("calls=%+v", up.calls)
	}
}

func TestRecoverFinalizedSessionUploads(t *testing.T) {
	up := &fakeUploader{}
	dir := t.TempDir()
	ff := fakeFFmpeg(t, dir, false)
	now := time.Now()
	end := now.Add(-time.Minute)
	s := &Session{
		ID: "r", SafeID: "r", SessionID: "r_20260101T000000Z", Source: "http://example.invalid/x",
		Start: now.Add(-time.Hour), End: end, Key: "r/x.aac", SessionStart: now.Add(-time.Hour), SessionEnd: &end,
		State: StateUploading, FinishReason: ReasonEnded, dir: filepath.Join(dir, "recordings", "r"),
	}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.FilePath(), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := NewManager(testConfig(dir, ff), slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), up, "test")
	m.Recover()
	m.settle()
	st, _ := m.sessionState(s.SessionID)
	if st != StateUploaded || up.count() != 1 {
		t.Fatalf("state=%s uploads=%d", st, up.count())
	}
}

func TestEmptyRecordingIsFailedNotUploaded(t *testing.T) {
	up := &fakeUploader{}
	dir := t.TempDir()
	ff := fakeFFmpeg(t, dir, false)
	now := time.Now()
	end := now.Add(-time.Minute)
	s := &Session{
		ID: "e", SafeID: "e", SessionID: "e_20260101T000000Z", Source: "http://example.invalid/x",
		Start: now.Add(-time.Hour), End: end, Key: "e/x.aac", SessionStart: now.Add(-time.Hour), SessionEnd: &end,
		State: StateFinalized, FinishReason: ReasonEnded, dir: filepath.Join(dir, "recordings", "e"),
	}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.FilePath(), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	m := NewManager(testConfig(dir, ff), slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), up, "test")
	m.Recover()
	m.settle()
	st, _ := m.sessionState(s.SessionID)
	if st != StateFailed || up.count() != 0 {
		t.Fatalf("state=%s uploads=%d", st, up.count())
	}
}

func TestUploadRetryPermanentAndObjectExists(t *testing.T) {
	old := blockedRetryInterval
	blockedRetryInterval = 50 * time.Millisecond
	t.Cleanup(func() { blockedRetryInterval = old })

	up := &fakeUploader{}
	up.setErrs(&PermanentError{Err: errors.New("AccessDenied")}, errors.New("temporary"), ErrObjectExists)
	m, _ := newTestManager(t, up)
	now := time.Now()
	m.SetSchedule(sched(item("a", now.Add(-time.Second), now.Add(time.Hour))))
	ar := m.activeFor("a")
	if ar == nil {
		t.Fatal("not started")
	}
	waitFor(t, 5*time.Second, "bytes", func() bool { return ar.bytes.Load() > 0 })
	origKey := ar.s.Key
	m.Reconcile(now.Add(2 * time.Hour))
	m.settle()

	st, _ := m.sessionState(ar.s.SessionID)
	if st != StateUploaded {
		t.Fatalf("state=%s", st)
	}
	m.mu.Lock()
	s := m.sessions[ar.s.SessionID]
	key, attempts, blocked := s.Key, s.UploadAttempts, s.UploadBlocked
	m.mu.Unlock()
	if blocked {
		t.Fatal("blocked flag must be cleared after success")
	}
	if attempts < 4 {
		t.Fatalf("attempts = %d", attempts)
	}
	if key == origKey || !strings.HasSuffix(key, ".aac") || !strings.Contains(key, "-") {
		t.Fatalf("key after ErrObjectExists = %q (orig %q)", key, origKey)
	}
	if up.count() != 1 || up.calls[0].key != key {
		t.Fatalf("calls=%+v", up.calls)
	}
}

func TestShutdownSuspendsRecordings(t *testing.T) {
	up := &fakeUploader{}
	dir := t.TempDir()
	ff := fakeFFmpeg(t, dir, false)
	m := NewManager(testConfig(dir, ff), slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), up, "test")
	now := time.Now()
	m.SetSchedule(sched(item("a", now.Add(-time.Second), now.Add(time.Hour))))
	ar := m.activeFor("a")
	if ar == nil {
		t.Fatal("not started")
	}
	waitFor(t, 5*time.Second, "bytes", func() bool { return ar.bytes.Load() > 0 })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start := time.Now()
	m.Shutdown(ctx)
	if time.Since(start) > 5*time.Second {
		t.Fatalf("shutdown took %s", time.Since(start))
	}
	if m.FFmpegProcessCount() != 0 {
		t.Fatal("ffmpeg still running after shutdown")
	}
	m.mu.Lock()
	st, suspended := ar.s.State, ar.s.SuspendedAt
	m.mu.Unlock()
	if st != StateRecording || suspended == nil {
		t.Fatalf("state=%s suspendedAt=%v (should stay resumable)", st, suspended)
	}
	if up.count() != 0 {
		t.Fatal("no upload on shutdown")
	}
	if ok, reason := m.Ready(); ok || reason != "shutting down" {
		t.Fatalf("ready=%v reason=%q", ok, reason)
	}
	// The sidecar on disk says "recording" so the next start resumes it.
	loaded, err := loadSession(ar.s.SidecarPath())
	if err != nil || loaded.State != StateRecording {
		t.Fatalf("sidecar: %+v err=%v", loaded, err)
	}
}

func TestFFmpegFailureRestartsWithBackoff(t *testing.T) {
	up := &fakeUploader{}
	dir := t.TempDir()
	ff := fakeFFmpeg(t, dir, true)
	m := NewManager(testConfig(dir, ff), slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), up, "test")
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
	waitFor(t, 5*time.Second, "restarts", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return ar.s.Restarts >= 3
	})
	m.mu.Lock()
	lastErr := ar.s.LastError
	m.mu.Unlock()
	if !strings.Contains(lastErr, "fake ffmpeg failure") {
		t.Fatalf("lastError = %q", lastErr)
	}
	if m.FFmpegProcessCount() != 0 {
		t.Fatal("no process should be counted while failing")
	}
	// Still active: a failing stream never gives up while the window is open.
	if m.activeFor("a") != ar {
		t.Fatal("recording must stay active")
	}
	views := m.Recordings()
	if len(views) != 1 || views[0].State != StateRecording || views[0].Restarts < 3 {
		t.Fatalf("views=%+v", views)
	}
}

func TestPollerKeepsLastKnownScheduleOnFailure(t *testing.T) {
	m, dir := newTestManager(t, &fakeUploader{})
	now := time.Now()
	body := fmt.Sprintf(`[{"id":"a","source":"http://example.invalid/a","start":%q,"end":%q}]`,
		now.Add(-time.Second).Format(time.RFC3339), now.Add(time.Hour).Format(time.RFC3339))

	var mode string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		cur := mode
		mu.Unlock()
		switch cur {
		case "fail":
			w.WriteHeader(http.StatusInternalServerError)
		case "garbage":
			_, _ = io.WriteString(w, `{"items": [{"id": "a"}]}`) // all invalid
		case "notmodified":
			if r.Header.Get("If-None-Match") == `"v1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			fallthrough
		default:
			w.Header().Set("ETag", `"v1"`)
			_, _ = io.WriteString(w, body)
		}
	}))
	defer srv.Close()
	setMode := func(s string) { mu.Lock(); mode = s; mu.Unlock() }

	f := schedule.NewFetcher(srv.URL, 2*time.Second, "", "test", dir, time.UTC)
	ctx := context.Background()

	setMode("fail")
	m.pollOnce(ctx, f, true)
	if ok, _ := m.Ready(); ok {
		t.Fatal("no schedule should be loaded after a failed first fetch (no cache)")
	}

	setMode("ok")
	m.pollOnce(ctx, f, false)
	if ok, _ := m.Ready(); !ok {
		t.Fatal("schedule should be loaded")
	}
	if m.activeFor("a") == nil {
		t.Fatal("reconcile after load should start the recording")
	}

	setMode("fail")
	m.pollOnce(ctx, f, false)
	m.mu.Lock()
	failures, items := m.info.ConsecutiveFailures, m.info.ItemCount
	m.mu.Unlock()
	if failures != 1 || items != 1 || m.activeFor("a") == nil {
		t.Fatalf("failures=%d items=%d active=%v", failures, items, m.activeFor("a") != nil)
	}

	setMode("garbage") // every item invalid -> treated as a failed fetch, nothing stops
	m.pollOnce(ctx, f, false)
	m.mu.Lock()
	failures = m.info.ConsecutiveFailures
	m.mu.Unlock()
	if failures != 2 || m.activeFor("a") == nil {
		t.Fatalf("all-invalid document must not replace the schedule (failures=%d)", failures)
	}

	setMode("notmodified")
	m.pollOnce(ctx, f, false)
	m.mu.Lock()
	failures, lastErr := m.info.ConsecutiveFailures, m.info.LastError
	m.mu.Unlock()
	if failures != 0 || lastErr != "" {
		t.Fatalf("304 should reset failure state: failures=%d err=%q", failures, lastErr)
	}

	// Cache was written by the successful fetch: a fresh manager can start from it.
	m2 := NewManager(m.cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test2"), nil, "test")
	cached, err := f.LoadCache()
	if err != nil {
		t.Fatal(err)
	}
	if cached.Origin != "cache" || len(cached.Items) != 1 {
		t.Fatalf("cache: %+v", cached)
	}
	m2.mu.Lock()
	m2.applyScheduleLocked(cached)
	m2.mu.Unlock()
	if ok, _ := m2.Ready(); !ok {
		t.Fatal("manager with cached schedule should be ready")
	}
}

func TestRecordingsViewRedactsSecrets(t *testing.T) {
	m, _ := newTestManager(t, &fakeUploader{})
	now := time.Now()
	it := item("a", now.Add(-time.Second), now.Add(time.Hour))
	it.Source = "https://user:pass@cdn.example.com/live.m3u8?token=SECRET"
	it.Headers = map[string]string{"Authorization": "Bearer xyz"}
	m.SetSchedule(sched(it))
	views := m.Recordings()
	if len(views) != 1 {
		t.Fatalf("views=%d", len(views))
	}
	if strings.Contains(views[0].Source, "SECRET") || strings.Contains(views[0].Source, "pass") {
		t.Fatalf("source not redacted: %s", views[0].Source)
	}
	sv := m.ScheduleView().(map[string]any)
	items := sv["items"].([]schedule.Item)
	if items[0].Headers["Authorization"] != "***" || strings.Contains(items[0].Source, "SECRET") {
		t.Fatalf("schedule view leaks secrets: %+v", items[0])
	}
}
