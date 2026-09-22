package recorder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/spectado/stream-recorder/internal/adts"
	"github.com/spectado/stream-recorder/internal/config"
	"github.com/spectado/stream-recorder/internal/metrics"
	"github.com/spectado/stream-recorder/internal/remux"
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

// flakyFFmpeg writes a script that exits 0 with a trailing "Error during
// demuxing" line (a transient live-reload failure -> reason "demux-error") its
// first `fails` invocations, then streams bytes like the healthy fake. A
// counter file in dir makes the runs deterministic and sequential.
func flakyFFmpeg(t *testing.T, dir string, fails int) string {
	t.Helper()
	counter := filepath.Join(dir, "flaky-runs.count")
	frames := adtsFramesFile(t, dir)
	body := fmt.Sprintf(`#!/bin/sh
trap 'exit 0' INT TERM
c=%q
n=$(cat "$c" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > "$c"
if [ "$n" -le %d ]; then
  echo '[error] Error during demuxing: boom' >&2
  exit 0
fi
while :; do
  cat %q || exit 0
  sleep 0.05
done
`, counter, fails, frames)
	p := filepath.Join(dir, "ffmpeg-flaky.sh")
	if err := os.WriteFile(p, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

type uploadCall struct {
	path, key, contentType string
	size                   int64
	meta                   map[string]string
	replace                string // ETag the upload replaced ("" for a new object)
}

// fakeObject is one stored object: bytes, user metadata and an ETag that
// changes on every write (so conditional replaces can be exercised).
type fakeObject struct {
	body  []byte
	meta  map[string]string
	etag  string
	ctype string
}

// fakeUploader is an in-memory object store with the semantics the recorder
// relies on: conditional create, conditional replace, HEAD with metadata and
// download.
type fakeUploader struct {
	mu        sync.Mutex
	calls     []uploadCall
	errs      []error
	block     chan struct{} // when non-nil, Upload blocks until closed or ctx done
	objects   map[string]*fakeObject
	heads     int
	downloads int
	noCond    bool // endpoint does NOT enforce conditional writes
	condErr   error
	etagSeq   int
}

func copyMeta(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (f *fakeUploader) Upload(ctx context.Context, path, key, contentType string, meta map[string]string, replaceETag string) (string, error) {
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
	body, rerr := os.ReadFile(path)
	if rerr != nil {
		return "", rerr
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.objects == nil {
		f.objects = map[string]*fakeObject{}
	}
	cur := f.objects[key]
	if replaceETag == "" {
		if cur != nil && !bytes.Equal(cur.body, body) {
			return "", ErrObjectExists
		}
	} else if cur == nil || cur.etag != replaceETag {
		return "", ErrObjectChanged
	}
	f.etagSeq++
	etag := fmt.Sprintf("etag-%d", f.etagSeq)
	f.objects[key] = &fakeObject{body: body, meta: copyMeta(meta), etag: etag, ctype: contentType}
	f.calls = append(f.calls, uploadCall{path: path, key: key, contentType: contentType,
		size: int64(len(body)), meta: copyMeta(meta), replace: replaceETag})
	return etag, nil
}

func (f *fakeUploader) Head(ctx context.Context, key string) (ObjectInfo, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heads++
	o := f.objects[key]
	if o == nil {
		return ObjectInfo{}, false, nil
	}
	return ObjectInfo{Size: int64(len(o.body)), ETag: o.etag, Metadata: copyMeta(o.meta)}, true, nil
}

func (f *fakeUploader) Download(ctx context.Context, key, ifMatchETag, path string) error {
	f.mu.Lock()
	f.downloads++
	o := f.objects[key]
	f.mu.Unlock()
	if o == nil {
		return fmt.Errorf("no such object %q", key)
	}
	if ifMatchETag != "" && ifMatchETag != o.etag {
		return ErrObjectChanged
	}
	return os.WriteFile(path, o.body, 0o644)
}

func (f *fakeUploader) ConditionalWrites(ctx context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.noCond, f.condErr
}

// seed stores an object as if a previous run had uploaded it.
func (f *fakeUploader) seed(key string, body []byte, meta map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.objects == nil {
		f.objects = map[string]*fakeObject{}
	}
	f.etagSeq++
	f.objects[key] = &fakeObject{body: append([]byte(nil), body...), meta: copyMeta(meta),
		etag: fmt.Sprintf("seed-%d", f.etagSeq), ctype: "audio/mp4"}
}

func (f *fakeUploader) object(key string) *fakeObject {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.objects[key]
}

// headCount is the locked read of the HEAD counter: tests that inspect a loop
// while it is still running cannot touch the field directly.
func (f *fakeUploader) headCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.heads
}

func (f *fakeUploader) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeUploader) lastCall() uploadCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return uploadCall{}
	}
	return f.calls[len(f.calls)-1]
}

func (f *fakeUploader) setErrs(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = errs
}

// fakeRemuxer stands in for ffmpeg: "remuxing" is byte concatenation of the
// ADTS parts (which is exactly what the real remux does to the audio, minus
// the container), so the produced file scans back to the summed frames and
// duration and an Extract of it is the identity. Format tags are remembered per
// file content, which survives the round trip through the fake store.
type fakeRemuxer struct {
	mu         sync.Mutex
	builds     int
	transcodes int
	extracts   int
	errs       []error
	metas      []remux.Metadata
	expects    []remux.Expect
	tags       map[string]map[string]string
}

func newFakeRemuxer() *fakeRemuxer { return &fakeRemuxer{tags: map[string]map[string]string{}} }

func (f *fakeRemuxer) setErrs(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = errs
}

func (f *fakeRemuxer) lastMeta() (remux.Metadata, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.metas) == 0 {
		return remux.Metadata{}, false
	}
	return f.metas[len(f.metas)-1], true
}

func (f *fakeRemuxer) nextErr() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.errs) == 0 {
		return nil
	}
	var err error
	err, f.errs = f.errs[0], f.errs[1:]
	return err
}

func (f *fakeRemuxer) Build(ctx context.Context, parts []string, out string, expect remux.Expect, meta remux.Metadata) (remux.Result, error) {
	f.mu.Lock()
	f.builds++
	f.mu.Unlock()
	if err := f.nextErr(); err != nil {
		return remux.Result{}, err
	}
	if _, err := remux.Concat(parts, out); err != nil {
		return remux.Result{}, err
	}
	res, err := f.Probe(ctx, out)
	if err != nil {
		return remux.Result{}, err
	}
	// The real Build refuses to report success when the sample table does not
	// match the source; a wrong expectation must fail the test, not pass.
	if expect.Frames > 0 && res.Frames != expect.Frames {
		return remux.Result{}, fmt.Errorf("fake remux: %d frames produced, %d expected", res.Frames, expect.Frames)
	}
	f.remember(out, meta, expect)
	return res, nil
}

func (f *fakeRemuxer) BuildTranscode(ctx context.Context, chunks []remux.Chunk, out string, expectedDuration time.Duration, bitrate string, meta remux.Metadata) (remux.Result, error) {
	f.mu.Lock()
	f.transcodes++
	f.mu.Unlock()
	if err := f.nextErr(); err != nil {
		return remux.Result{}, err
	}
	paths := make([]string, len(chunks))
	for i, c := range chunks {
		paths[i] = c.Path
	}
	if _, err := remux.Concat(paths, out); err != nil {
		return remux.Result{}, err
	}
	res, err := f.Probe(ctx, out)
	if err != nil {
		return remux.Result{}, err
	}
	f.remember(out, meta, remux.Expect{Duration: expectedDuration})
	return res, nil
}

func (f *fakeRemuxer) Extract(ctx context.Context, in, out string) error {
	f.mu.Lock()
	f.extracts++
	f.mu.Unlock()
	data, err := os.ReadFile(in)
	if err != nil {
		return err
	}
	return os.WriteFile(out, data, 0o644)
}

func (f *fakeRemuxer) Probe(ctx context.Context, path string) (remux.Result, error) {
	info, err := adts.Scan(path)
	if err != nil {
		return remux.Result{}, err
	}
	st, err := os.Stat(path)
	if err != nil {
		return remux.Result{}, err
	}
	res := remux.Result{
		Path: path, Size: st.Size(), Duration: info.Duration, Frames: info.Frames,
		Format: "mov,mp4,m4a,3gp,3g2,mj2", Codec: "aac", Profile: info.Params.ProfileName(),
		SampleRate: info.Params.SampleRate, Channels: info.Params.Channels(),
	}
	f.mu.Lock()
	res.Tags = f.tags[fileDigest(path)]
	f.mu.Unlock()
	return res, nil
}

// remember keys the format tags by the file's content so the same bytes, read
// back from the store, probe with the same tags.
func (f *fakeRemuxer) remember(out string, meta remux.Metadata, expect remux.Expect) {
	tags := map[string]string{}
	if meta.Description != "" {
		tags["description"] = meta.Description
	}
	if meta.Title != "" {
		tags["title"] = meta.Title
	}
	if meta.Comment != "" {
		tags["comment"] = meta.Comment
	}
	if meta.Date != "" {
		tags["date"] = meta.Date
	}
	if !meta.CreationTime.IsZero() {
		tags["creation_time"] = meta.CreationTime.UTC().Format(time.RFC3339)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metas = append(f.metas, meta)
	f.expects = append(f.expects, expect)
	f.tags[fileDigest(out)] = tags
}

func fileDigest(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "missing:" + path
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// counterValue reads one Prometheus counter (optionally one label set) out of
// the manager's registry.
func counterValue(t *testing.T, m *Manager, name string, labels map[string]string) float64 {
	t.Helper()
	fams, err := m.met.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var total float64
	for _, fam := range fams {
		if fam.GetName() != name {
			continue
		}
		for _, metric := range fam.GetMetric() {
			match := true
			for k, v := range labels {
				found := false
				for _, l := range metric.GetLabel() {
					if l.GetName() == k && l.GetValue() == v {
						found = true
					}
				}
				if !found {
					match = false
				}
			}
			if match {
				total += metric.GetCounter().GetValue()
			}
		}
	}
	return total
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

// newTestManager builds a manager whose fake ffmpeg streams real ADTS frames
// and whose remuxer needs no ffmpeg: the upload path measures what it captured,
// so a capture of junk bytes would (correctly) be refused as "no ADTS frames".
func newTestManager(t *testing.T, up Uploader) (*Manager, string) {
	t.Helper()
	m, dir, _ := newTestManagerRx(t, up)
	return m, dir
}

func newTestManagerRx(t *testing.T, up Uploader) (*Manager, string, *fakeRemuxer) {
	t.Helper()
	dir := t.TempDir()
	ff := fakeFFmpegADTS(t, dir)
	m := NewManager(testConfig(dir, ff), slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), up, "test")
	m.SetRemuxer(newFakeRemuxer())
	rx := newFakeRemuxer()
	m.SetRemuxer(rx)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})
	return m, dir, rx
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
	// One object per recording, named after the scheduled start date and the id.
	wantKey := it.Start.UTC().Format("2006-01-02") + "/a.m4a"
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
	call := up.calls[0]
	if call.contentType != "audio/mp4" {
		t.Fatalf("content type = %q, want audio/mp4", call.contentType)
	}
	if call.replace != "" {
		t.Fatalf("a first upload must not replace anything, replace=%q", call.replace)
	}
	// The manifest the next session of this recording reads back.
	if call.meta["recording-id"] != "a" || call.meta["parts"] != "1" ||
		call.meta["sessions"] != sessionDigest(sid) || call.meta["last-session-start"] == "" {
		t.Fatalf("object manifest = %v", call.meta)
	}
	if got := call.meta["duration-seconds"]; got == "" || got == "0.000" {
		t.Fatalf("duration missing from object metadata: %v", call.meta)
	}
	if call.meta["frames"] == "" || call.meta["codec"] != "aac" {
		t.Fatalf("probe metadata missing: %v", call.meta)
	}
	// A clean single run never restarted, so the exit-reasons key is omitted.
	if got, ok := call.meta["ffmpeg-exit-reasons"]; ok {
		t.Fatalf("ffmpeg-exit-reasons should be absent for a clean run, got %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "recordings", "a", sid+".aac")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("capture should be deleted after upload, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "recordings", "a", sid+".json")); err != nil {
		t.Fatalf("sidecar should remain: %v", err)
	}
	// No leftovers of the remux.
	entries, _ := os.ReadDir(filepath.Join(dir, "recordings", "a"))
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Fatalf("temporary file left behind: %s", e.Name())
		}
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
	it.Key = "/shows/morning/"
	m.SetSchedule(sched(it))
	first := m.activeFor("a")
	if first == nil {
		t.Fatal("not started")
	}
	// An explicit key now names the OBJECT, not a folder.
	if first.s.Key != "shows/morning.m4a" {
		t.Fatalf("explicit key must name the object: %q", first.s.Key)
	}
	waitFor(t, 5*time.Second, "bytes", func() bool { return first.bytes.Load() > 0 })
	m.Reconcile(now.Add(90 * time.Minute)) // ended
	m.settle()
	if up.count() != 1 {
		t.Fatalf("uploads after the first part = %d", up.count())
	}

	// The producer extends the show after it already ended: a new session, the
	// same object — the second part is merged into what is already stored.
	it.End = now.Add(3 * time.Hour)
	m.SetSchedule(sched(it))
	m.Reconcile(now.Add(91 * time.Minute))
	second := m.activeFor("a")
	if second == nil {
		t.Fatal("extension after end should start a new session")
	}
	if second.s.SessionID == first.s.SessionID {
		t.Fatal("new session expected")
	}
	if second.s.Key != first.s.Key {
		t.Fatalf("second part key = %q, want the pinned %q", second.s.Key, first.s.Key)
	}
	waitFor(t, 5*time.Second, "bytes", func() bool { return second.bytes.Load() > 0 })
	m.Reconcile(now.Add(4 * time.Hour))
	m.settle()

	if up.count() != 2 {
		t.Fatalf("uploads = %d, want 2 (one per part, the second replacing)", up.count())
	}
	merge := up.calls[1]
	if merge.key != "shows/morning.m4a" || merge.replace == "" {
		t.Fatalf("second upload must replace the stored object: %+v", merge)
	}
	if merge.meta["parts"] != "2" {
		t.Fatalf("parts = %q, want 2 (meta=%v)", merge.meta["parts"], merge.meta)
	}
	wantSessions := sessionDigest(first.s.SessionID) + "," + sessionDigest(second.s.SessionID)
	if merge.meta["sessions"] != wantSessions {
		t.Fatalf("sessions = %q, want %q", merge.meta["sessions"], wantSessions)
	}
	if up.downloads != 1 || up.heads < 2 {
		t.Fatalf("merging must HEAD and download the stored object: heads=%d downloads=%d", up.heads, up.downloads)
	}
	// The merged object really holds both captures.
	obj := up.object("shows/morning.m4a")
	if obj == nil || int64(len(obj.body)) <= up.calls[0].size {
		t.Fatalf("merged object did not grow: %d <= %d", len(obj.body), up.calls[0].size)
	}
	if st, _ := m.sessionState(first.s.SessionID); st != StateUploaded {
		t.Fatalf("first session state = %s", st)
	}
	if st, _ := m.sessionState(second.s.SessionID); st != StateUploaded {
		t.Fatalf("second session state = %s", st)
	}
}

// TestForeignObjectIsNeverOverwritten covers HEAD case (a): an object under our
// key that this recorder did not write is left untouched and the recording goes
// to a second key, loudly.
func TestForeignObjectIsNeverOverwritten(t *testing.T) {
	up := &fakeUploader{}
	m, _ := newTestManager(t, up)
	now := time.Now()
	it := item("a", now.Add(-time.Second), now.Add(time.Hour))
	day := it.Start.UTC().Format("2006-01-02")
	up.seed(day+"/a.m4a", []byte("someone else's audio"), map[string]string{"foo": "bar"})

	m.SetSchedule(sched(it))
	ar := m.activeFor("a")
	if ar == nil {
		t.Fatal("not started")
	}
	waitFor(t, 5*time.Second, "bytes", func() bool { return ar.bytes.Load() > 0 })
	m.Reconcile(now.Add(2 * time.Hour))
	m.settle()

	if st, _ := m.sessionState(ar.s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	if got := string(up.object(day + "/a.m4a").body); got != "someone else's audio" {
		t.Fatalf("foreign object was modified: %q", got)
	}
	want := day + "/a-2.m4a"
	if up.count() != 1 || up.calls[0].key != want {
		t.Fatalf("calls = %+v, want a single upload to %s", up.calls, want)
	}
	if n := counterValue(t, m, "recorder_object_key_renames_total", map[string]string{"reason": "conflict"}); n != 1 {
		t.Fatalf("rename metric = %v, want 1", n)
	}
}

func TestObjectKey(t *testing.T) {
	start := time.Date(2026, 9, 22, 23, 30, 0, 0, time.UTC)
	s := &Session{ID: "match-ro-jpOkle8Mp0", SafeID: "match-ro-jpOkle8Mp0", Start: start}
	if got := objectKey("", time.UTC, s); got != "2026-09-22/match-ro-jpOkle8Mp0.m4a" {
		t.Errorf("objectKey = %q", got)
	}
	if got := objectKey("recordings/", time.UTC, s); got != "recordings/2026-09-22/match-ro-jpOkle8Mp0.m4a" {
		t.Errorf("prefixed objectKey = %q", got)
	}
	// KEY_DATE_TZ decides which day a late-evening show belongs to.
	prague, err := time.LoadLocation("Europe/Prague")
	if err != nil {
		t.Fatal(err)
	}
	if got := objectKey("", prague, s); got != "2026-09-23/match-ro-jpOkle8Mp0.m4a" {
		t.Errorf("objectKey in Europe/Prague = %q", got)
	}

	for in, want := range map[string]string{
		"/shows/morning/":   "shows/morning.m4a",
		"shows/morning":     "shows/morning.m4a",
		"shows/morning.m4a": "shows/morning.m4a",
		"shows/morning.M4A": "shows/morning.M4A",
		"  /a/b/c.m4a  ":    "a/b/c.m4a",
	} {
		if got := explicitObjectKey("", in); got != want {
			t.Errorf("explicitObjectKey(%q) = %q, want %q", in, got, want)
		}
	}
	if got := explicitObjectKey("p/", "shows/x"); got != "p/shows/x.m4a" {
		t.Errorf("prefixed explicit key = %q", got)
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
	// Only the finalize goroutine is waited for: the first part's upload stays
	// deferred while the second part records (see TestRotationDefersAndMergesLocally).
	m.stopWG.Wait()
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
	ff := fakeFFmpegADTS(t, dir)
	cfg := testConfig(dir, ff)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// A previous run left an unfinished session behind.
	now := time.Now()
	s := &Session{
		ID: "radio", SafeID: "radio", SessionID: "radio_20260101T000000Z", Source: "http://example.invalid/x",
		Start: now.Add(-time.Hour), End: now.Add(time.Hour), Key: "radio/x.aac", Codec: "aac", ResolvedCodec: "aac",
		SessionStart: now.Add(-time.Hour), State: StateRecording, Bytes: int64(len(adtsFrame(100))),
		Restarts: 2, RecordLastError: "ffmpeg exited after 1s: exit status 1 | boom",
		Exits: []RunExit{{Reason: "demux-error"}, {Reason: "exit-error"}},
		dir:   filepath.Join(dir, "recordings", "radio"),
	}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	head := adtsFrame(100)
	if err := os.WriteFile(s.FilePath(), head, 0o644); err != nil {
		t.Fatal(err)
	}

	m := NewManager(cfg, log, metrics.New("test"), up, "test")
	m.SetRemuxer(newFakeRemuxer())
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
	if !ar.recovered || ar.bytes.Load() != int64(len(head)) {
		t.Fatalf("recovered=%v bytes=%d", ar.recovered, ar.bytes.Load())
	}
	// A 1.0.x sidecar (key ends in .aac, one object per session) is migrated to
	// the single-object layout before anything else happens to it.
	wantKey := s.Start.UTC().Format("2006-01-02") + "/radio.m4a"
	m.mu.Lock()
	gotKey := ar.s.Key
	m.mu.Unlock()
	if gotKey != wantKey {
		t.Fatalf("migrated key = %q, want %q", gotKey, wantKey)
	}
	// Recover must reconstruct an active session with its pre-existing exit
	// history intact (the sidecar is the record of why it flapped so far).
	if len(ar.s.Exits) != 2 || ar.s.Exits[0].Reason != "demux-error" || ar.s.RecordLastError == "" {
		t.Fatalf("recover lost exit history: exits=%+v recordLastError=%q", ar.s.Exits, ar.s.RecordLastError)
	}
	// No schedule loaded yet: the sidecar is the last known state -> resume.
	m.Reconcile(time.Now())
	waitFor(t, 5*time.Second, "appended bytes", func() bool { return ar.bytes.Load() > int64(len(head)) })
	data, _ := os.ReadFile(s.FilePath())
	if !bytes.HasPrefix(data, head) {
		t.Fatal("resume must append, not overwrite")
	}
	// Its own end passes -> finalize and upload.
	m.Reconcile(now.Add(2 * time.Hour))
	m.settle()
	st, reason := m.sessionState(s.SessionID)
	if st != StateUploaded || reason != ReasonEnded {
		t.Fatalf("state=%s reason=%s", st, reason)
	}
	if up.count() != 1 || up.calls[0].key != wantKey {
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
	if err := os.WriteFile(s.FilePath(), adtsStream(20), 0o644); err != nil {
		t.Fatal(err)
	}
	m := NewManager(testConfig(dir, ff), slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), up, "test")
	m.SetRemuxer(newFakeRemuxer())
	m.Recover()
	m.settle()
	st, _ := m.sessionState(s.SessionID)
	if st != StateUploaded || up.count() != 1 {
		t.Fatalf("state=%s uploads=%d", st, up.count())
	}
	// The 1.0.x key was recomputed into the single-object layout.
	want := s.Start.UTC().Format("2006-01-02") + "/r.m4a"
	if up.calls[0].key != want {
		t.Fatalf("key = %q, want %q", up.calls[0].key, want)
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
	m.SetRemuxer(newFakeRemuxer())
	m.Recover()
	m.settle()
	st, _ := m.sessionState(s.SessionID)
	if st != StateFailed || up.count() != 0 {
		t.Fatalf("state=%s uploads=%d", st, up.count())
	}
	m.mu.Lock()
	key := m.sessions[s.SessionID].Key
	m.mu.Unlock()
	if want := s.Start.UTC().Format("2006-01-02") + "/e.m4a"; key != want {
		t.Fatalf("key = %q, want the recomputed %q", key, want)
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
	if attempts < 3 {
		t.Fatalf("attempts = %d", attempts)
	}
	// ErrObjectExists no longer renames blindly: the next attempt HEADs the key
	// and finds nothing there, so the recording keeps its own object.
	if key != origKey || !strings.HasSuffix(key, ".m4a") {
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
	m.SetRemuxer(newFakeRemuxer())
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
	m.SetRemuxer(newFakeRemuxer())
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
	recErr := ar.s.RecordLastError
	exits := append([]RunExit(nil), ar.s.Exits...)
	m.mu.Unlock()
	if !strings.Contains(lastErr, "fake ffmpeg failure") {
		t.Fatalf("lastError = %q", lastErr)
	}
	// supervise must also populate the exit history and the upload-surviving
	// RecordLastError (the whole point of A2), not just LastError.
	if !strings.Contains(recErr, "fake ffmpeg failure") {
		t.Fatalf("recordLastError = %q", recErr)
	}
	if len(exits) == 0 {
		t.Fatal("supervise recorded no exits")
	}
	last := exits[len(exits)-1]
	if last.Reason != "exit-error" {
		t.Fatalf("last exit reason = %q, want exit-error (exit 1)", last.Reason)
	}
	if !strings.Contains(last.ErrorLine, "fake ffmpeg failure") {
		t.Fatalf("last exit errorLine = %q", last.ErrorLine)
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
	// The view exposes the classified exits for the active recording too.
	if len(views[0].Exits) == 0 || views[0].Exits[len(views[0].Exits)-1].Reason != "exit-error" {
		t.Fatalf("view exits = %+v", views[0].Exits)
	}
}

// TestRestartedRecordingUploadsExitReasons covers the A2 upload wiring: a
// recording that restarted before it settled carries its classified exit
// history into the S3 object metadata and the API view.
func TestRestartedRecordingUploadsExitReasons(t *testing.T) {
	up := &fakeUploader{}
	dir := t.TempDir()
	ff := flakyFFmpeg(t, dir, 2) // two demux-error exits, then it streams
	m := NewManager(testConfig(dir, ff), slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), up, "test")
	m.SetRemuxer(newFakeRemuxer())
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
	// Bytes only arrive on the third run, i.e. after both demux-error exits.
	waitFor(t, 5*time.Second, "bytes after restarts", func() bool { return ar.bytes.Load() > 0 })
	sid := ar.s.SessionID

	m.Reconcile(now.Add(2 * time.Hour))
	m.settle()
	if st, _ := m.sessionState(sid); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	if up.count() != 1 {
		t.Fatalf("upload calls: %+v", up.calls)
	}
	if got := up.calls[0].meta["ffmpeg-exit-reasons"]; got != "demux-error=2" {
		t.Fatalf("ffmpeg-exit-reasons = %q, want demux-error=2 (meta=%v)", got, up.calls[0].meta)
	}
	// exits[] is copied into the view for finished/uploaded sessions too.
	views := m.Recordings()
	if len(views) != 1 || len(views[0].Exits) != 2 || views[0].Exits[0].Reason != "demux-error" {
		t.Fatalf("view exits after upload = %+v", views)
	}
}

// TestSpawnFailureClassifiedAsExitError covers the classification of a run that
// never started (fork/exec failure): it is an exit-error, and the stall flag
// must never leak in from a previous run and mislabel it "stall".
func TestSpawnFailureClassifiedAsExitError(t *testing.T) {
	up := &fakeUploader{}
	dir := t.TempDir()
	cfg := testConfig(dir, filepath.Join(dir, "no-such-ffmpeg"))
	m := NewManager(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New("test"), up, "test")
	m.SetRemuxer(newFakeRemuxer())
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
	waitFor(t, 5*time.Second, "exit recorded after spawn failure", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(ar.s.Exits) >= 1
	})
	m.mu.Lock()
	exits := append([]RunExit(nil), ar.s.Exits...)
	m.mu.Unlock()
	for i, e := range exits {
		if e.Reason != "exit-error" {
			t.Fatalf("exit %d classified %q, want exit-error", i, e.Reason)
		}
	}
	if ar.stallKilled.Load() {
		t.Fatal("stallKilled must stay false for a spawn failure")
	}
}

// TestRecordingsViewExposesSuppressedAndRedactsRecordError covers the remaining
// A2 view surface: StderrSuppressed for an active recording and redaction of
// RecordLastError.
func TestRecordingsViewExposesSuppressedAndRedactsRecordError(t *testing.T) {
	m, _ := newTestManager(t, &fakeUploader{})
	now := time.Now()
	m.SetSchedule(sched(item("a", now.Add(-time.Second), now.Add(time.Hour))))
	ar := m.activeFor("a")
	if ar == nil {
		t.Fatal("not started")
	}
	// Wait until the run is under way so supervise's start-of-run resetRun() has
	// already fired; the healthy fake never restarts, so the per-run counter is
	// then stable for the rest of the test.
	waitFor(t, 5*time.Second, "ffmpeg running", func() bool { return ar.running.Load() })
	// A benign MOOV line is dropped but counted; the view surfaces the count.
	_, _ = ar.stderr.Write([]byte("[warning] Found duplicated MOOV Atom. Skipped it\n"))
	m.mu.Lock()
	ar.s.RecordLastError = "ffmpeg exited: reload of https://cdn/live.m3u8?token=SECRET failed"
	m.mu.Unlock()

	views := m.Recordings()
	if len(views) != 1 {
		t.Fatalf("views=%d", len(views))
	}
	if views[0].StderrSuppressed != 1 {
		t.Fatalf("stderrSuppressed = %d, want 1", views[0].StderrSuppressed)
	}
	if strings.Contains(views[0].RecordLastError, "SECRET") || views[0].RecordLastError == "" {
		t.Fatalf("recordLastError not redacted: %q", views[0].RecordLastError)
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

func TestLoadSessionOldSidecarZeroValues(t *testing.T) {
	// A sidecar written before Chunk A has no exits/recordLastError; it must load
	// cleanly with zero values so Recover() keeps working across upgrades.
	dir := t.TempDir()
	old := `{"schemaVersion":1,"id":"radio-1","safeId":"radio-1","sessionId":"radio-1_20260101T000000Z",` +
		`"source":"https://x/live.m3u8","start":"2026-01-01T00:00:00Z","end":"2026-01-01T01:00:00Z",` +
		`"state":"finalized","bytes":123,"restarts":2,"codec":"auto"}`
	p := filepath.Join(dir, "radio-1_20260101T000000Z.json")
	if err := os.WriteFile(p, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := loadSession(p)
	if err != nil {
		t.Fatalf("loadSession: %v", err)
	}
	if s.Exits != nil || s.RecordLastError != "" {
		t.Fatalf("old sidecar should yield zero values: exits=%v recordLastError=%q", s.Exits, s.RecordLastError)
	}
	if s.Restarts != 2 || s.Bytes != 123 {
		t.Fatalf("existing fields lost: %+v", s)
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
