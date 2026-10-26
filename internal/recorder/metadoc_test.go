package recorder

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

// storedDoc decodes the .json metadata document stored under key.
func storedDoc(t *testing.T, up *fakeUploader, key string) metadataDoc {
	t.Helper()
	o := up.object(key)
	if o == nil {
		t.Fatalf("no metadata document under %s", key)
	}
	if o.ctype != metadataContentType {
		t.Fatalf("metadata document content type = %q", o.ctype)
	}
	var doc metadataDoc
	if err := json.Unmarshal(o.body, &doc); err != nil {
		t.Fatalf("metadata document is not JSON: %v\n%s", err, o.body)
	}
	return doc
}

// TestMetadataDocumentMirrorsTheObject: every stored recording gets a .json
// next to its .m4a that carries the same facts as the object metadata and the
// container tags — typed, with the run table as a real array.
func TestMetadataDocumentMirrorsTheObject(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	jkey := strings.TrimSuffix(key, ".m4a") + ".json"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
	m, rx := newRecoverManager(t, dir, up)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	if len(up.docCalls) != 1 || up.docCalls[0].key != jkey || !up.docCalls[0].overwrite {
		t.Fatalf("metadata puts = %+v, want one unconditional put to %s", up.docCalls, jkey)
	}
	if got := up.docCalls[0].meta["recording-id"]; got != "a" {
		t.Errorf("metadata document recording-id = %q", got)
	}
	a := up.calls[0].meta
	doc := storedDoc(t, up, jkey)
	if doc.Schema != metadataSchema || doc.RecorderVersion != "test" || doc.RecordingID != "a" || doc.Name != "Test a" {
		t.Errorf("identity = %+v", doc)
	}
	if doc.Audio.Key != key || doc.Audio.ContentType != "audio/mp4" || doc.Audio.Codec != a["codec"] ||
		doc.Audio.Frames != 30 || doc.Audio.RemuxFailed {
		t.Errorf("audio = %+v, object metadata %v", doc.Audio, a)
	}
	if want, err := strconv.ParseFloat(a["duration-seconds"], 64); err != nil || doc.Audio.DurationSeconds != want {
		t.Errorf("durationSeconds = %v, object says %q", doc.Audio.DurationSeconds, a["duration-seconds"])
	}
	if doc.Parts != 1 || strings.Join(doc.Sessions, ",") != a["sessions"] {
		t.Errorf("parts/sessions = %d %q, object says %s %s", doc.Parts, doc.Sessions, a["parts"], a["sessions"])
	}
	for name, pair := range map[string][2]string{
		"scheduledStart":   {doc.ScheduledStart, a["scheduled-start"]},
		"scheduledEnd":     {doc.ScheduledEnd, a["scheduled-end"]},
		"lastSessionStart": {doc.LastSessionStart, a["last-session-start"]},
		"firstSampleTime":  {doc.FirstSampleTime, a["first-sample-time"]},
		"clockSource":      {doc.ClockSource, a["clock-source"]},
		"source":           {doc.Source, a["source"]},
	} {
		if pair[0] == "" || pair[0] != pair[1] {
			t.Errorf("%s = %q, object says %q", name, pair[0], pair[1])
		}
	}
	if doc.Video != nil {
		t.Errorf("video = %+v without RECORD_VIDEO", doc.Video)
	}
	meta, ok := rx.lastMeta()
	if !ok || doc.Tags == nil || doc.Tags.Title != meta.Title || doc.Tags.Comment != meta.Comment ||
		doc.Tags.Date != meta.Date || doc.Date != meta.Date || doc.Tags.CreationTime == "" {
		t.Fatalf("tags = %+v, embedded %+v", doc.Tags, meta)
	}
	runs, err := json.Marshal(doc.Runs)
	if err != nil || string(runs) != meta.Description {
		t.Fatalf("runs = %s, embedded table %s", runs, meta.Description)
	}
	if n := counterValue(t, m, "recorder_metadata_uploads_total", map[string]string{"result": "success"}); n != 1 {
		t.Fatalf("metadata success metric = %v, want 1", n)
	}
}

// TestMetadataDocumentFollowsMerge: a session merged into the stored object
// rewrites the document with the merged object's view (both parts).
func TestMetadataDocumentFollowsMerge(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
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
	doc := storedDoc(t, up, strings.TrimSuffix(key, ".m4a")+".json")
	want := "aaaaaaaa," + sessionDigest(s.SessionID)
	if doc.Parts != 2 || strings.Join(doc.Sessions, ",") != want || doc.Sessions[1] != sessionDigest(s.SessionID) {
		t.Fatalf("merged document parts=%d sessions=%q, want 2 %q", doc.Parts, doc.Sessions, want)
	}
}

// TestMetadataDocumentGoesBeforeTheAudio: the document is written before the
// .m4a, so a transfer error on it retries the attempt and the document is
// never missing next to a stored object; a refusal that cannot be retried
// costs the document only, never the audio.
func TestMetadataDocumentGoesBeforeTheAudio(t *testing.T) {
	t.Run("transient", func(t *testing.T) {
		dir := t.TempDir()
		start := time.Now().Add(-2 * time.Hour)
		key := start.UTC().Format("2006-01-02") + "/a.m4a"
		s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

		up := &fakeUploader{docErrs: []error{errors.New("connection reset by peer")}}
		m, _ := newRecoverManager(t, dir, up)
		m.Recover()
		m.settle()

		if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
			t.Fatalf("state = %s", st)
		}
		if up.count() != 1 || len(up.docCalls) != 1 {
			t.Fatalf("media puts = %d, document puts = %d; want the .m4a once, after the document", up.count(), len(up.docCalls))
		}
		storedDoc(t, up, strings.TrimSuffix(key, ".m4a")+".json")
		if n := counterValue(t, m, "recorder_metadata_uploads_total", map[string]string{"result": "failure"}); n != 1 {
			t.Fatalf("metadata failure metric = %v, want 1", n)
		}
	})
	t.Run("permanent", func(t *testing.T) {
		dir := t.TempDir()
		start := time.Now().Add(-2 * time.Hour)
		key := start.UTC().Format("2006-01-02") + "/a.m4a"
		s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

		up := &fakeUploader{docErrs: []error{&PermanentError{Err: errors.New("AccessDenied")}}}
		m, _ := newRecoverManager(t, dir, up)
		m.Recover()
		m.settle()

		if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
			t.Fatalf("state = %s", st)
		}
		if up.count() != 1 || up.calls[0].key != key {
			t.Fatalf("calls = %+v, want the .m4a stored without its document", up.calls)
		}
		if o := up.object(strings.TrimSuffix(key, ".m4a") + ".json"); o != nil {
			t.Fatalf("a refused document must not be stored: %s", o.body)
		}
	})
}

// TestMetadataDocumentForRawFallback: the raw .aac fallback gets a document
// too — same base name, no container tags, but the run table.
func TestMetadataDocumentForRawFallback(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-2 * time.Hour)
	key := start.UTC().Format("2006-01-02") + "/a.m4a"
	s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)

	up := &fakeUploader{}
	m, rx := newRecoverManager(t, dir, up)
	boom := errors.New("remux: ffmpeg exited 1")
	rx.setErrs(boom, boom, boom)
	m.Recover()
	m.settle()

	if st, _ := m.sessionState(s.SessionID); st != StateUploaded {
		t.Fatalf("state = %s", st)
	}
	doc := storedDoc(t, up, strings.TrimSuffix(key, ".m4a")+".json")
	if doc.Audio.Key != strings.TrimSuffix(key, ".m4a")+".aac" || doc.Audio.ContentType != "audio/aac" || !doc.Audio.RemuxFailed {
		t.Fatalf("audio = %+v", doc.Audio)
	}
	if doc.Tags != nil {
		t.Fatalf("raw ADTS has no container tags, document says %+v", doc.Tags)
	}
	if len(doc.Runs) != 1 || doc.Runs[0].SID != s.SessionID {
		t.Fatalf("runs = %+v", doc.Runs)
	}
}

// TestMetadataDocumentDescribesVideo: with RECORD_VIDEO the document says
// where the .mp4 is and what it holds — or why it is missing.
func TestMetadataDocumentDescribesVideo(t *testing.T) {
	t.Run("stored", func(t *testing.T) {
		dir := t.TempDir()
		start := time.Now().Add(-2 * time.Hour)
		key := start.UTC().Format("2006-01-02") + "/a.m4a"
		s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)
		markVideo(t, s, "none")

		up := &fakeUploader{}
		m, _ := newVideoRecoverManager(t, dir, up)
		m.Recover()
		m.settle()

		doc := storedDoc(t, up, strings.TrimSuffix(key, ".m4a")+".json")
		v := doc.Video
		if v == nil || !v.Stored || v.Key != strings.TrimSuffix(key, ".m4a")+".mp4" || v.Kind != "black" ||
			v.Synthetic == nil || !*v.Synthetic || v.Codec != "h264" || v.Width != 16 || v.Height != 16 || v.Error != "" {
			t.Fatalf("video = %+v", v)
		}
	})
	t.Run("refused", func(t *testing.T) {
		dir := t.TempDir()
		start := time.Now().Add(-2 * time.Hour)
		key := start.UTC().Format("2006-01-02") + "/a.m4a"
		s := seedFinalized(t, dir, "a", "a_20260101T000000Z", start, key, 30)
		markVideo(t, s, "none")

		up := &fakeUploader{errs: []error{&PermanentError{Err: errors.New("NotImplemented")}}}
		m, _ := newVideoRecoverManager(t, dir, up)
		m.Recover()
		m.settle()

		doc := storedDoc(t, up, strings.TrimSuffix(key, ".m4a")+".json")
		if v := doc.Video; v == nil || v.Stored || !strings.Contains(v.Error, "video upload refused") || v.Kind != "" {
			t.Fatalf("video = %+v", v)
		}
	})
}

func TestMetadataKey(t *testing.T) {
	for in, want := range map[string]string{
		"2026-10-26/a.m4a":   "2026-10-26/a.json",
		"2026-10-26/a.aac":   "2026-10-26/a.json",
		"2026-10-26/a-2.m4a": "2026-10-26/a-2.json",
	} {
		if got := metadataKey(in); got != want {
			t.Errorf("metadataKey(%q) = %q, want %q", in, got, want)
		}
	}
}
