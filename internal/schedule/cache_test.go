package schedule

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// The cache must preserve rejected ids: after a restart a present-but-invalid
// item must not look "removed". It must also refuse documents written for a
// different URL and corrupt files.
func TestCachePreservesInvalidIDsAndChecksURL(t *testing.T) {
	doc := `[
	  {"id":"ok","source":"http://h/a","start":"2026-01-01T10:00:00Z","end":"2026-01-01T11:00:00Z"},
	  {"id":"typo","source":"ftp://h/b","start":"2026-01-01T10:00:00Z","end":"2026-01-01T11:00:00Z"}
	]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(doc))
	}))
	defer srv.Close()
	dir := t.TempDir()
	f := NewFetcher(srv.URL, 2*time.Second, "", "", dir, time.UTC)
	s, err := f.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !s.Present("typo") || len(s.Invalid) != 1 {
		t.Fatalf("fetch: Present(typo)=%v invalid=%+v", s.Present("typo"), s.Invalid)
	}

	c, err := f.LoadCache()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Items) != 1 || c.Items[0].ID != "ok" {
		t.Fatalf("cached items = %+v", c.Items)
	}
	if !c.Present("typo") || c.InvalidReason("typo") == "" {
		t.Fatalf("cache lost the rejected id: invalid=%+v", c.Invalid)
	}
	if c.Present("gone") {
		t.Fatal("unknown id must not be present")
	}

	// Same cache dir, different SCHEDULE_URL: refuse it.
	other := NewFetcher(srv.URL+"/other", 2*time.Second, "", "", dir, time.UTC)
	if _, err := other.LoadCache(); err == nil || !strings.Contains(err.Error(), "different SCHEDULE_URL") {
		t.Fatalf("expected URL mismatch error, got %v", err)
	}

	// Corrupt or foreign documents are errors, never an empty schedule.
	for _, bad := range []string{"", "{", `{"fetchedAt":"2026-01-01T00:00:00Z"}`, `{"items":"nope"}`, `[]`} {
		if err := os.WriteFile(f.CachePath, []byte(bad), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := f.LoadCache(); err == nil {
			t.Fatalf("corrupt cache %q accepted", bad)
		}
	}
	// A genuinely empty schedule is fine.
	if err := os.WriteFile(f.CachePath, []byte(`{"items":[],"url":"`+srv.URL+`"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if c, err := f.LoadCache(); err != nil || len(c.Items) != 0 || c.Origin != "cache" {
		t.Fatalf("empty cache: %+v err=%v", c, err)
	}
}
