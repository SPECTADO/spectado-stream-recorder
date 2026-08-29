package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// testDeps returns a fully populated Deps with a registry holding one gauge.
func testDeps() Deps {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "recorder_test_gauge", Help: "test gauge"})
	g.Set(7)
	reg.MustRegister(g)
	return Deps{
		Version:   "1.2.3",
		StartedAt: time.Now().Add(-90 * time.Second),
		Registry:  reg,
		Log:       discardLog(),
		Health:    func() (bool, map[string]any) { return true, map[string]any{"disk": "ok", "ffmpeg": true} },
		Ready:     func() (bool, string) { return true, "" },
		State: func() any {
			return map[string]any{"scheduleLoaded": true, "recordings": []any{map[string]any{"id": "radio-1"}}}
		},
		Schedule:   func() any { return map[string]any{"url": "https://example.com/s.json", "items": 2.0} },
		Recordings: func() any { return []any{map[string]any{"id": "radio-1", "state": "recording"}} },
		System:     func() any { return map[string]any{"cpuPercent": 12.5, "hostname": "box"} },
	}
}

func newHandler(t *testing.T, d Deps) http.Handler {
	t.Helper()
	srv := New(":0", d)
	if srv == nil || srv.Handler == nil {
		t.Fatal("New returned no handler")
	}
	if srv.Addr != ":0" {
		t.Errorf("Addr = %q, want :0", srv.Addr)
	}
	return srv.Handler
}

type reqOpt func(*http.Request)

func bearer(token string) reqOpt {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

func header(k, v string) reqOpt {
	return func(r *http.Request) { r.Header.Set(k, v) }
}

func do(h http.Handler, method, target string, opts ...reqOpt) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	for _, o := range opts {
		o(req)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func get(h http.Handler, target string, opts ...reqOpt) *httptest.ResponseRecorder {
	return do(h, http.MethodGet, target, opts...)
}

func decodeObject(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q, want application/json; charset=utf-8", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}
	var doc map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("body is not a JSON object: %v\n%s", err, rec.Body.String())
	}
	return doc
}

func TestHealthz(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		h := newHandler(t, testDeps())
		for _, path := range []string{"/healthz", "/health", "/heartbeat"} {
			rec := get(h, path)
			if rec.Code != http.StatusOK {
				t.Errorf("%s: status = %d, want 200", path, rec.Code)
			}
			doc := decodeObject(t, rec)
			if doc["status"] != "ok" {
				t.Errorf("%s: status field = %v, want ok", path, doc["status"])
			}
			if doc["version"] != "1.2.3" {
				t.Errorf("%s: version = %v", path, doc["version"])
			}
			checks, _ := doc["checks"].(map[string]any)
			if checks["disk"] != "ok" || checks["ffmpeg"] != true {
				t.Errorf("%s: checks = %v", path, doc["checks"])
			}
			if _, ok := doc["hostname"]; !ok {
				t.Errorf("%s: hostname missing", path)
			}
			if ts, _ := doc["time"].(string); ts == "" {
				t.Errorf("%s: time missing", path)
			} else if _, err := time.Parse(time.RFC3339, ts); err != nil {
				t.Errorf("%s: time %q is not RFC3339: %v", path, ts, err)
			}
			up, err := time.ParseDuration(doc["uptime"].(string))
			if err != nil || up < 90*time.Second || up > time.Hour {
				t.Errorf("%s: uptime = %v (%v), want about 90s", path, doc["uptime"], err)
			}
		}
	})
	t.Run("unhealthy", func(t *testing.T) {
		d := testDeps()
		d.Health = func() (bool, map[string]any) { return false, map[string]any{"disk": "low", "freeBytes": 1024.0} }
		h := newHandler(t, d)
		rec := get(h, "/healthz")
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", rec.Code)
		}
		doc := decodeObject(t, rec)
		if doc["status"] != "unhealthy" {
			t.Errorf("status field = %v, want unhealthy", doc["status"])
		}
		checks, _ := doc["checks"].(map[string]any)
		if checks["disk"] != "low" || checks["freeBytes"] != 1024.0 {
			t.Errorf("checks = %v", doc["checks"])
		}
	})
	t.Run("no health func", func(t *testing.T) {
		d := testDeps()
		d.Health = nil
		h := newHandler(t, d)
		rec := get(h, "/healthz")
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
		doc := decodeObject(t, rec)
		if doc["status"] != "ok" {
			t.Errorf("status field = %v", doc["status"])
		}
		if checks, ok := doc["checks"].(map[string]any); !ok || len(checks) != 0 {
			t.Errorf("checks = %v, want empty object", doc["checks"])
		}
	})
}

func TestReadyz(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		rec := get(newHandler(t, testDeps()), "/readyz")
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
		doc := decodeObject(t, rec)
		if doc["status"] != "ready" {
			t.Errorf("status field = %v", doc["status"])
		}
		if _, ok := doc["reason"]; ok {
			t.Errorf("reason present on a ready response: %v", doc)
		}
	})
	t.Run("not ready", func(t *testing.T) {
		d := testDeps()
		d.Ready = func() (bool, string) { return false, "schedule not loaded" }
		rec := get(newHandler(t, d), "/readyz")
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", rec.Code)
		}
		doc := decodeObject(t, rec)
		if doc["status"] != "not_ready" || doc["reason"] != "schedule not loaded" {
			t.Errorf("doc = %v", doc)
		}
	})
	t.Run("no ready func", func(t *testing.T) {
		d := testDeps()
		d.Ready = nil
		rec := get(newHandler(t, d), "/readyz")
		if rec.Code != http.StatusOK || decodeObject(t, rec)["status"] != "ready" {
			t.Errorf("status = %d body = %s", rec.Code, rec.Body.String())
		}
	})
}

func TestMetrics(t *testing.T) {
	rec := get(newHandler(t, testDeps()), "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body:\n%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "# TYPE recorder_test_gauge gauge") {
		t.Errorf("metrics body lacks the TYPE line:\n%s", body)
	}
	if !strings.Contains(body, "recorder_test_gauge 7") {
		t.Errorf("metrics body lacks the gauge sample:\n%s", body)
	}
	if ct := rec.Header().Get("Content-Type"); ct == "" {
		t.Error("metrics response has no Content-Type")
	}

	// A different registry yields different content: nothing is global.
	d := testDeps()
	d.Registry = prometheus.NewRegistry()
	if body := get(newHandler(t, d), "/metrics").Body.String(); strings.Contains(body, "recorder_test_gauge") {
		t.Errorf("fresh registry still exposes recorder_test_gauge:\n%s", body)
	}
}

func TestAPIState(t *testing.T) {
	t.Run("map state is merged", func(t *testing.T) {
		rec := get(newHandler(t, testDeps()), "/api/state")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		doc := decodeObject(t, rec)
		if doc["version"] != "1.2.3" {
			t.Errorf("version = %v", doc["version"])
		}
		if doc["scheduleLoaded"] != true {
			t.Errorf("State() keys were not merged: %v", doc)
		}
		recs, _ := doc["recordings"].([]any)
		if len(recs) != 1 {
			t.Errorf("recordings = %v", doc["recordings"])
		}
		if _, ok := doc["recorder"]; ok {
			t.Errorf("map state must be merged, not nested under recorder: %v", doc)
		}
		if doc["ready"] != true || doc["healthy"] != true {
			t.Errorf("ready = %v healthy = %v", doc["ready"], doc["healthy"])
		}
		if _, ok := doc["notReadyReason"]; ok {
			t.Errorf("notReadyReason present while ready: %v", doc)
		}
		checks, _ := doc["checks"].(map[string]any)
		if checks["disk"] != "ok" {
			t.Errorf("checks = %v", doc["checks"])
		}
		sys, _ := doc["system"].(map[string]any)
		if sys["cpuPercent"] != 12.5 {
			t.Errorf("system = %v", doc["system"])
		}
		for _, k := range []string{"hostname", "time", "uptime"} {
			if _, ok := doc[k]; !ok {
				t.Errorf("%s missing from %v", k, doc)
			}
		}
		if up, err := time.ParseDuration(doc["uptime"].(string)); err != nil || up < 90*time.Second {
			t.Errorf("uptime = %v (%v)", doc["uptime"], err)
		}
	})
	t.Run("not ready adds a reason", func(t *testing.T) {
		d := testDeps()
		d.Ready = func() (bool, string) { return false, "warming up" }
		d.Health = func() (bool, map[string]any) { return false, map[string]any{"disk": "low"} }
		doc := decodeObject(t, get(newHandler(t, d), "/api/state"))
		if doc["ready"] != false || doc["notReadyReason"] != "warming up" {
			t.Errorf("ready = %v reason = %v", doc["ready"], doc["notReadyReason"])
		}
		if doc["healthy"] != false {
			t.Errorf("healthy = %v", doc["healthy"])
		}
	})
	t.Run("non-map state is nested under recorder", func(t *testing.T) {
		type st struct {
			Active int `json:"active"`
		}
		d := testDeps()
		d.State = func() any { return st{Active: 3} }
		doc := decodeObject(t, get(newHandler(t, d), "/api/state"))
		rec, _ := doc["recorder"].(map[string]any)
		if rec["active"] != 3.0 {
			t.Errorf("recorder = %v", doc["recorder"])
		}
	})
	t.Run("state keys override the envelope", func(t *testing.T) {
		d := testDeps()
		d.State = func() any { return map[string]any{"version": "from-state"} }
		doc := decodeObject(t, get(newHandler(t, d), "/api/state"))
		if doc["version"] != "from-state" {
			t.Errorf("version = %v, want the State() value to win", doc["version"])
		}
	})
	t.Run("optional deps may be nil", func(t *testing.T) {
		d := testDeps()
		d.Ready, d.Health, d.State, d.System = nil, nil, nil, nil
		rec := get(newHandler(t, d), "/api/state")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		doc := decodeObject(t, rec)
		for _, k := range []string{"ready", "healthy", "checks", "recorder", "system", "notReadyReason"} {
			if _, ok := doc[k]; ok {
				t.Errorf("%s present although its dep is nil: %v", k, doc)
			}
		}
		if doc["version"] != "1.2.3" {
			t.Errorf("version = %v", doc["version"])
		}
	})
}

func TestAPIEndpoints(t *testing.T) {
	d := testDeps()
	h := newHandler(t, d)
	cases := []struct {
		path string
		want any
	}{
		{"/api/schedule", d.Schedule()},
		{"/api/recordings", d.Recordings()},
		{"/api/system", d.System()},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			rec := get(h, tc.path)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d", rec.Code)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
				t.Errorf("Content-Type = %q", ct)
			}
			want, _ := json.Marshal(tc.want)
			var gotDoc, wantDoc any
			if err := json.Unmarshal(rec.Body.Bytes(), &gotDoc); err != nil {
				t.Fatalf("body is not JSON: %v", err)
			}
			_ = json.Unmarshal(want, &wantDoc)
			got, _ := json.Marshal(gotDoc)
			want, _ = json.Marshal(wantDoc)
			if string(got) != string(want) {
				t.Errorf("body = %s, want %s", got, want)
			}
		})
	}
	// Functions are invoked per request, not captured at construction.
	calls := 0
	d.Recordings = func() any { calls++; return []any{} }
	h = newHandler(t, d)
	get(h, "/api/recordings")
	get(h, "/api/recordings")
	if calls != 2 {
		t.Errorf("Recordings called %d times, want 2", calls)
	}
}

func TestAuth(t *testing.T) {
	d := testDeps()
	d.APIToken = "s3cret"
	h := newHandler(t, d)

	cases := []struct {
		name     string
		path     string
		opts     []reqOpt
		wantCode int
	}{
		{"no token", "/api/state", nil, http.StatusUnauthorized},
		{"bearer token", "/api/state", []reqOpt{bearer("s3cret")}, http.StatusOK},
		{"lower-case scheme", "/api/state", []reqOpt{header("Authorization", "bearer s3cret")}, http.StatusOK},
		{"padded bearer token", "/api/state", []reqOpt{header("Authorization", "Bearer   s3cret  ")}, http.StatusOK},
		{"query token", "/api/state?token=s3cret", nil, http.StatusOK},
		{"wrong bearer", "/api/state", []reqOpt{bearer("nope")}, http.StatusUnauthorized},
		{"wrong query", "/api/state?token=nope", nil, http.StatusUnauthorized},
		{"prefix of token", "/api/state", []reqOpt{bearer("s3cre")}, http.StatusUnauthorized},
		{"token with suffix", "/api/state", []reqOpt{bearer("s3cret1")}, http.StatusUnauthorized},
		{"basic scheme is rejected", "/api/state", []reqOpt{header("Authorization", "Basic s3cret")}, http.StatusUnauthorized},
		{"bearer header wins over bad query", "/api/state?token=nope", []reqOpt{bearer("s3cret")}, http.StatusOK},
		{"bad bearer header is not rescued by query", "/api/state?token=s3cret", []reqOpt{bearer("nope")}, http.StatusUnauthorized},
		{"schedule needs token", "/api/schedule", nil, http.StatusUnauthorized},
		{"recordings needs token", "/api/recordings", nil, http.StatusUnauthorized},
		{"system needs token", "/api/system", nil, http.StatusUnauthorized},
		{"schedule with token", "/api/schedule", []reqOpt{bearer("s3cret")}, http.StatusOK},
		{"unknown api path without token", "/api/nope", nil, http.StatusUnauthorized},
		{"unknown api path with token", "/api/nope", []reqOpt{bearer("s3cret")}, http.StatusNotFound},
		{"healthz stays open", "/healthz", nil, http.StatusOK},
		{"health stays open", "/health", nil, http.StatusOK},
		{"readyz stays open", "/readyz", nil, http.StatusOK},
		{"root stays open", "/", nil, http.StatusOK},
		{"metrics open by default", "/metrics", nil, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := get(h, tc.path, tc.opts...)
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantCode == http.StatusUnauthorized {
				if got := rec.Header().Get("WWW-Authenticate"); got != `Bearer realm="recorder"` {
					t.Errorf("WWW-Authenticate = %q", got)
				}
				if doc := decodeObject(t, rec); doc["error"] != "unauthorized" {
					t.Errorf("body = %v", doc)
				}
			} else if rec.Header().Get("WWW-Authenticate") != "" {
				t.Errorf("WWW-Authenticate set on a %d response", rec.Code)
			}
		})
	}

	t.Run("no token configured leaves the api open", func(t *testing.T) {
		h := newHandler(t, testDeps())
		for _, p := range []string{"/api/state", "/api/schedule", "/api/recordings", "/api/system"} {
			if rec := get(h, p); rec.Code != http.StatusOK {
				t.Errorf("%s: status = %d, want 200", p, rec.Code)
			}
		}
		if rec := get(h, "/api/state", bearer("anything")); rec.Code != http.StatusOK {
			t.Errorf("stray Authorization header rejected: %d", rec.Code)
		}
	})
}

func TestMetricsAuth(t *testing.T) {
	t.Run("required", func(t *testing.T) {
		d := testDeps()
		d.APIToken = "s3cret"
		d.MetricsRequireAuth = true
		h := newHandler(t, d)
		if rec := get(h, "/metrics"); rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("no token: status = %d", rec.Code)
		}
		if rec := get(h, "/metrics", bearer("nope")); rec.Code != http.StatusUnauthorized {
			t.Errorf("wrong token: status = %d", rec.Code)
		}
		if rec := get(h, "/metrics", bearer("s3cret")); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "recorder_test_gauge") {
			t.Errorf("bearer: status = %d", rec.Code)
		}
		if rec := get(h, "/metrics?token=s3cret"); rec.Code != http.StatusOK {
			t.Errorf("query token: status = %d", rec.Code)
		}
		// Other endpoints keep their usual policy.
		if rec := get(h, "/healthz"); rec.Code != http.StatusOK {
			t.Errorf("healthz: status = %d", rec.Code)
		}
		if rec := get(h, "/api/state"); rec.Code != http.StatusUnauthorized {
			t.Errorf("api without token: status = %d", rec.Code)
		}
	})
	t.Run("not required", func(t *testing.T) {
		d := testDeps()
		d.APIToken = "s3cret"
		d.MetricsRequireAuth = false
		h := newHandler(t, d)
		if rec := get(h, "/metrics"); rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
		if rec := get(h, "/api/state"); rec.Code != http.StatusUnauthorized {
			t.Errorf("api without token: status = %d, want 401", rec.Code)
		}
	})
	t.Run("required but no token configured", func(t *testing.T) {
		d := testDeps()
		d.MetricsRequireAuth = true
		if rec := get(newHandler(t, d), "/metrics"); rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200 (empty token disables auth)", rec.Code)
		}
	})
}

func TestRoot(t *testing.T) {
	rec := get(newHandler(t, testDeps()), "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	doc := decodeObject(t, rec)
	if doc["service"] != "spectado-stream-recorder" || doc["version"] != "1.2.3" {
		t.Errorf("doc = %v", doc)
	}
	eps, _ := doc["endpoints"].([]any)
	want := []string{"/healthz", "/readyz", "/metrics", "/api/state", "/api/schedule", "/api/recordings", "/api/system"}
	if len(eps) != len(want) {
		t.Fatalf("endpoints = %v, want %v", eps, want)
	}
	for i, w := range want {
		if eps[i] != w {
			t.Errorf("endpoints[%d] = %v, want %s", i, eps[i], w)
		}
	}
}

func TestRouting(t *testing.T) {
	h := newHandler(t, testDeps())
	cases := []struct {
		name     string
		method   string
		path     string
		wantCode int
	}{
		{"unknown path", http.MethodGet, "/nope", http.StatusNotFound},
		{"unknown nested path", http.MethodGet, "/healthz/extra", http.StatusNotFound},
		{"root only matches exactly", http.MethodGet, "/index.html", http.StatusNotFound},
		{"unknown api path", http.MethodGet, "/api/nope", http.StatusNotFound},
		{"api prefix alone", http.MethodGet, "/api/", http.StatusNotFound},
		{"POST healthz", http.MethodPost, "/healthz", http.StatusMethodNotAllowed},
		{"PUT readyz", http.MethodPut, "/readyz", http.StatusMethodNotAllowed},
		{"POST metrics", http.MethodPost, "/metrics", http.StatusMethodNotAllowed},
		{"POST api state", http.MethodPost, "/api/state", http.StatusMethodNotAllowed},
		{"DELETE api recordings", http.MethodDelete, "/api/recordings", http.StatusMethodNotAllowed},
		{"POST root", http.MethodPost, "/", http.StatusMethodNotAllowed},
		{"HEAD healthz", http.MethodHead, "/healthz", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(h, tc.method, tc.path)
			if rec.Code != tc.wantCode {
				t.Errorf("%s %s: status = %d, want %d", tc.method, tc.path, rec.Code, tc.wantCode)
			}
			if tc.wantCode == http.StatusMethodNotAllowed && rec.Header().Get("Allow") == "" {
				t.Errorf("%s %s: 405 without an Allow header", tc.method, tc.path)
			}
		})
	}
}

func TestServerTimeouts(t *testing.T) {
	srv := New("127.0.0.1:0", testDeps())
	if srv.Addr != "127.0.0.1:0" {
		t.Errorf("Addr = %q", srv.Addr)
	}
	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.WriteTimeout <= 0 || srv.IdleTimeout <= 0 {
		t.Errorf("timeouts not set: %+v", srv)
	}
}
