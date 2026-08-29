// Package httpapi serves the heartbeat, state and metrics endpoints.
package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Deps are the data sources the API exposes. Functions are called per request.
type Deps struct {
	Version            string
	StartedAt          time.Time
	Registry           *prometheus.Registry
	APIToken           string // optional bearer token for /api/*
	MetricsRequireAuth bool   // also protect /metrics with the token
	Log                *slog.Logger

	Health     func() (ok bool, checks map[string]any) // liveness
	Ready      func() (ok bool, reason string)         // readiness
	State      func() any                              // schedule info + recordings + uploads
	Schedule   func() any
	Recordings func() any
	System     func() any
}

// New builds the HTTP server.
func New(addr string, d Deps) *http.Server {
	if d.Log == nil {
		d.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	mux := http.NewServeMux()
	hostname, _ := os.Hostname()

	heartbeat := func(w http.ResponseWriter, r *http.Request) {
		ok, checks := true, map[string]any{}
		if d.Health != nil {
			ok, checks = d.Health()
		}
		status := http.StatusOK
		state := "ok"
		if !ok {
			status = http.StatusServiceUnavailable
			state = "unhealthy"
		}
		writeJSON(w, status, map[string]any{
			"status":   state,
			"version":  d.Version,
			"hostname": hostname,
			"time":     time.Now().UTC().Format(time.RFC3339),
			"uptime":   time.Since(d.StartedAt).Truncate(time.Second).String(),
			"checks":   checks,
		})
	}
	mux.HandleFunc("GET /healthz", heartbeat)
	mux.HandleFunc("GET /health", heartbeat)
	mux.HandleFunc("GET /heartbeat", heartbeat)

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if d.Ready == nil {
			writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
			return
		}
		if ok, reason := d.Ready(); ok {
			writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
		} else {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "reason": reason})
		}
	})

	metricsHandler := promhttp.HandlerFor(d.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
		ErrorLog:          slog.NewLogLogger(d.Log.Handler(), slog.LevelWarn),
	})
	if d.MetricsRequireAuth {
		mux.Handle("GET /metrics", auth(d.APIToken, metricsHandler))
	} else {
		mux.Handle("GET /metrics", metricsHandler)
	}

	api := http.NewServeMux()
	api.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		doc := map[string]any{
			"version":  d.Version,
			"hostname": hostname,
			"time":     time.Now().UTC().Format(time.RFC3339),
			"uptime":   time.Since(d.StartedAt).Truncate(time.Second).String(),
		}
		if d.Ready != nil {
			ok, reason := d.Ready()
			doc["ready"] = ok
			if !ok {
				doc["notReadyReason"] = reason
			}
		}
		if d.Health != nil {
			ok, checks := d.Health()
			doc["healthy"] = ok
			doc["checks"] = checks
		}
		if d.State != nil {
			if st, ok := d.State().(map[string]any); ok {
				for k, v := range st {
					doc[k] = v
				}
			} else {
				doc["recorder"] = d.State()
			}
		}
		if d.System != nil {
			doc["system"] = d.System()
		}
		writeJSON(w, http.StatusOK, doc)
	})
	call := func(f func() any) any {
		if f == nil {
			return map[string]any{}
		}
		return f()
	}
	api.HandleFunc("GET /api/schedule", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, call(d.Schedule))
	})
	api.HandleFunc("GET /api/recordings", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, call(d.Recordings))
	})
	api.HandleFunc("GET /api/system", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, call(d.System))
	})
	mux.Handle("/api/", auth(d.APIToken, api))

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"service": "spectado-stream-recorder",
			"version": d.Version,
			"endpoints": []string{
				"/healthz", "/readyz", "/metrics",
				"/api/state", "/api/schedule", "/api/recordings", "/api/system",
			},
		})
	})

	return &http.Server{
		Addr:              addr,
		Handler:           logging(d.Log, mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

func auth(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := ""
		if h := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(h), "bearer ") {
			got = strings.TrimSpace(h[7:])
		} else if q := r.URL.Query().Get("token"); q != "" {
			got = q
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="recorder"`)
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func logging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		switch r.URL.Path {
		case "/healthz", "/health", "/heartbeat", "/metrics", "/readyz":
			return // scraped constantly; keep logs quiet
		}
		log.Debug("http", "method", r.Method, "path", r.URL.Path, "status", sw.status,
			"duration", time.Since(start).Truncate(time.Microsecond).String(), "remote", r.RemoteAddr)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
