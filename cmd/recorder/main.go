// Command recorder records scheduled live audio streams and uploads them to
// S3 compatible storage.
//
// Subcommands:
//
//	recorder               run the service
//	recorder healthcheck   GET /healthz on HTTP_ADDR and exit 0/1 (for Docker HEALTHCHECK)
//	recorder --version     print the version
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/spectado/stream-recorder/internal/config"
	"github.com/spectado/stream-recorder/internal/httpapi"
	"github.com/spectado/stream-recorder/internal/metrics"
	"github.com/spectado/stream-recorder/internal/recorder"
	"github.com/spectado/stream-recorder/internal/schedule"
	"github.com/spectado/stream-recorder/internal/storage"
	"github.com/spectado/stream-recorder/internal/sysmon"
)

// version is injected at build time: -ldflags "-X main.version=1.2.3".
var version = "dev"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "--version", "-v", "version":
			fmt.Println(version)
			return
		case "healthcheck":
			os.Exit(healthcheck())
		}
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

// healthcheck probes the local /healthz endpoint; used by the Docker HEALTHCHECK.
func healthcheck() int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid HTTP_ADDR:", addr)
		return 1
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: status %d: %s\n", resp.StatusCode, strings.TrimSpace(string(body)))
		return 1
	}
	return 0
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration:\n%w", err)
	}
	log := newLogger(cfg)
	slog.SetDefault(log)
	startedAt := time.Now()

	log.Info("starting spectado-stream-recorder", "version", version, "go", runtime.Version(),
		"scheduleUrl", recorder.RedactURL(cfg.ScheduleURL), "pollInterval", cfg.SchedulePollInterval.String(),
		"dataDir", cfg.DataDir, "httpAddr", cfg.HTTPAddr, "uploads", !cfg.UploadDisabled,
		"bucket", cfg.S3Bucket, "codec", cfg.AudioCodec, "bitrate", cfg.AudioBitrate,
		"startEarly", cfg.RecordStartEarly.String(), "stopLate", cfg.RecordStopLate.String())

	if err := prepareDataDir(cfg.DataDir); err != nil {
		return err
	}
	unlock, err := lockDataDir(cfg.DataDir)
	if err != nil {
		return err
	}
	defer unlock()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	met := metrics.New(version)

	// ffmpeg/ffprobe must be usable; record which version we run with.
	ffPath, ffVersion, err := checkFFmpeg(ctx, cfg.FFmpegPath)
	if err != nil {
		return err
	}
	if _, _, err := checkFFmpeg(ctx, cfg.FFprobePath); err != nil {
		return err
	}
	met.FFmpegInfo.WithLabelValues(ffVersion, ffPath).Set(1)
	log.Info("ffmpeg available", "version", ffVersion, "path", ffPath)
	if cfg.APIToken == "" && !isLoopback(cfg.HTTPAddr) {
		log.Warn("API_TOKEN is empty: /api/* and /metrics are unauthenticated on a non-loopback address")
	}

	var up recorder.Uploader
	var s3up *storage.S3
	if !cfg.UploadDisabled {
		s3up, err = storage.NewS3(ctx, cfg, log.With("component", "s3"))
		if err != nil {
			return err
		}
		up = s3up
	} else {
		log.Warn("UPLOAD_DISABLED=true: recordings stay in the data directory")
	}

	mgr := recorder.NewManager(cfg, log, met, up, version)
	caps := recorder.DetectFFmpegCapabilities(ctx, ffPath)
	mgr.SetFFmpegCapabilities(caps)
	log.Info("ffmpeg capabilities", "hlsExtensionPicky", caps.HLSExtensionPicky, "caFile", caps.CAFile, "tlsVerify", cfg.FFmpegTLSVerify)

	mon := sysmon.New(cfg.SysmonInterval, cfg.DataDir, mgr.FFmpegProcessCount)
	mon.OnSample = met.UpdateSystem
	mon.Sample() // populate disk numbers before the first reconcile
	mgr.SetDiskFree(func() (uint64, bool) {
		s := mon.Snapshot()
		return s.Disk.Free, s.Disk.Total > 0
	})

	// Resume/queue whatever the previous run left behind before anything else.
	mgr.Recover()

	srv := httpapi.New(cfg.HTTPAddr, httpapi.Deps{
		Version:            version,
		StartedAt:          startedAt,
		Registry:           met.Registry,
		APIToken:           cfg.APIToken,
		MetricsRequireAuth: cfg.MetricsRequireAuth,
		Log:                log.With("component", "http"),
		Health:             mgr.Health,
		Ready:              mgr.Ready,
		State:              mgr.State,
		Schedule:           mgr.ScheduleView,
		Recordings:         func() any { return mgr.Recordings() },
		System:             func() any { return mon.Snapshot() },
	})
	httpErr := make(chan error, 1)
	go func() {
		log.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()

	if s3up != nil {
		go func() {
			if err := s3up.Check(ctx); err != nil {
				log.Error("S3 bucket check failed (uploads will keep retrying)", "bucket", cfg.S3Bucket,
					"endpoint", cfg.S3Endpoint, "error", err)
				return
			}
			log.Info("S3 bucket reachable", "bucket", cfg.S3Bucket, "endpoint", cfg.S3Endpoint)
			if n, err := s3up.CleanupStaleMultipartUploads(ctx, time.Hour); err != nil {
				log.Warn("could not clean up stale multipart uploads", "error", err)
			} else if n > 0 {
				log.Info("aborted stale multipart uploads", "count", n)
			}
		}()
	}

	fetcher := schedule.NewFetcher(cfg.ScheduleURL, cfg.ScheduleFetchTimeout, cfg.ScheduleAuthHeader,
		"spectado-stream-recorder/"+version, cfg.DataDir, cfg.ScheduleLocation)

	go mon.Run(ctx)
	go mgr.RunSchedulePoller(ctx, fetcher)
	go mgr.RunReconcile(ctx)
	go mgr.RunPersister(ctx)
	go mgr.RunRetention(ctx)

	var fatal error
	select {
	case <-ctx.Done():
		log.Info("shutdown signal received", "timeout", cfg.ShutdownTimeout.String())
	case err := <-httpErr:
		log.Error("http server failed", "error", err)
		fatal = fmt.Errorf("http server: %w", err)
		stop()
	}
	// A second signal during the graceful shutdown forces an immediate exit.
	stop()
	force := make(chan os.Signal, 1)
	signal.Notify(force, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-force
		log.Error("second signal received: forcing exit (recordings resume from their sidecars on next start)")
		os.Exit(130)
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout-5*time.Second)
	defer cancel()
	mgr.Shutdown(shutdownCtx)
	httpCtx, cancelHTTP := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelHTTP()
	_ = srv.Shutdown(httpCtx)
	log.Info("stopped")
	return fatal
}

// prepareDataDir creates the data directory and verifies it is writable,
// printing an actionable message otherwise (typical cause: bind mount owned
// by another uid).
func prepareDataDir(dir string) error {
	if err := os.MkdirAll(filepath.Join(dir, "recordings"), 0o755); err != nil {
		return fmt.Errorf("DATA_DIR %s is not writable (uid %d, gid %d): %w — fix ownership, e.g. `chown 10001:10001 <dir>`",
			dir, os.Getuid(), os.Getgid(), err)
	}
	probe := filepath.Join(dir, ".write-test")
	if err := os.WriteFile(probe, []byte("ok"), 0o644); err != nil {
		return fmt.Errorf("DATA_DIR %s is not writable (uid %d, gid %d): %w — fix ownership, e.g. `chown 10001:10001 <dir>`",
			dir, os.Getuid(), os.Getgid(), err)
	}
	_ = os.Remove(probe)
	return nil
}

// lockDataDir takes an exclusive lock so two recorders never share a data
// directory (they would append to the same files and double-upload).
func lockDataDir(dir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dir, "recorder.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another recorder instance is already using DATA_DIR %s (lock held): %w", dir, err)
	}
	_, _ = f.WriteString(fmt.Sprintf("%d\n", os.Getpid()))
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

func checkFFmpeg(ctx context.Context, path string) (resolved, ver string, err error) {
	resolved, err = exec.LookPath(path)
	if err != nil {
		return "", "", fmt.Errorf("%s not found (set FFMPEG_PATH/FFPROBE_PATH): %w", path, err)
	}
	ver, err = recorder.FFmpegVersion(ctx, resolved)
	if err != nil {
		return "", "", fmt.Errorf("%s -version failed: %w", resolved, err)
	}
	return resolved, ver, nil
}

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func newLogger(cfg *config.Config) *slog.Logger {
	var level slog.Level
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if cfg.LogFormat == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(h)
}
