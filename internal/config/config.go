// Package config loads the recorder configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the fully resolved runtime configuration.
type Config struct {
	// Schedule source.
	ScheduleURL          string
	SchedulePollInterval time.Duration
	ScheduleFetchTimeout time.Duration
	ScheduleAuthHeader   string         // optional raw "Name: value" header sent with schedule requests
	ScheduleDefaultTZ    string         // zone applied to timestamps without an offset
	ScheduleLocation     *time.Location // resolved ScheduleDefaultTZ

	// Local storage.
	DataDir           string
	RetentionUploaded time.Duration // how long uploaded/failed sidecars stay visible in the API
	MinFreeDiskBytes  int64         // refuse to start new recordings below this

	// HTTP.
	HTTPAddr           string
	APIToken           string // optional bearer token protecting /api/*
	MetricsRequireAuth bool   // also protect /metrics with APIToken

	// S3 / R2.
	S3Endpoint          string
	S3Region            string
	S3Bucket            string
	S3AccessKeyID       string
	S3SecretAccessKey   string
	S3Prefix            string // normalized: no leading '/', trailing '/' when non-empty
	S3ForcePathStyle    bool
	S3ChecksumAlgorithm string // none | crc32 | crc32c
	S3ConditionalPut    bool   // use If-None-Match: * to never overwrite objects silently
	UploadConcurrency   int
	UploadPartSize      int64
	UploadBackoffMax    time.Duration
	UploadMinThroughput int64 // bytes/s assumed when computing the per-attempt upload deadline
	UploadDisabled      bool  // keep recordings locally (testing)

	// Recording.
	RecordStartEarly        time.Duration
	RecordStopLate          time.Duration
	MaxSessionDuration      time.Duration // 0 = never rotate
	MaxRecordings           int           // hard cap on simultaneous recordings (0 = unlimited)
	FFmpegPath              string
	FFprobePath             string
	FFprobeTimeout          time.Duration
	ProbeConcurrency        int
	AudioCodec              string // auto | aac | copy
	AudioBitrate            string
	FFmpegUserAgent         string
	FFmpegRWTimeout         time.Duration // network read/write timeout passed to ffmpeg/ffprobe
	FFmpegStallTimeout      time.Duration
	FFmpegRestartBackoffMin time.Duration
	FFmpegRestartBackoffMax time.Duration
	FFmpegStopGrace         time.Duration
	FFmpegStderrLog         string // warn | debug | off
	FFmpegTLSVerify         bool   // verify TLS certificates of stream sources

	// Lifecycle / monitoring / logging.
	ShutdownTimeout time.Duration
	SysmonInterval  time.Duration
	LogLevel        string
	LogFormat       string // json | text
}

// Load reads configuration from the environment and validates it.
func Load() (*Config, error) {
	var errs []error
	c := &Config{}

	c.ScheduleURL = strings.TrimSpace(os.Getenv("SCHEDULE_URL"))
	c.SchedulePollInterval = envDuration(&errs, "SCHEDULE_POLL_INTERVAL", 60*time.Second)
	c.ScheduleFetchTimeout = envDuration(&errs, "SCHEDULE_FETCH_TIMEOUT", 20*time.Second)
	c.ScheduleAuthHeader = os.Getenv("SCHEDULE_AUTH_HEADER")
	c.ScheduleDefaultTZ = envString("SCHEDULE_DEFAULT_TZ", "UTC")

	c.DataDir = envString("DATA_DIR", "/data")
	c.RetentionUploaded = envDuration(&errs, "RETENTION_UPLOADED", 24*time.Hour)
	c.MinFreeDiskBytes = envBytes(&errs, "MIN_FREE_DISK", 2*1024*1024*1024)

	c.HTTPAddr = envString("HTTP_ADDR", ":8080")
	c.APIToken = os.Getenv("API_TOKEN")
	c.MetricsRequireAuth = envBool(&errs, "METRICS_REQUIRE_AUTH", false)

	c.S3Endpoint = strings.TrimSpace(os.Getenv("S3_ENDPOINT"))
	c.S3Region = envString("S3_REGION", "auto")
	c.S3Bucket = strings.TrimSpace(os.Getenv("S3_BUCKET"))
	c.S3AccessKeyID = strings.TrimSpace(os.Getenv("S3_ACCESS_KEY_ID"))
	c.S3SecretAccessKey = strings.TrimSpace(os.Getenv("S3_SECRET_ACCESS_KEY"))
	c.S3Prefix = NormalizePrefix(os.Getenv("S3_PREFIX"))
	c.S3ForcePathStyle = envBool(&errs, "S3_FORCE_PATH_STYLE", true)
	c.S3ChecksumAlgorithm = strings.ToLower(envString("S3_CHECKSUM_ALGORITHM", "none"))
	c.S3ConditionalPut = envBool(&errs, "S3_CONDITIONAL_PUT", true)
	c.UploadConcurrency = envInt(&errs, "UPLOAD_CONCURRENCY", 4)
	c.UploadPartSize = envBytes(&errs, "UPLOAD_PART_SIZE", 16*1024*1024)
	c.UploadBackoffMax = envDuration(&errs, "UPLOAD_BACKOFF_MAX", 5*time.Minute)
	c.UploadMinThroughput = envBytes(&errs, "UPLOAD_MIN_THROUGHPUT", 128*1024)
	c.UploadDisabled = envBool(&errs, "UPLOAD_DISABLED", false)

	c.RecordStartEarly = envDuration(&errs, "RECORD_START_EARLY", 10*time.Second)
	c.RecordStopLate = envDuration(&errs, "RECORD_STOP_LATE", 30*time.Second)
	c.MaxSessionDuration = envDuration(&errs, "MAX_SESSION_DURATION", 0)
	c.MaxRecordings = envInt(&errs, "MAX_RECORDINGS", 0)
	c.FFmpegPath = envString("FFMPEG_PATH", "ffmpeg")
	c.FFprobePath = envString("FFPROBE_PATH", "ffprobe")
	c.FFprobeTimeout = envDuration(&errs, "FFPROBE_TIMEOUT", 15*time.Second)
	c.ProbeConcurrency = envInt(&errs, "PROBE_CONCURRENCY", 32)
	c.AudioCodec = strings.ToLower(envString("AUDIO_CODEC", "auto"))
	c.AudioBitrate = envString("AUDIO_BITRATE", "128k")
	c.FFmpegUserAgent = envString("FFMPEG_USER_AGENT", "spectado-stream-recorder/1.0")
	c.FFmpegRWTimeout = envDuration(&errs, "FFMPEG_RW_TIMEOUT", 10*time.Second)
	c.FFmpegStallTimeout = envDuration(&errs, "FFMPEG_STALL_TIMEOUT", 60*time.Second)
	c.FFmpegRestartBackoffMin = envDuration(&errs, "FFMPEG_RESTART_BACKOFF_MIN", time.Second)
	c.FFmpegRestartBackoffMax = envDuration(&errs, "FFMPEG_RESTART_BACKOFF_MAX", 30*time.Second)
	c.FFmpegStopGrace = envDuration(&errs, "FFMPEG_STOP_GRACE", 5*time.Second)
	c.FFmpegStderrLog = strings.ToLower(envString("FFMPEG_STDERR_LOG", "warn"))
	c.FFmpegTLSVerify = envBool(&errs, "FFMPEG_TLS_VERIFY", false)

	c.ShutdownTimeout = envDuration(&errs, "SHUTDOWN_TIMEOUT", 45*time.Second)
	c.SysmonInterval = envDuration(&errs, "SYSMON_INTERVAL", 10*time.Second)
	c.LogLevel = strings.ToLower(envString("LOG_LEVEL", "info"))
	c.LogFormat = strings.ToLower(envString("LOG_FORMAT", "json"))

	// Validation.
	if c.ScheduleURL == "" {
		errs = append(errs, errors.New("SCHEDULE_URL is required"))
	} else if !strings.HasPrefix(c.ScheduleURL, "http://") && !strings.HasPrefix(c.ScheduleURL, "https://") {
		errs = append(errs, errors.New("SCHEDULE_URL must start with http:// or https://"))
	}
	if c.SchedulePollInterval < 5*time.Second {
		errs = append(errs, errors.New("SCHEDULE_POLL_INTERVAL must be at least 5s"))
	}
	if loc, err := time.LoadLocation(c.ScheduleDefaultTZ); err != nil {
		errs = append(errs, fmt.Errorf("SCHEDULE_DEFAULT_TZ: unknown time zone %q", c.ScheduleDefaultTZ))
	} else {
		c.ScheduleLocation = loc
	}
	switch c.AudioCodec {
	case "auto", "aac", "copy":
	default:
		errs = append(errs, fmt.Errorf("AUDIO_CODEC must be auto, aac or copy (got %q)", c.AudioCodec))
	}
	switch c.S3ChecksumAlgorithm {
	case "none", "crc32", "crc32c":
	default:
		errs = append(errs, fmt.Errorf("S3_CHECKSUM_ALGORITHM must be none, crc32 or crc32c (got %q)", c.S3ChecksumAlgorithm))
	}
	switch c.FFmpegStderrLog {
	case "warn", "debug", "off":
	default:
		errs = append(errs, fmt.Errorf("FFMPEG_STDERR_LOG must be warn, debug or off (got %q)", c.FFmpegStderrLog))
	}
	if c.UploadConcurrency < 1 {
		errs = append(errs, errors.New("UPLOAD_CONCURRENCY must be >= 1"))
	}
	if c.UploadPartSize < 5*1024*1024 || c.UploadPartSize > 5*1024*1024*1024 {
		errs = append(errs, errors.New("UPLOAD_PART_SIZE must be between 5MiB and 5GiB"))
	}
	if c.FFmpegRestartBackoffMin <= 0 || c.FFmpegRestartBackoffMax < c.FFmpegRestartBackoffMin {
		errs = append(errs, errors.New("FFMPEG_RESTART_BACKOFF_MIN/MAX must be positive and MIN <= MAX"))
	}
	if c.FFmpegStallTimeout < 5*time.Second {
		errs = append(errs, errors.New("FFMPEG_STALL_TIMEOUT must be at least 5s"))
	}
	if c.FFmpegRWTimeout < time.Second {
		errs = append(errs, errors.New("FFMPEG_RW_TIMEOUT must be at least 1s"))
	}
	if c.FFmpegStopGrace < time.Second {
		errs = append(errs, errors.New("FFMPEG_STOP_GRACE must be at least 1s"))
	}
	if c.ProbeConcurrency < 1 {
		errs = append(errs, errors.New("PROBE_CONCURRENCY must be >= 1"))
	}
	if c.MaxSessionDuration != 0 && c.MaxSessionDuration < time.Minute {
		errs = append(errs, errors.New("MAX_SESSION_DURATION must be 0 or at least 1m"))
	}
	if c.RecordStartEarly < 0 || c.RecordStopLate < 0 {
		errs = append(errs, errors.New("RECORD_START_EARLY and RECORD_STOP_LATE must not be negative"))
	}
	if c.ShutdownTimeout < 10*time.Second {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT must be at least 10s"))
	}
	if c.MaxRecordings < 0 {
		errs = append(errs, errors.New("MAX_RECORDINGS must not be negative"))
	}
	if c.UploadBackoffMax < 5*time.Second || c.RetentionUploaded < time.Minute || c.SysmonInterval < time.Second ||
		c.FFprobeTimeout < time.Second || c.ScheduleFetchTimeout < time.Second {
		errs = append(errs, errors.New("UPLOAD_BACKOFF_MAX (>=5s), RETENTION_UPLOADED (>=1m), SYSMON_INTERVAL (>=1s), FFPROBE_TIMEOUT (>=1s) and SCHEDULE_FETCH_TIMEOUT (>=1s) must be positive"))
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "warning", "error":
	default:
		errs = append(errs, fmt.Errorf("LOG_LEVEL must be debug, info, warn or error (got %q)", c.LogLevel))
	}
	if !ValidBitrate(c.AudioBitrate) {
		errs = append(errs, fmt.Errorf("AUDIO_BITRATE must look like 128k or 96000 (got %q)", c.AudioBitrate))
	}
	if c.ScheduleAuthHeader != "" {
		name, _, ok := strings.Cut(c.ScheduleAuthHeader, ":")
		if !ok || strings.TrimSpace(name) == "" || strings.ContainsAny(c.ScheduleAuthHeader, "\r\n") {
			errs = append(errs, errors.New(`SCHEDULE_AUTH_HEADER must have the form "Name: value"`))
		}
	}
	if c.MetricsRequireAuth && c.APIToken == "" {
		errs = append(errs, errors.New("METRICS_REQUIRE_AUTH=true requires API_TOKEN"))
	}
	if c.UploadMinThroughput < 1024 {
		errs = append(errs, errors.New("UPLOAD_MIN_THROUGHPUT must be at least 1KiB (bytes per second)"))
	}
	switch c.LogFormat {
	case "json", "text":
	default:
		errs = append(errs, fmt.Errorf("LOG_FORMAT must be json or text (got %q)", c.LogFormat))
	}

	s3Configured := c.S3Endpoint != "" || c.S3Bucket != "" || c.S3AccessKeyID != "" || c.S3SecretAccessKey != ""
	if !c.UploadDisabled {
		if !s3Configured {
			errs = append(errs, errors.New("S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required (or set UPLOAD_DISABLED=true to keep recordings locally)"))
		} else {
			if c.S3Endpoint == "" {
				errs = append(errs, errors.New("S3_ENDPOINT is required"))
			} else if !strings.HasPrefix(c.S3Endpoint, "http://") && !strings.HasPrefix(c.S3Endpoint, "https://") {
				errs = append(errs, errors.New("S3_ENDPOINT must start with http:// or https://"))
			}
			if c.S3Bucket == "" {
				errs = append(errs, errors.New("S3_BUCKET is required"))
			}
			if c.S3AccessKeyID == "" || c.S3SecretAccessKey == "" {
				errs = append(errs, errors.New("S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required"))
			}
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return c, nil
}

// ValidBitrate accepts ffmpeg bitrate syntax: digits with an optional k/M suffix.
func ValidBitrate(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimSuffix(s, "k")
	s = strings.TrimSuffix(s, "m")
	if s == "" {
		return false
	}
	n, err := strconv.ParseFloat(s, 64)
	return err == nil && n > 0
}

// NormalizePrefix trims leading slashes and guarantees a trailing slash for a
// non-empty prefix, so "recordings" and "/recordings/" both mean "recordings/".
func NormalizePrefix(p string) string {
	p = strings.Trim(strings.TrimSpace(p), "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

func envString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func envDuration(errs *[]error, key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	d, err := ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: invalid duration %q", key, v))
		return def
	}
	return d
}

// ParseDuration parses Go durations ("90s", "5m", "1h30m"); plain integers
// are interpreted as seconds.
func ParseDuration(v string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second, nil
	}
	return time.ParseDuration(v)
}

func envInt(errs *[]error, key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: invalid integer %q", key, v))
		return def
	}
	return n
}

func envBool(errs *[]error, key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: invalid boolean %q", key, v))
		return def
	}
	return b
}

// envBytes parses sizes like "16MiB", "16MB", "16M", "16777216".
func envBytes(errs *[]error, key string, def int64) int64 {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	n, err := ParseBytes(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %v", key, err))
		return def
	}
	return n
}

// ParseBytes parses a human readable byte size. Both binary (KiB, MiB, GiB)
// and short (K, M, G, KB, MB, GB) suffixes are treated as powers of 1024.
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, errors.New("empty size")
	}
	mult := int64(1)
	suffixes := []struct {
		suf  string
		mult int64
	}{
		{"GIB", 1 << 30}, {"GB", 1 << 30}, {"G", 1 << 30},
		{"MIB", 1 << 20}, {"MB", 1 << 20}, {"M", 1 << 20},
		{"KIB", 1 << 10}, {"KB", 1 << 10}, {"K", 1 << 10},
		{"B", 1},
	}
	for _, sfx := range suffixes {
		if strings.HasSuffix(s, sfx.suf) {
			mult = sfx.mult
			s = strings.TrimSpace(strings.TrimSuffix(s, sfx.suf))
			break
		}
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return int64(n * float64(mult)), nil
}
