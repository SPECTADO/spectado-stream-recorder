package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// envKeys lists every variable Load reads. Each test resets all of them so
// the developer's shell environment cannot leak into the assertions.
var envKeys = []string{
	"SCHEDULE_URL", "SCHEDULE_POLL_INTERVAL", "SCHEDULE_FETCH_TIMEOUT", "SCHEDULE_AUTH_HEADER", "SCHEDULE_DEFAULT_TZ",
	"DATA_DIR", "RETENTION_UPLOADED", "MIN_FREE_DISK",
	"HTTP_ADDR", "API_TOKEN", "METRICS_REQUIRE_AUTH",
	"S3_ENDPOINT", "S3_REGION", "S3_BUCKET", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "S3_PREFIX",
	"S3_FORCE_PATH_STYLE", "S3_CHECKSUM_ALGORITHM", "S3_CONDITIONAL_PUT",
	"UPLOAD_CONCURRENCY", "UPLOAD_PART_SIZE", "UPLOAD_BACKOFF_MAX", "UPLOAD_DISABLED",
	"RECORD_START_EARLY", "RECORD_STOP_LATE", "MAX_SESSION_DURATION", "MAX_RECORDINGS",
	"FFMPEG_PATH", "FFPROBE_PATH", "FFPROBE_TIMEOUT", "PROBE_CONCURRENCY", "AUDIO_CODEC", "AUDIO_BITRATE",
	"FFMPEG_USER_AGENT", "FFMPEG_RW_TIMEOUT", "FFMPEG_STALL_TIMEOUT", "FFMPEG_RESTART_BACKOFF_MIN",
	"FFMPEG_RESTART_BACKOFF_MAX", "FFMPEG_STOP_GRACE", "FFMPEG_STDERR_LOG", "FFMPEG_TLS_VERIFY",
	"SHUTDOWN_TIMEOUT", "SYSMON_INTERVAL", "LOG_LEVEL", "LOG_FORMAT",
}

const testURL = "https://example.com/schedule.json"

// setEnv clears every known variable (empty values fall back to defaults in
// Load) and then applies vars.
func setEnv(t *testing.T, vars map[string]string) {
	t.Helper()
	for _, k := range envKeys {
		t.Setenv(k, "")
	}
	for k, v := range vars {
		if !slices.Contains(envKeys, k) {
			t.Fatalf("test bug: %q is not a known env key", k)
		}
		t.Setenv(k, v)
	}
}

// localEnv is the minimal valid configuration (no uploads) plus overrides.
func localEnv(extra map[string]string) map[string]string {
	m := map[string]string{"SCHEDULE_URL": testURL, "UPLOAD_DISABLED": "true"}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func mustLoad(t *testing.T) *Config {
	t.Helper()
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return c
}

func mustFail(t *testing.T, want ...string) error {
	t.Helper()
	c, err := Load()
	if err == nil {
		t.Fatalf("Load = %+v, want error containing %q", c, want)
	}
	for _, w := range want {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("error does not mention %q:\n%v", w, err)
		}
	}
	return err
}

func TestLoad_MissingScheduleURL(t *testing.T) {
	setEnv(t, map[string]string{"UPLOAD_DISABLED": "true"})
	mustFail(t, "SCHEDULE_URL is required")

	setEnv(t, map[string]string{"UPLOAD_DISABLED": "true", "SCHEDULE_URL": "   "})
	mustFail(t, "SCHEDULE_URL is required")

	setEnv(t, localEnv(map[string]string{"SCHEDULE_URL": "ftp://example.com/schedule.json"}))
	mustFail(t, "SCHEDULE_URL must start with http:// or https://")

	setEnv(t, localEnv(map[string]string{"SCHEDULE_URL": "example.com/schedule.json"}))
	mustFail(t, "SCHEDULE_URL must start with http:// or https://")
}

func TestLoad_Defaults(t *testing.T) {
	setEnv(t, localEnv(nil))
	c := mustLoad(t)

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"ScheduleURL", c.ScheduleURL, testURL},
		{"SchedulePollInterval", c.SchedulePollInterval, 60 * time.Second},
		{"ScheduleFetchTimeout", c.ScheduleFetchTimeout, 20 * time.Second},
		{"ScheduleAuthHeader", c.ScheduleAuthHeader, ""},
		{"ScheduleDefaultTZ", c.ScheduleDefaultTZ, "UTC"},
		{"DataDir", c.DataDir, "/data"},
		{"RetentionUploaded", c.RetentionUploaded, 24 * time.Hour},
		{"MinFreeDiskBytes", c.MinFreeDiskBytes, int64(2 << 30)},
		{"HTTPAddr", c.HTTPAddr, ":8080"},
		{"APIToken", c.APIToken, ""},
		{"MetricsRequireAuth", c.MetricsRequireAuth, false},
		{"S3Endpoint", c.S3Endpoint, ""},
		{"S3Region", c.S3Region, "auto"},
		{"S3Bucket", c.S3Bucket, ""},
		{"S3Prefix", c.S3Prefix, ""},
		{"S3ForcePathStyle", c.S3ForcePathStyle, true},
		{"S3ChecksumAlgorithm", c.S3ChecksumAlgorithm, "none"},
		{"S3ConditionalPut", c.S3ConditionalPut, true},
		{"UploadConcurrency", c.UploadConcurrency, 4},
		{"UploadPartSize", c.UploadPartSize, int64(16 << 20)},
		{"UploadBackoffMax", c.UploadBackoffMax, 5 * time.Minute},
		{"UploadDisabled", c.UploadDisabled, true},
		{"RecordStartEarly", c.RecordStartEarly, 10 * time.Second},
		{"RecordStopLate", c.RecordStopLate, 30 * time.Second},
		{"MaxSessionDuration", c.MaxSessionDuration, time.Duration(0)},
		{"MaxRecordings", c.MaxRecordings, 0},
		{"FFmpegPath", c.FFmpegPath, "ffmpeg"},
		{"FFprobePath", c.FFprobePath, "ffprobe"},
		{"FFprobeTimeout", c.FFprobeTimeout, 15 * time.Second},
		{"ProbeConcurrency", c.ProbeConcurrency, 32},
		{"AudioCodec", c.AudioCodec, "auto"},
		{"AudioBitrate", c.AudioBitrate, "128k"},
		{"FFmpegUserAgent", c.FFmpegUserAgent, "spectado-stream-recorder/1.0"},
		{"FFmpegRWTimeout", c.FFmpegRWTimeout, 10 * time.Second},
		{"FFmpegStallTimeout", c.FFmpegStallTimeout, 60 * time.Second},
		{"FFmpegRestartBackoffMin", c.FFmpegRestartBackoffMin, time.Second},
		{"FFmpegRestartBackoffMax", c.FFmpegRestartBackoffMax, 30 * time.Second},
		{"FFmpegStopGrace", c.FFmpegStopGrace, 5 * time.Second},
		{"FFmpegStderrLog", c.FFmpegStderrLog, "warn"},
		{"FFmpegTLSVerify", c.FFmpegTLSVerify, false},
		{"ShutdownTimeout", c.ShutdownTimeout, 45 * time.Second},
		{"SysmonInterval", c.SysmonInterval, 10 * time.Second},
		{"LogLevel", c.LogLevel, "info"},
		{"LogFormat", c.LogFormat, "json"},
	}
	for _, ck := range checks {
		if ck.got != ck.want {
			t.Errorf("%s = %v (%T), want %v (%T)", ck.name, ck.got, ck.got, ck.want, ck.want)
		}
	}
	if c.ScheduleLocation == nil || c.ScheduleLocation.String() != "UTC" {
		t.Errorf("ScheduleLocation = %v, want UTC", c.ScheduleLocation)
	}
}

func TestLoad_S3Required(t *testing.T) {
	t.Run("uploads enabled without any S3 settings", func(t *testing.T) {
		setEnv(t, map[string]string{"SCHEDULE_URL": testURL})
		mustFail(t, "S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required", "UPLOAD_DISABLED=true")
	})
	t.Run("uploads disabled needs no S3", func(t *testing.T) {
		setEnv(t, map[string]string{"SCHEDULE_URL": testURL, "UPLOAD_DISABLED": "true"})
		c := mustLoad(t)
		if !c.UploadDisabled {
			t.Error("UploadDisabled = false")
		}
	})
	t.Run("partial S3 reports the missing pieces", func(t *testing.T) {
		setEnv(t, map[string]string{"SCHEDULE_URL": testURL, "S3_ENDPOINT": "https://acc.r2.cloudflarestorage.com"})
		err := mustFail(t, "S3_BUCKET is required", "S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required")
		if strings.Contains(err.Error(), "S3_ENDPOINT is required") || strings.Contains(err.Error(), "UPLOAD_DISABLED=true") {
			t.Errorf("unexpected generic message when S3 is partially configured:\n%v", err)
		}
	})
	t.Run("bucket only", func(t *testing.T) {
		setEnv(t, map[string]string{"SCHEDULE_URL": testURL, "S3_BUCKET": "recordings"})
		mustFail(t, "S3_ENDPOINT is required", "S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required")
	})
	t.Run("secret without key id", func(t *testing.T) {
		setEnv(t, map[string]string{
			"SCHEDULE_URL": testURL, "S3_ENDPOINT": "https://acc.r2.cloudflarestorage.com",
			"S3_BUCKET": "recordings", "S3_SECRET_ACCESS_KEY": "s",
		})
		mustFail(t, "S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required")
	})
	t.Run("endpoint without scheme", func(t *testing.T) {
		setEnv(t, map[string]string{
			"SCHEDULE_URL": testURL, "S3_ENDPOINT": "acc.r2.cloudflarestorage.com",
			"S3_BUCKET": "recordings", "S3_ACCESS_KEY_ID": "k", "S3_SECRET_ACCESS_KEY": "s",
		})
		mustFail(t, "S3_ENDPOINT must start with http:// or https://")
	})
	t.Run("complete S3 configuration", func(t *testing.T) {
		setEnv(t, map[string]string{
			"SCHEDULE_URL":          testURL,
			"S3_ENDPOINT":           "  https://acc.r2.cloudflarestorage.com ",
			"S3_BUCKET":             " recordings ",
			"S3_ACCESS_KEY_ID":      " key ",
			"S3_SECRET_ACCESS_KEY":  " secret ",
			"S3_PREFIX":             "/radio/",
			"S3_REGION":             "eu",
			"S3_FORCE_PATH_STYLE":   "false",
			"S3_CHECKSUM_ALGORITHM": "CRC32C",
			"S3_CONDITIONAL_PUT":    "0",
			"UPLOAD_CONCURRENCY":    "2",
			"UPLOAD_PART_SIZE":      "5MiB",
		})
		c := mustLoad(t)
		if c.UploadDisabled {
			t.Error("UploadDisabled = true")
		}
		if c.S3Endpoint != "https://acc.r2.cloudflarestorage.com" || c.S3Bucket != "recordings" || c.S3AccessKeyID != "key" || c.S3SecretAccessKey != "secret" {
			t.Errorf("S3 values not trimmed: %+v", c)
		}
		if c.S3Prefix != "radio/" {
			t.Errorf("S3Prefix = %q, want radio/", c.S3Prefix)
		}
		if c.S3Region != "eu" || c.S3ForcePathStyle || c.S3ChecksumAlgorithm != "crc32c" || c.S3ConditionalPut {
			t.Errorf("S3 options = region %q pathStyle %v checksum %q conditional %v", c.S3Region, c.S3ForcePathStyle, c.S3ChecksumAlgorithm, c.S3ConditionalPut)
		}
		if c.UploadConcurrency != 2 || c.UploadPartSize != 5<<20 {
			t.Errorf("upload = concurrency %d part %d", c.UploadConcurrency, c.UploadPartSize)
		}
	})
}

func TestLoad_S3PrefixNormalization(t *testing.T) {
	cases := map[string]string{
		"/recordings/": "recordings/",
		"recordings":   "recordings/",
		"recordings/":  "recordings/",
		"":             "",
		"   ":          "",
		"/":            "",
		"///":          "",
		"a/b":          "a/b/",
		"/a/b/":        "a/b/",
		"  a/b  ":      "a/b/",
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			setEnv(t, localEnv(map[string]string{"S3_PREFIX": in}))
			if c := mustLoad(t); c.S3Prefix != want {
				t.Errorf("S3_PREFIX=%q -> %q, want %q", in, c.S3Prefix, want)
			}
		})
	}
}

func TestLoad_InvalidValues(t *testing.T) {
	cases := []struct {
		key, value string
		want       string
	}{
		{"SCHEDULE_POLL_INTERVAL", "1s", "SCHEDULE_POLL_INTERVAL must be at least 5s"},
		{"SCHEDULE_POLL_INTERVAL", "4", "SCHEDULE_POLL_INTERVAL must be at least 5s"},
		{"SCHEDULE_POLL_INTERVAL", "soon", `SCHEDULE_POLL_INTERVAL: invalid duration "soon"`},
		{"SCHEDULE_FETCH_TIMEOUT", "1.5", "SCHEDULE_FETCH_TIMEOUT: invalid duration"},
		{"SCHEDULE_DEFAULT_TZ", "Mars/Olympus", `SCHEDULE_DEFAULT_TZ: unknown time zone "Mars/Olympus"`},
		{"AUDIO_CODEC", "mp3", `AUDIO_CODEC must be auto, aac or copy (got "mp3")`},
		{"UPLOAD_PART_SIZE", "1MiB", "UPLOAD_PART_SIZE must be between 5MiB and 5GiB"},
		{"UPLOAD_PART_SIZE", "6GiB", "UPLOAD_PART_SIZE must be between 5MiB and 5GiB"},
		{"UPLOAD_PART_SIZE", "lots", `UPLOAD_PART_SIZE: invalid size "LOTS"`},
		{"UPLOAD_CONCURRENCY", "0", "UPLOAD_CONCURRENCY must be >= 1"},
		{"UPLOAD_CONCURRENCY", "four", `UPLOAD_CONCURRENCY: invalid integer "four"`},
		{"S3_CHECKSUM_ALGORITHM", "sha1", `S3_CHECKSUM_ALGORITHM must be none, crc32 or crc32c (got "sha1")`},
		{"LOG_FORMAT", "xml", `LOG_FORMAT must be json or text (got "xml")`},
		{"FFMPEG_STDERR_LOG", "loud", `FFMPEG_STDERR_LOG must be warn, debug or off (got "loud")`},
		{"MAX_SESSION_DURATION", "30s", "MAX_SESSION_DURATION must be 0 or at least 1m"},
		{"FFMPEG_STALL_TIMEOUT", "2s", "FFMPEG_STALL_TIMEOUT must be at least 5s"},
		{"FFMPEG_RW_TIMEOUT", "500ms", "FFMPEG_RW_TIMEOUT must be at least 1s"},
		{"FFMPEG_STOP_GRACE", "0", "FFMPEG_STOP_GRACE must be at least 1s"},
		{"FFMPEG_RESTART_BACKOFF_MIN", "0", "FFMPEG_RESTART_BACKOFF_MIN/MAX must be positive and MIN <= MAX"},
		{"FFMPEG_RESTART_BACKOFF_MAX", "500ms", "FFMPEG_RESTART_BACKOFF_MIN/MAX must be positive and MIN <= MAX"},
		{"PROBE_CONCURRENCY", "0", "PROBE_CONCURRENCY must be >= 1"},
		{"RECORD_START_EARLY", "-5s", "RECORD_START_EARLY and RECORD_STOP_LATE must not be negative"},
		{"RECORD_STOP_LATE", "-1", "RECORD_START_EARLY and RECORD_STOP_LATE must not be negative"},
		{"SHUTDOWN_TIMEOUT", "5s", "SHUTDOWN_TIMEOUT must be at least 10s"},
		{"MIN_FREE_DISK", "abc", `MIN_FREE_DISK: invalid size "ABC"`},
		{"MIN_FREE_DISK", "-1GiB", "MIN_FREE_DISK: invalid size"},
		{"METRICS_REQUIRE_AUTH", "yes", `METRICS_REQUIRE_AUTH: invalid boolean "yes"`},
		{"S3_FORCE_PATH_STYLE", "maybe", `S3_FORCE_PATH_STYLE: invalid boolean "maybe"`},
		{"FFMPEG_TLS_VERIFY", "on", `FFMPEG_TLS_VERIFY: invalid boolean "on"`},
		{"MAX_RECORDINGS", "many", `MAX_RECORDINGS: invalid integer "many"`},
	}
	for _, tc := range cases {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			setEnv(t, localEnv(map[string]string{tc.key: tc.value}))
			mustFail(t, tc.want)
		})
	}

	t.Run("all errors are reported together", func(t *testing.T) {
		setEnv(t, localEnv(map[string]string{"AUDIO_CODEC": "mp3", "LOG_FORMAT": "xml", "SCHEDULE_POLL_INTERVAL": "1s"}))
		err := mustFail(t, "AUDIO_CODEC", "LOG_FORMAT", "SCHEDULE_POLL_INTERVAL")
		if n := strings.Count(err.Error(), "\n"); n < 2 {
			t.Errorf("joined error has %d newlines, want at least 2:\n%v", n, err)
		}
	})

	t.Run("invalid UPLOAD_DISABLED falls back to uploads enabled", func(t *testing.T) {
		setEnv(t, map[string]string{"SCHEDULE_URL": testURL, "UPLOAD_DISABLED": "maybe"})
		mustFail(t, `UPLOAD_DISABLED: invalid boolean "maybe"`, "S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required")
	})
}

func TestLoad_Durations(t *testing.T) {
	setEnv(t, localEnv(map[string]string{
		"SCHEDULE_POLL_INTERVAL": "120",
		"SCHEDULE_FETCH_TIMEOUT": " 30 ",
		"RECORD_STOP_LATE":       "45",
		"RECORD_START_EARLY":     "0",
		"MAX_SESSION_DURATION":   "1h30m",
		"RETENTION_UPLOADED":     "48h",
		"FFMPEG_STALL_TIMEOUT":   "5s",
		"UPLOAD_BACKOFF_MAX":     "90",
	}))
	c := mustLoad(t)
	checks := map[string][2]time.Duration{
		"SchedulePollInterval": {c.SchedulePollInterval, 2 * time.Minute},
		"ScheduleFetchTimeout": {c.ScheduleFetchTimeout, 30 * time.Second},
		"RecordStopLate":       {c.RecordStopLate, 45 * time.Second},
		"RecordStartEarly":     {c.RecordStartEarly, 0},
		"MaxSessionDuration":   {c.MaxSessionDuration, 90 * time.Minute},
		"RetentionUploaded":    {c.RetentionUploaded, 48 * time.Hour},
		"FFmpegStallTimeout":   {c.FFmpegStallTimeout, 5 * time.Second},
		"UploadBackoffMax":     {c.UploadBackoffMax, 90 * time.Second},
	}
	for name, v := range checks {
		if v[0] != v[1] {
			t.Errorf("%s = %v, want %v", name, v[0], v[1])
		}
	}
}

func TestLoad_ValuesAndNormalization(t *testing.T) {
	setEnv(t, localEnv(map[string]string{
		"SCHEDULE_URL":          "  https://example.com/s.json  ",
		"SCHEDULE_AUTH_HEADER":  "X-Token: abc",
		"SCHEDULE_DEFAULT_TZ":   "Europe/Prague",
		"AUDIO_CODEC":           " AAC ",
		"AUDIO_BITRATE":         "192k",
		"S3_CHECKSUM_ALGORITHM": "CRC32",
		"FFMPEG_STDERR_LOG":     "DEBUG",
		"LOG_LEVEL":             "Debug",
		"LOG_FORMAT":            "TEXT",
		"API_TOKEN":             "s3cret",
		"METRICS_REQUIRE_AUTH":  "true",
		"FFMPEG_TLS_VERIFY":     "1",
		"MIN_FREE_DISK":         "512MiB",
		"MAX_RECORDINGS":        "12",
		"DATA_DIR":              "/var/lib/recorder",
		"HTTP_ADDR":             "127.0.0.1:9090",
	}))
	if _, err := time.LoadLocation("Europe/Prague"); err != nil {
		t.Skipf("time zone database unavailable: %v", err)
	}
	c := mustLoad(t)
	if c.ScheduleURL != "https://example.com/s.json" {
		t.Errorf("ScheduleURL = %q, want trimmed", c.ScheduleURL)
	}
	if c.ScheduleAuthHeader != "X-Token: abc" {
		t.Errorf("ScheduleAuthHeader = %q", c.ScheduleAuthHeader)
	}
	if c.ScheduleDefaultTZ != "Europe/Prague" || c.ScheduleLocation == nil || c.ScheduleLocation.String() != "Europe/Prague" {
		t.Errorf("tz = %q loc = %v", c.ScheduleDefaultTZ, c.ScheduleLocation)
	}
	if c.AudioCodec != "aac" || c.AudioBitrate != "192k" {
		t.Errorf("audio = %q %q", c.AudioCodec, c.AudioBitrate)
	}
	if c.S3ChecksumAlgorithm != "crc32" || c.FFmpegStderrLog != "debug" || c.LogLevel != "debug" || c.LogFormat != "text" {
		t.Errorf("lower-casing: checksum %q stderr %q level %q format %q", c.S3ChecksumAlgorithm, c.FFmpegStderrLog, c.LogLevel, c.LogFormat)
	}
	if c.APIToken != "s3cret" || !c.MetricsRequireAuth || !c.FFmpegTLSVerify {
		t.Errorf("token %q metricsAuth %v tlsVerify %v", c.APIToken, c.MetricsRequireAuth, c.FFmpegTLSVerify)
	}
	if c.MinFreeDiskBytes != 512<<20 || c.MaxRecordings != 12 {
		t.Errorf("MinFreeDiskBytes %d MaxRecordings %d", c.MinFreeDiskBytes, c.MaxRecordings)
	}
	if c.DataDir != "/var/lib/recorder" || c.HTTPAddr != "127.0.0.1:9090" {
		t.Errorf("DataDir %q HTTPAddr %q", c.DataDir, c.HTTPAddr)
	}
}

func TestParseBytes(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"16MiB", 16 << 20, false},
		{"16MB", 16 << 20, false},
		{"16M", 16 << 20, false},
		{"16mib", 16 << 20, false},
		{"16 MiB", 16 << 20, false},
		{" 16MiB ", 16 << 20, false},
		{"512k", 512 << 10, false},
		{"512K", 512 << 10, false},
		{"512KB", 512 << 10, false},
		{"512KiB", 512 << 10, false},
		{"1.5GiB", 1610612736, false},
		{"2GiB", 2 << 30, false},
		{"2GB", 2 << 30, false},
		{"2G", 2 << 30, false},
		{"12345", 12345, false},
		{"100B", 100, false},
		{"0", 0, false},
		{"1.5", 1, false},
		{"", 0, true},
		{"   ", 0, true},
		{"abc", 0, true},
		{"MiB", 0, true},
		{"-1", 0, true},
		{"-5MiB", 0, true},
		{"1TiB", 0, true},
		{"16 Mi B", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseBytes(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseBytes(%q) = %d, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseBytes(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("ParseBytes(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizePrefix(t *testing.T) {
	cases := map[string]string{
		"":             "",
		"   ":          "",
		"/":            "",
		"///":          "",
		"recordings":   "recordings/",
		"/recordings/": "recordings/",
		"recordings/":  "recordings/",
		"//recordings": "recordings/",
		"a/b":          "a/b/",
		"/a/b/":        "a/b/",
		"  a/b/  ":     "a/b/",
		"a//b":         "a//b/",
	}
	for in, want := range cases {
		if got := NormalizePrefix(in); got != want {
			t.Errorf("NormalizePrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseDuration(t *testing.T) {
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"90s", 90 * time.Second, false},
		{"5m", 5 * time.Minute, false},
		{"1h30m", 90 * time.Minute, false},
		{"250ms", 250 * time.Millisecond, false},
		{"120", 2 * time.Minute, false},
		{" 7 ", 7 * time.Second, false},
		{"0", 0, false},
		{"-5", -5 * time.Second, false},
		{"-5s", -5 * time.Second, false},
		{"1.5", 0, true},
		{"", 0, true},
		{"abc", 0, true},
		{"5 m", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseDuration(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseDuration(%q) = %v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDuration(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("ParseDuration(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
