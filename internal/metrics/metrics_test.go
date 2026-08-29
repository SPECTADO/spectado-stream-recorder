package metrics

import (
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/spectado/stream-recorder/internal/sysmon"
)

func gather(t *testing.T, m *Metrics) map[string]*dto.MetricFamily {
	t.Helper()
	fams, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string]*dto.MetricFamily{}
	for _, f := range fams {
		out[f.GetName()] = f
	}
	return out
}

func TestNewRegistersEverythingOnce(t *testing.T) {
	m := New("1.2.3")
	fams := gather(t, m)
	for _, name := range []string{
		"recorder_build_info", "recorder_schedule_fetch_total", "recorder_recordings_active",
		"recorder_upload_duration_seconds", "recorder_cgroup_cpu_usage_seconds_total", "recorder_disk_low",
		"go_goroutines", "process_open_fds",
	} {
		if _, ok := fams[name]; !ok {
			t.Errorf("metric %s not registered", name)
		}
	}
	bi := fams["recorder_build_info"].GetMetric()[0]
	var version string
	for _, l := range bi.GetLabel() {
		if l.GetName() == "version" {
			version = l.GetValue()
		}
	}
	if version != "1.2.3" || bi.GetGauge().GetValue() != 1 {
		t.Fatalf("build_info = %v", bi)
	}
	// A second registry must not collide with the first (no global state).
	_ = New("other")
}

func TestUpdateSystemAndForget(t *testing.T) {
	m := New("t")
	m.UpdateSystem(sysmon.Snapshot{
		CPUPercent: 12.5, CPUCount: 4,
		Memory:    sysmon.Memory{Total: 100, Used: 40, Available: 60, Percent: 40},
		Processes: sysmon.Processes{Total: 7, FFmpeg: 3},
		Disk:      sysmon.Disk{Path: "/data", Total: 1000, Free: 250, Percent: 75},
		Cgroup: &sysmon.Cgroup{MemoryLimit: 4096, MemoryUsage: 2048, CPUQuotaCores: 2, CPUUsageCores: 0.5,
			CPUUsageSeconds: 123.5, ThrottledSeconds: 1.5, ThrottledPeriods: 3, PidsCurrent: 42, PidsMax: 8192, OOMKills: 1},
		Errors: []string{"x"},
	})
	fams := gather(t, m)
	get := func(name string) float64 {
		f, ok := fams[name]
		if !ok || len(f.GetMetric()) == 0 {
			t.Fatalf("metric %s missing", name)
		}
		mm := f.GetMetric()[0]
		if mm.GetGauge() != nil {
			return mm.GetGauge().GetValue()
		}
		return mm.GetCounter().GetValue()
	}
	checks := map[string]float64{
		"recorder_host_cpu_percent":                   12.5,
		"recorder_ffmpeg_processes":                   3,
		"recorder_disk_free_bytes":                    250,
		"recorder_cgroup_memory_usage_bytes":          2048,
		"recorder_cgroup_cpu_usage_seconds_total":     123.5,
		"recorder_cgroup_cpu_throttled_periods_total": 3,
		"recorder_cgroup_oom_kills_total":             1,
		"recorder_cgroup_pids_current":                42,
		"recorder_sysmon_errors":                      1,
	}
	for name, want := range checks {
		if got := get(name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}

	m.RecordingBytes.WithLabelValues("a").Add(10)
	m.FFmpegRestartsTotal.WithLabelValues("a").Inc()
	m.FFmpegRSS.WithLabelValues("a").Set(5)
	m.RecordingBytes.WithLabelValues("b").Add(1)
	if n := len(gather(t, m)["recorder_recording_bytes_total"].GetMetric()); n != 2 {
		t.Fatalf("series before forget = %d", n)
	}
	m.ForgetRecording("a")
	fams = gather(t, m)
	if n := len(fams["recorder_recording_bytes_total"].GetMetric()); n != 1 {
		t.Fatalf("series after forget = %d", n)
	}
	for _, mm := range fams["recorder_recording_bytes_total"].GetMetric() {
		for _, l := range mm.GetLabel() {
			if l.GetName() == "id" && strings.Contains(l.GetValue(), "a") && l.GetValue() == "a" {
				t.Fatal("series for id a should be gone")
			}
		}
	}
	if _, ok := fams["recorder_ffmpeg_memory_rss_bytes"]; ok && len(fams["recorder_ffmpeg_memory_rss_bytes"].GetMetric()) != 0 {
		t.Fatal("rss series for id a should be gone")
	}
}
