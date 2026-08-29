// Package metrics defines the Prometheus metrics exposed by the recorder.
package metrics

import (
	"math"
	"runtime"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/spectado/stream-recorder/internal/sysmon"
)

const namespace = "recorder"

// Metrics groups every metric plus the registry they live in.
type Metrics struct {
	Registry *prometheus.Registry

	// Schedule.
	ScheduleFetchTotal          *prometheus.CounterVec // result=success|failure|not_modified
	ScheduleLastSuccess         prometheus.Gauge
	ScheduleLastAttempt         prometheus.Gauge
	ScheduleConsecutiveFailures prometheus.Gauge
	ScheduleItems               prometheus.Gauge
	ScheduleInvalidItems        prometheus.Gauge
	ScheduleLoaded              prometheus.Gauge // 1 once any schedule (url or cache) is loaded
	ScheduleClockSkew           prometheus.Gauge

	// Recording.
	RecordingsActive         prometheus.Gauge
	RecordingsStartedTotal   prometheus.Counter
	RecordingsFinishedTotal  *prometheus.CounterVec // reason=ended|removed|rotated|superseded|error
	RecordingsSuspendedTotal prometheus.Counter     // paused for shutdown (resumed on next start)
	RecordingsSkippedTotal   *prometheus.CounterVec // reason=max_recordings|disk_low|error
	RecordingBytes           *prometheus.CounterVec // id
	RecordingFFmpegRunning   *prometheus.GaugeVec   // id
	RecordingLastData        *prometheus.GaugeVec   // id (unix seconds)
	FFmpegRestartsTotal      *prometheus.CounterVec // id
	FFmpegCPUSeconds         *prometheus.CounterVec // id
	FFmpegRSS                *prometheus.GaugeVec   // id
	FFmpegStallsTotal        prometheus.Counter
	FFmpegKilledTotal        prometheus.Counter
	FFmpegSpawnFailuresTotal prometheus.Counter
	FFmpegProcesses          prometheus.Gauge
	FFmpegSpawnedTotal       prometheus.Counter
	FFmpegInfo               *prometheus.GaugeVec // version, path
	CodecFallbacksTotal      prometheus.Counter
	WriteErrorsTotal         prometheus.Counter
	RecordingsOnDiskBytes    prometheus.Gauge
	DiskLow                  prometheus.Gauge

	// Upload.
	UploadsTotal           *prometheus.CounterVec // result=success|failure
	UploadBytesTotal       prometheus.Counter
	UploadsPending         prometheus.Gauge
	UploadPendingBytes     prometheus.Gauge
	UploadsInProgress      prometheus.Gauge
	UploadsBlocked         prometheus.Gauge
	UploadsFailedFinal     prometheus.Gauge // recordings that ended up in a failed state
	UploadOldestPendingAge prometheus.Gauge
	UploadDuration         prometheus.Histogram
	UploadRetriesTotal     prometheus.Counter

	// System (host view via /proc, plus container cgroup view).
	SysCPUPercent          prometheus.Gauge
	SysCPUCount            prometheus.Gauge
	SysLoad1               prometheus.Gauge
	SysLoad5               prometheus.Gauge
	SysLoad15              prometheus.Gauge
	SysMemTotal            prometheus.Gauge
	SysMemUsed             prometheus.Gauge
	SysMemAvailable        prometheus.Gauge
	SysMemPercent          prometheus.Gauge
	SysProcesses           prometheus.Gauge
	DiskTotal              *prometheus.GaugeVec // path
	DiskFree               *prometheus.GaugeVec // path
	DiskUsedPercent        *prometheus.GaugeVec // path
	SelfRSS                prometheus.Gauge
	SelfCPUPercent         prometheus.Gauge
	SelfOpenFDs            prometheus.Gauge
	SelfThreads            prometheus.Gauge
	CgroupMemoryLimit      prometheus.Gauge
	CgroupMemoryUsage      prometheus.Gauge
	CgroupMemoryWorkingSet prometheus.Gauge
	CgroupCPUQuotaCores    prometheus.Gauge
	CgroupCPUUsageCores    prometheus.Gauge
	CgroupCPUPercent       prometheus.Gauge
	CgroupPids             prometheus.Gauge
	CgroupPidsMax          prometheus.Gauge
	SysmonErrors           prometheus.Gauge

	// cgroup counters are exposed via CounterFunc reading these values.
	cgroupCPUSeconds       atomicFloat
	cgroupThrottledSeconds atomicFloat
	cgroupThrottledPeriods atomicFloat
	cgroupOOMKills         atomicFloat

	BuildInfo *prometheus.GaugeVec
}

type atomicFloat struct{ v atomic.Uint64 }

func (a *atomicFloat) Store(f float64) { a.v.Store(math.Float64bits(f)) }
func (a *atomicFloat) Load() float64   { return math.Float64frombits(a.v.Load()) }

// New builds and registers all metrics on a fresh registry.
func New(version string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: help})
		reg.MustRegister(g)
		return g
	}
	gaugeVec := func(name, help string, labels ...string) *prometheus.GaugeVec {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: help}, labels)
		reg.MustRegister(g)
		return g
	}
	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Namespace: namespace, Name: name, Help: help})
		reg.MustRegister(c)
		return c
	}
	counterVec := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: name, Help: help}, labels)
		reg.MustRegister(c)
		return c
	}
	counterFunc := func(name, help string, f func() float64) {
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: namespace, Name: name, Help: help}, f))
	}

	m := &Metrics{Registry: reg}

	m.ScheduleFetchTotal = counterVec("schedule_fetch_total", "Schedule fetch attempts by result.", "result")
	for _, r := range []string{"success", "failure", "not_modified"} {
		m.ScheduleFetchTotal.WithLabelValues(r)
	}
	m.ScheduleLastSuccess = gauge("schedule_last_success_timestamp_seconds", "Unix time of the last successful schedule fetch.")
	m.ScheduleLastAttempt = gauge("schedule_last_attempt_timestamp_seconds", "Unix time of the last schedule fetch attempt.")
	m.ScheduleConsecutiveFailures = gauge("schedule_consecutive_failures", "Number of consecutive failed schedule fetches.")
	m.ScheduleItems = gauge("schedule_items", "Number of valid items in the last known schedule.")
	m.ScheduleInvalidItems = gauge("schedule_invalid_items", "Number of items rejected in the last known schedule.")
	m.ScheduleLoaded = gauge("schedule_loaded", "1 when a schedule has been loaded from the URL or the cache.")
	m.ScheduleClockSkew = gauge("schedule_clock_skew_seconds", "Recorder clock minus the schedule server's Date header at the last fetch.")

	m.RecordingsActive = gauge("recordings_active", "Number of recording sessions currently capturing.")
	m.RecordingsStartedTotal = counter("recordings_started_total", "Recording sessions started.")
	m.RecordingsFinishedTotal = counterVec("recordings_finished_total", "Recording sessions finished by reason.", "reason")
	for _, r := range []string{"ended", "removed", "rotated", "superseded", "error"} {
		m.RecordingsFinishedTotal.WithLabelValues(r)
	}
	m.RecordingsSuspendedTotal = counter("recordings_suspended_total", "Recording sessions paused for shutdown (resumed on the next start).")
	m.RecordingsSkippedTotal = counterVec("recordings_skipped_total", "Recording starts refused by reason.", "reason")
	for _, r := range []string{"max_recordings", "disk_low", "error"} {
		m.RecordingsSkippedTotal.WithLabelValues(r)
	}
	m.RecordingBytes = counterVec("recording_bytes_total", "Bytes captured per recording id.", "id")
	m.RecordingFFmpegRunning = gaugeVec("recording_ffmpeg_running", "1 while an ffmpeg process is running for the recording id.", "id")
	m.RecordingLastData = gaugeVec("recording_last_data_timestamp_seconds", "Unix time when data last arrived for the recording id.", "id")
	m.FFmpegRestartsTotal = counterVec("ffmpeg_restarts_total", "ffmpeg restarts per recording id.", "id")
	m.FFmpegCPUSeconds = counterVec("ffmpeg_cpu_seconds_total", "CPU time consumed by the ffmpeg processes of a recording id (accumulated across restarts).", "id")
	m.FFmpegRSS = gaugeVec("ffmpeg_memory_rss_bytes", "Resident memory of the current ffmpeg process per recording id.", "id")
	m.FFmpegStallsTotal = counter("ffmpeg_stalls_total", "ffmpeg processes killed because no data arrived within the stall timeout.")
	m.FFmpegKilledTotal = counter("ffmpeg_killed_total", "ffmpeg processes that died from SIGKILL not sent by the recorder (OOM killer?).")
	m.FFmpegSpawnFailuresTotal = counter("ffmpeg_spawn_failures_total", "Failed attempts to start an ffmpeg process (fork/exec errors).")
	m.FFmpegProcesses = gauge("ffmpeg_processes", "Currently running ffmpeg processes.")
	m.FFmpegSpawnedTotal = counter("ffmpeg_spawned_total", "Total ffmpeg processes spawned.")
	m.FFmpegInfo = gaugeVec("ffmpeg_info", "ffmpeg binary in use.", "version", "path")
	m.CodecFallbacksTotal = counter("codec_fallbacks_total", "Sessions that switched from stream copy to transcoding after repeated empty runs.")
	m.WriteErrorsTotal = counter("write_errors_total", "Failed writes to recording files (e.g. disk full).")
	m.RecordingsOnDiskBytes = gauge("recordings_on_disk_bytes", "Bytes of recordings currently stored locally (recording + waiting for upload + kept).")
	m.DiskLow = gauge("disk_low", "1 while free disk is below MIN_FREE_DISK (new recordings are refused).")

	m.UploadsTotal = counterVec("uploads_total", "Upload attempts by result.", "result")
	for _, r := range []string{"success", "failure"} {
		m.UploadsTotal.WithLabelValues(r)
	}
	m.UploadBytesTotal = counter("upload_bytes_total", "Bytes successfully uploaded.")
	m.UploadsPending = gauge("uploads_pending", "Recordings waiting to be uploaded (including retries).")
	m.UploadPendingBytes = gauge("upload_pending_bytes", "Bytes of recordings waiting to be uploaded.")
	m.UploadsInProgress = gauge("uploads_in_progress", "Uploads currently running.")
	m.UploadsBlocked = gauge("uploads_blocked", "Pending uploads whose last failure looked permanent (credentials, bucket, request).")
	m.UploadsFailedFinal = gauge("recordings_failed", "Recordings in a failed state (e.g. empty file).")
	m.UploadOldestPendingAge = gauge("upload_oldest_pending_age_seconds", "Age of the oldest recording still waiting for upload.")
	m.UploadDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Name: "upload_duration_seconds", Help: "Duration of successful uploads.",
		Buckets: []float64{1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600, 1200, 1800, 3600},
	})
	reg.MustRegister(m.UploadDuration)
	m.UploadRetriesTotal = counter("upload_retries_total", "Upload attempts that were retried after a failure.")

	m.SysCPUPercent = gauge("host_cpu_percent", "Host-wide CPU utilisation percent as visible through /proc (not container-scoped).")
	m.SysCPUCount = gauge("host_cpu_count", "Logical CPUs visible to the process.")
	m.SysLoad1 = gauge("host_load1", "Host 1 minute load average.")
	m.SysLoad5 = gauge("host_load5", "Host 5 minute load average.")
	m.SysLoad15 = gauge("host_load15", "Host 15 minute load average.")
	m.SysMemTotal = gauge("host_memory_total_bytes", "Host total memory as visible through /proc.")
	m.SysMemUsed = gauge("host_memory_used_bytes", "Host used memory as visible through /proc.")
	m.SysMemAvailable = gauge("host_memory_available_bytes", "Host available memory as visible through /proc.")
	m.SysMemPercent = gauge("host_memory_used_percent", "Host used memory percent.")
	m.SysProcesses = gauge("system_processes", "Number of processes visible to the recorder (its PID namespace).")
	m.DiskTotal = gaugeVec("disk_total_bytes", "Filesystem size.", "path")
	m.DiskFree = gaugeVec("disk_free_bytes", "Filesystem free space.", "path")
	m.DiskUsedPercent = gaugeVec("disk_used_percent", "Filesystem used percent.", "path")
	m.SelfRSS = gauge("self_rss_bytes", "Resident memory of the recorder process.")
	m.SelfCPUPercent = gauge("self_cpu_percent", "CPU percent used by the recorder process itself.")
	m.SelfOpenFDs = gauge("self_open_fds", "Open file descriptors of the recorder process.")
	m.SelfThreads = gauge("self_threads", "OS threads of the recorder process.")
	m.CgroupMemoryLimit = gauge("cgroup_memory_limit_bytes", "Container memory limit (-1 when unlimited).")
	m.CgroupMemoryUsage = gauge("cgroup_memory_usage_bytes", "Container memory usage including page cache (whole cgroup, ffmpeg children included).")
	m.CgroupMemoryWorkingSet = gauge("cgroup_memory_working_set_bytes", "Container working set: usage minus inactive file cache (what the OOM killer acts on).")
	m.CgroupCPUQuotaCores = gauge("cgroup_cpu_quota_cores", "Container CPU quota in cores (-1 when unlimited).")
	m.CgroupCPUUsageCores = gauge("cgroup_cpu_usage_cores", "Container CPU usage in cores during the last sampling interval.")
	m.CgroupCPUPercent = gauge("cgroup_cpu_percent_of_quota", "Container CPU usage as percent of quota.")
	m.CgroupPids = gauge("cgroup_pids_current", "Tasks (processes+threads) in the container cgroup.")
	m.CgroupPidsMax = gauge("cgroup_pids_max", "Task limit of the container cgroup (-1 when unlimited).")
	counterFunc("cgroup_cpu_usage_seconds_total", "Cumulative CPU time of the container cgroup.", m.cgroupCPUSeconds.Load)
	counterFunc("cgroup_cpu_throttled_seconds_total", "Cumulative time the container was CPU throttled.", m.cgroupThrottledSeconds.Load)
	counterFunc("cgroup_cpu_throttled_periods_total", "Number of throttled CPU periods.", m.cgroupThrottledPeriods.Load)
	counterFunc("cgroup_oom_kills_total", "OOM kills recorded by the container cgroup.", m.cgroupOOMKills.Load)
	m.SysmonErrors = gauge("sysmon_errors", "Number of errors in the last system sample.")

	m.BuildInfo = gaugeVec("build_info", "Build information.", "version", "goversion")
	m.BuildInfo.WithLabelValues(version, runtime.Version()).Set(1)

	return m
}

// UpdateSystem copies a system snapshot into the gauges.
func (m *Metrics) UpdateSystem(s sysmon.Snapshot) {
	m.SysCPUPercent.Set(s.CPUPercent)
	m.SysCPUCount.Set(float64(s.CPUCount))
	m.SysLoad1.Set(s.Load1)
	m.SysLoad5.Set(s.Load5)
	m.SysLoad15.Set(s.Load15)
	m.SysMemTotal.Set(float64(s.Memory.Total))
	m.SysMemUsed.Set(float64(s.Memory.Used))
	m.SysMemAvailable.Set(float64(s.Memory.Available))
	m.SysMemPercent.Set(s.Memory.Percent)
	m.SysProcesses.Set(float64(s.Processes.Total))
	m.FFmpegProcesses.Set(float64(s.Processes.FFmpeg))
	if s.Disk.Path != "" {
		m.DiskTotal.WithLabelValues(s.Disk.Path).Set(float64(s.Disk.Total))
		m.DiskFree.WithLabelValues(s.Disk.Path).Set(float64(s.Disk.Free))
		m.DiskUsedPercent.WithLabelValues(s.Disk.Path).Set(s.Disk.Percent)
	}
	m.SelfRSS.Set(float64(s.Self.RSS))
	m.SelfCPUPercent.Set(s.Self.CPUPercent)
	m.SelfOpenFDs.Set(float64(s.Self.OpenFDs))
	m.SelfThreads.Set(float64(s.Self.Threads))
	if s.Cgroup != nil {
		m.CgroupMemoryLimit.Set(float64(s.Cgroup.MemoryLimit))
		m.CgroupMemoryUsage.Set(float64(s.Cgroup.MemoryUsage))
		m.CgroupMemoryWorkingSet.Set(float64(s.Cgroup.MemoryWorkingSet))
		m.CgroupCPUQuotaCores.Set(s.Cgroup.CPUQuotaCores)
		m.CgroupCPUUsageCores.Set(s.Cgroup.CPUUsageCores)
		m.CgroupCPUPercent.Set(s.Cgroup.CPUPercentOfQuota)
		m.CgroupPids.Set(float64(s.Cgroup.PidsCurrent))
		m.CgroupPidsMax.Set(float64(s.Cgroup.PidsMax))
		m.cgroupCPUSeconds.Store(s.Cgroup.CPUUsageSeconds)
		m.cgroupThrottledSeconds.Store(s.Cgroup.ThrottledSeconds)
		m.cgroupThrottledPeriods.Store(float64(s.Cgroup.ThrottledPeriods))
		m.cgroupOOMKills.Store(float64(s.Cgroup.OOMKills))
	}
	m.SysmonErrors.Set(float64(len(s.Errors)))
}

// ForgetRecording removes per-id series once a recording is fully done, so
// label cardinality stays bounded to the set of recent recordings.
func (m *Metrics) ForgetRecording(id string) {
	m.RecordingBytes.DeleteLabelValues(id)
	m.RecordingFFmpegRunning.DeleteLabelValues(id)
	m.RecordingLastData.DeleteLabelValues(id)
	m.FFmpegRestartsTotal.DeleteLabelValues(id)
	m.FFmpegCPUSeconds.DeleteLabelValues(id)
	m.FFmpegRSS.DeleteLabelValues(id)
}
