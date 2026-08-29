// Package sysmon samples basic system health (CPU, memory, load, processes,
// disk, cgroup limits and the recorder's own resource usage).
//
// Values read from /proc (CPU percent, memory, load) describe the HOST (or the
// VM) as seen from inside the container; the cgroup section describes the
// container itself.
package sysmon

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/process"
)

// Snapshot is one sample of system state.
type Snapshot struct {
	SampledAt  time.Time `json:"sampledAt"`
	CPUPercent float64   `json:"hostCpuPercent"` // host-wide (as visible through /proc)
	CPUCount   int       `json:"cpuCount"`
	Load1      float64   `json:"hostLoad1"`
	Load5      float64   `json:"hostLoad5"`
	Load15     float64   `json:"hostLoad15"`
	Memory     Memory    `json:"hostMemory"`
	Processes  Processes `json:"processes"`
	Disk       Disk      `json:"disk"`
	Self       Self      `json:"self"`
	Cgroup     *Cgroup   `json:"container,omitempty"`
	Errors     []string  `json:"errors,omitempty"`
}

// Memory describes system memory.
type Memory struct {
	Total     uint64  `json:"total"`
	Used      uint64  `json:"used"`
	Available uint64  `json:"available"`
	Percent   float64 `json:"percent"`
}

// Processes counts processes visible to the recorder.
type Processes struct {
	Total  int `json:"total"`
	FFmpeg int `json:"ffmpeg"`
}

// Disk describes the filesystem holding the recordings.
type Disk struct {
	Path    string  `json:"path"`
	Total   uint64  `json:"total"`
	Used    uint64  `json:"used"`
	Free    uint64  `json:"free"`
	Percent float64 `json:"percent"`
}

// Self describes the recorder process itself.
type Self struct {
	PID        int     `json:"pid"`
	RSS        uint64  `json:"rss"`
	CPUPercent float64 `json:"cpuPercent"`
	Goroutines int     `json:"goroutines"`
	OpenFDs    int     `json:"openFds"`
	Threads    int     `json:"threads"`
}

// Cgroup holds container resource limits and usage (Linux only).
type Cgroup struct {
	Version           int     `json:"version"`
	MemoryLimit       int64   `json:"memoryLimit"`      // -1 when unlimited
	MemoryUsage       int64   `json:"memoryUsage"`      // includes page cache
	MemoryWorkingSet  int64   `json:"memoryWorkingSet"` // usage minus inactive file cache (what OOM cares about)
	MemoryPercent     float64 `json:"memoryPercent"`    // working set / limit, 0 when unlimited
	CPUQuotaCores     float64 `json:"cpuQuotaCores"`    // -1 when unlimited
	CPUUsageCores     float64 `json:"cpuUsageCores"`    // cores consumed during the last interval
	CPUPercentOfQuota float64 `json:"cpuPercentOfQuota"`
	CPUUsageSeconds   float64 `json:"cpuUsageSeconds"` // cumulative
	ThrottledSeconds  float64 `json:"cpuThrottledSeconds"`
	ThrottledPeriods  int64   `json:"cpuThrottledPeriods"`
	PidsCurrent       int64   `json:"pidsCurrent"`
	PidsMax           int64   `json:"pidsMax"` // -1 when unlimited
	OOMKills          int64   `json:"oomKills"`
}

// ProcStat is a point sample of one child process.
type ProcStat struct {
	CPUSeconds float64
	RSS        uint64
	Threads    int
}

// ProcStats reads CPU time, RSS and thread count of a process.
func ProcStats(pid int) (ProcStat, error) {
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return ProcStat{}, err
	}
	var st ProcStat
	if t, err := p.Times(); err == nil && t != nil {
		st.CPUSeconds = t.User + t.System
	} else if err != nil {
		return ProcStat{}, err
	}
	if mi, err := p.MemoryInfo(); err == nil && mi != nil {
		st.RSS = mi.RSS
	}
	if n, err := p.NumThreads(); err == nil {
		st.Threads = int(n)
	}
	return st, nil
}

// Monitor periodically samples the system.
type Monitor struct {
	interval time.Duration
	diskPath string
	ffmpeg   func() int

	// OnSample, when set, is called with every new sample (e.g. to update metrics).
	OnSample func(Snapshot)

	self *process.Process

	cgroupCPUUsage int64 // last cgroup cpu usage in microseconds
	cgroupSampled  time.Time

	mu   sync.RWMutex
	snap Snapshot
}

// New creates a Monitor. ffmpegCount reports the number of running ffmpeg
// children (the recorder knows this exactly, no process scan needed).
func New(interval time.Duration, diskPath string, ffmpegCount func() int) *Monitor {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	m := &Monitor{interval: interval, diskPath: diskPath, ffmpeg: ffmpegCount}
	if p, err := process.NewProcess(int32(os.Getpid())); err == nil {
		m.self = p
	}
	// Prime the delta based counters so the first real sample is meaningful.
	_, _ = cpu.Percent(0, false)
	if m.self != nil {
		_, _ = m.self.Percent(0)
	}
	m.sampleCgroupCPU()
	return m
}

// Run samples until ctx is cancelled.
func (m *Monitor) Run(ctx context.Context) {
	// Small delay so the first CPU delta covers a real interval.
	t := time.NewTimer(time.Second)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return
	case <-t.C:
	}
	m.Sample()
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.Sample()
		}
	}
}

// Snapshot returns the latest sample.
func (m *Monitor) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snap
}

// Sample takes one sample now and stores it.
func (m *Monitor) Sample() Snapshot {
	s := Snapshot{SampledAt: time.Now()}
	addErr := func(where string, err error) {
		if err != nil {
			s.Errors = append(s.Errors, where+": "+err.Error())
		}
	}

	if pct, err := cpu.Percent(0, false); err == nil && len(pct) > 0 {
		s.CPUPercent = round2(pct[0])
	} else {
		addErr("cpu", err)
	}
	s.CPUCount = runtime.NumCPU()

	if l, err := load.Avg(); err == nil {
		s.Load1, s.Load5, s.Load15 = round2(l.Load1), round2(l.Load5), round2(l.Load15)
	} else {
		addErr("load", err)
	}

	if vm, err := mem.VirtualMemory(); err == nil {
		s.Memory = Memory{Total: vm.Total, Used: vm.Used, Available: vm.Available, Percent: round2(vm.UsedPercent)}
	} else {
		addErr("memory", err)
	}

	if pids, err := process.Pids(); err == nil {
		s.Processes.Total = len(pids)
	} else {
		addErr("processes", err)
	}
	if m.ffmpeg != nil {
		s.Processes.FFmpeg = m.ffmpeg()
	}

	if m.diskPath != "" {
		if du, err := disk.Usage(m.diskPath); err == nil {
			s.Disk = Disk{Path: m.diskPath, Total: du.Total, Used: du.Used, Free: du.Free, Percent: round2(du.UsedPercent)}
		} else {
			addErr("disk", err)
			s.Disk.Path = m.diskPath
		}
	}

	s.Self.PID = os.Getpid()
	s.Self.Goroutines = runtime.NumGoroutine()
	if m.self != nil {
		if mi, err := m.self.MemoryInfo(); err == nil && mi != nil {
			s.Self.RSS = mi.RSS
		}
		if pct, err := m.self.Percent(0); err == nil {
			s.Self.CPUPercent = round2(pct)
		}
		if n, err := m.self.NumFDs(); err == nil {
			s.Self.OpenFDs = int(n)
		}
		if n, err := m.self.NumThreads(); err == nil {
			s.Self.Threads = int(n)
		}
	}

	s.Cgroup = m.readCgroup()

	m.mu.Lock()
	m.snap = s
	m.mu.Unlock()
	if m.OnSample != nil {
		m.OnSample(s)
	}
	return s
}

func round2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

// --- cgroup support -------------------------------------------------------

const cgroupRoot = "/sys/fs/cgroup"

func readTrimmed(path string) (string, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(b)), true
}

func readInt(path string) (int64, bool) {
	s, ok := readTrimmed(path)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil
}

func parseLimit(s string) int64 {
	if s == "" || s == "max" {
		return -1
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n >= 1<<60 { // cgroup v1 reports a huge number for "unlimited"
		return -1
	}
	return n
}

// keyValueFile parses "key value" lines (cpu.stat, memory.events).
func keyValueFile(path string) map[string]int64 {
	s, ok := readTrimmed(path)
	if !ok {
		return nil
	}
	out := map[string]int64{}
	for _, line := range strings.Split(s, "\n") {
		if f := strings.Fields(line); len(f) == 2 {
			if n, err := strconv.ParseInt(f[1], 10, 64); err == nil {
				out[f[0]] = n
			}
		}
	}
	return out
}

// cgroupCPUUsageMicros returns cumulative CPU usage of the container in µs.
func cgroupCPUUsageMicros() (int64, int, bool) {
	if kv := keyValueFile(filepath.Join(cgroupRoot, "cpu.stat")); kv != nil {
		if n, ok := kv["usage_usec"]; ok {
			return n, 2, true
		}
	}
	// cgroup v1 (nanoseconds)
	for _, p := range []string{"cpu,cpuacct/cpuacct.usage", "cpuacct/cpuacct.usage"} {
		if n, ok := readInt(filepath.Join(cgroupRoot, p)); ok {
			return n / 1000, 1, true
		}
	}
	return 0, 0, false
}

func (m *Monitor) sampleCgroupCPU() (cores float64, usageMicros int64, ok bool) {
	usage, _, found := cgroupCPUUsageMicros()
	if !found {
		return 0, 0, false
	}
	now := time.Now()
	if !m.cgroupSampled.IsZero() {
		elapsed := now.Sub(m.cgroupSampled).Microseconds()
		if elapsed > 0 && usage >= m.cgroupCPUUsage {
			cores = float64(usage-m.cgroupCPUUsage) / float64(elapsed)
			ok = true
		}
	}
	m.cgroupCPUUsage = usage
	m.cgroupSampled = now
	return cores, usage, ok
}

func (m *Monitor) readCgroup() *Cgroup {
	if runtime.GOOS != "linux" {
		return nil
	}
	cg := &Cgroup{MemoryLimit: -1, CPUQuotaCores: -1, PidsMax: -1}
	found := false

	if s, ok := readTrimmed(filepath.Join(cgroupRoot, "memory.max")); ok {
		// cgroup v2
		found = true
		cg.Version = 2
		cg.MemoryLimit = parseLimit(s)
		cg.MemoryUsage, _ = readInt(filepath.Join(cgroupRoot, "memory.current"))
		cg.MemoryWorkingSet = cg.MemoryUsage
		if kv := keyValueFile(filepath.Join(cgroupRoot, "memory.stat")); kv != nil {
			if ws := cg.MemoryUsage - kv["inactive_file"]; ws > 0 {
				cg.MemoryWorkingSet = ws
			}
		}
		if q, ok := readTrimmed(filepath.Join(cgroupRoot, "cpu.max")); ok {
			if f := strings.Fields(q); len(f) == 2 && f[0] != "max" {
				quota, _ := strconv.ParseFloat(f[0], 64)
				period, _ := strconv.ParseFloat(f[1], 64)
				if quota > 0 && period > 0 {
					cg.CPUQuotaCores = round2(quota / period)
				}
			}
		}
		if kv := keyValueFile(filepath.Join(cgroupRoot, "cpu.stat")); kv != nil {
			cg.ThrottledSeconds = float64(kv["throttled_usec"]) / 1e6
			cg.ThrottledPeriods = kv["nr_throttled"]
		}
		if kv := keyValueFile(filepath.Join(cgroupRoot, "memory.events")); kv != nil {
			cg.OOMKills = kv["oom_kill"]
		}
		cg.PidsCurrent, _ = readInt(filepath.Join(cgroupRoot, "pids.current"))
		if s, ok := readTrimmed(filepath.Join(cgroupRoot, "pids.max")); ok {
			cg.PidsMax = parseLimit(s)
		}
	} else if s, ok := readTrimmed(filepath.Join(cgroupRoot, "memory/memory.limit_in_bytes")); ok {
		// cgroup v1
		found = true
		cg.Version = 1
		cg.MemoryLimit = parseLimit(s)
		cg.MemoryUsage, _ = readInt(filepath.Join(cgroupRoot, "memory/memory.usage_in_bytes"))
		cg.MemoryWorkingSet = cg.MemoryUsage
		if kv := keyValueFile(filepath.Join(cgroupRoot, "memory/memory.stat")); kv != nil {
			if ws := cg.MemoryUsage - kv["total_inactive_file"]; ws > 0 {
				cg.MemoryWorkingSet = ws
			}
		}
		q, okq := readTrimmed(filepath.Join(cgroupRoot, "cpu,cpuacct/cpu.cfs_quota_us"))
		p, okp := readTrimmed(filepath.Join(cgroupRoot, "cpu,cpuacct/cpu.cfs_period_us"))
		if !okq {
			q, okq = readTrimmed(filepath.Join(cgroupRoot, "cpu/cpu.cfs_quota_us"))
			p, okp = readTrimmed(filepath.Join(cgroupRoot, "cpu/cpu.cfs_period_us"))
		}
		if okq && okp {
			quota, _ := strconv.ParseFloat(q, 64)
			period, _ := strconv.ParseFloat(p, 64)
			if quota > 0 && period > 0 {
				cg.CPUQuotaCores = round2(quota / period)
			}
		}
		for _, p := range []string{"cpu,cpuacct/cpu.stat", "cpu/cpu.stat"} {
			if kv := keyValueFile(filepath.Join(cgroupRoot, p)); kv != nil {
				cg.ThrottledSeconds = float64(kv["throttled_time"]) / 1e9
				cg.ThrottledPeriods = kv["nr_throttled"]
				break
			}
		}
		if kv := keyValueFile(filepath.Join(cgroupRoot, "memory/memory.oom_control")); kv != nil {
			cg.OOMKills = kv["oom_kill"]
		}
		cg.PidsCurrent, _ = readInt(filepath.Join(cgroupRoot, "pids/pids.current"))
		if s, ok := readTrimmed(filepath.Join(cgroupRoot, "pids/pids.max")); ok {
			cg.PidsMax = parseLimit(s)
		}
	}
	if !found {
		return nil
	}
	if cg.MemoryLimit > 0 {
		cg.MemoryPercent = round2(float64(cg.MemoryWorkingSet) / float64(cg.MemoryLimit) * 100)
	}
	cores, usage, ok := m.sampleCgroupCPU()
	cg.CPUUsageSeconds = float64(usage) / 1e6
	if ok {
		cg.CPUUsageCores = round2(cores)
		if cg.CPUQuotaCores > 0 {
			cg.CPUPercentOfQuota = round2(cores / cg.CPUQuotaCores * 100)
		}
	}
	return cg
}
