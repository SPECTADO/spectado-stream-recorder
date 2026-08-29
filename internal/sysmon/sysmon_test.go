package sysmon

import (
	"os"
	"runtime"
	"testing"
	"time"
)

func TestSampleProducesPlausibleValues(t *testing.T) {
	var got Snapshot
	m := New(time.Second, t.TempDir(), func() int { return 7 })
	m.OnSample = func(s Snapshot) { got = s }
	s := m.Sample()
	if got.SampledAt.IsZero() || !got.SampledAt.Equal(s.SampledAt) {
		t.Fatal("OnSample not invoked with the sample")
	}
	if s.CPUCount < 1 || s.Memory.Total == 0 || s.Processes.Total < 1 || s.Processes.FFmpeg != 7 {
		t.Fatalf("implausible sample: %+v", s)
	}
	if s.Disk.Path == "" || s.Disk.Total == 0 {
		t.Fatalf("disk not sampled: %+v", s.Disk)
	}
	if s.Self.PID != os.Getpid() || s.Self.Goroutines < 1 || s.Self.RSS == 0 {
		t.Fatalf("self not sampled: %+v", s.Self)
	}
	if runtime.GOOS != "linux" && s.Cgroup != nil {
		t.Fatal("cgroup info must be nil outside linux")
	}
	if snap := m.Snapshot(); !snap.SampledAt.Equal(s.SampledAt) {
		t.Fatal("Snapshot should return the last sample")
	}
}

func TestProcStats(t *testing.T) {
	st, err := ProcStats(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if st.RSS == 0 || st.Threads < 1 || st.CPUSeconds < 0 {
		t.Fatalf("implausible: %+v", st)
	}
	if _, err := ProcStats(1<<30 - 1); err == nil {
		t.Fatal("expected an error for a non-existent pid")
	}
}

func TestParseLimitAndKeyValue(t *testing.T) {
	cases := map[string]int64{"max": -1, "": -1, "0": -1, "1024": 1024, "9223372036854771712": -1, "abc": -1}
	for in, want := range cases {
		if got := parseLimit(in); got != want {
			t.Errorf("parseLimit(%q) = %d, want %d", in, got, want)
		}
	}
	dir := t.TempDir()
	p := dir + "/cpu.stat"
	if err := os.WriteFile(p, []byte("usage_usec 123456\nuser_usec 100\nnr_throttled 3\nthrottled_usec 500\nbogus\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	kv := keyValueFile(p)
	if kv["usage_usec"] != 123456 || kv["nr_throttled"] != 3 || kv["throttled_usec"] != 500 {
		t.Fatalf("kv = %v", kv)
	}
	if keyValueFile(dir+"/missing") != nil {
		t.Fatal("missing file should yield nil")
	}
}
