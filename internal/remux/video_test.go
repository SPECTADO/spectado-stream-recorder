package remux

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Video tests drive the real binaries too (see remux_test.go); they also need
// libx264, which every recorder image ships when RECORD_VIDEO is usable.

func videoTools(t *testing.T) *Remuxer {
	t.Helper()
	r := tools(t)
	out, err := exec.Command(r.FFmpegPath, "-hide_banner", "-h", "encoder=libx264").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "Encoder libx264") {
		t.Skip("ffmpeg has no libx264")
	}
	return r
}

// avTS writes seconds of testsrc2 (w×h @ 25 fps, H.264, a keyframe every
// second) plus a sine (AAC) as MPEG-TS — what a capture of an A/V source
// looks like. Every such file starts its timestamps at ~1.4 s, exactly like
// each restart of the capture ffmpeg.
func avTS(t *testing.T, dir, name string, w, h int, seconds float64, sampleRate, channels int) string {
	t.Helper()
	path := filepath.Join(dir, name)
	args := []string{
		"-nostdin", "-hide_banner", "-v", "error", "-y",
		"-f", "lavfi", "-i", fmt.Sprintf("testsrc2=s=%dx%d:r=25:d=%g", w, h, seconds),
		"-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=440:sample_rate=%d:duration=%g", sampleRate, seconds),
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "25", "-pix_fmt", "yuv420p",
		"-ac", fmt.Sprint(channels), "-c:a", "aac", "-b:a", "128k",
		"-f", "mpegts", path,
	}
	out, err := exec.Command("ffmpeg", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("generate %s: %v\n%s", name, err, out)
	}
	return path
}

// truncateMidPacket cuts cut bytes off the end of path so that the size is no
// longer a multiple of 188 (a capture killed mid-write), and returns the size.
func truncateMidPacket(t *testing.T, path string, cut int64) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	size := fi.Size() - cut
	if size%188 == 0 {
		size--
	}
	if err := os.Truncate(path, size); err != nil {
		t.Fatal(err)
	}
	return size
}

// damageLastFrame truncates path mid-packet so that its last video frame is
// damaged the way a killed capture's is: it decodes as "corrupt decoded
// frame", which -xerror turns into a failed run.
func damageLastFrame(t *testing.T, path string) {
	t.Helper()
	orig, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []int{3013, 2013, 5013, 12013, 20013} {
		if err := os.WriteFile(path, orig[:len(orig)-cut], 0o644); err != nil {
			t.Fatal(err)
		}
		err := exec.Command("ffmpeg", "-nostdin", "-v", "quiet", "-xerror", "-fflags", "+discardcorrupt", "-i", path, "-f", "null", "-").Run()
		if err != nil {
			return
		}
	}
	t.Fatalf("no truncation of %s produced a corrupt last frame", path)
}

func probeVideo(t *testing.T, r *Remuxer, path string) VideoInfo {
	t.Helper()
	info, err := r.ProbeVideo(t.Context(), path)
	if err != nil {
		t.Fatalf("ProbeVideo %s: %v", path, err)
	}
	return info
}

// m4aFrom builds the .m4a the recorder would upload from an ADTS capture.
func m4aFrom(t *testing.T, r *Remuxer, adtsPath, out string, sampleRate, channels int) Result {
	t.Helper()
	info := scan(t, adtsPath)
	res, err := r.Build(t.Context(), []string{adtsPath}, out, Expect{
		Frames: info.Frames, Duration: info.Duration, Params: paramsFor(t, sampleRate, channels),
	}, Metadata{})
	if err != nil {
		t.Fatalf("Build %s: %v", out, err)
	}
	return res
}

func TestProbeVideoReadsTSStreams(t *testing.T) {
	r := videoTools(t)
	dir := t.TempDir()
	ts := avTS(t, dir, "a.ts", 320, 240, 3, 48000, 1)

	info := probeVideo(t, r, ts)
	if !info.HasVideo || info.VideoCodec != "h264" || info.Width != 320 || info.Height != 240 || info.PixFmt != "yuv420p" {
		t.Errorf("video = %s", describeVideo(info))
	}
	if info.FPS != 25 {
		t.Errorf("fps = %g, want 25", info.FPS)
	}
	if info.AudioCodec != "aac" || info.SampleRate != 48000 || info.Channels != 1 {
		t.Errorf("audio = %s", describeVideo(info))
	}
	if info.VideoPackets != 75 {
		t.Errorf("video packets = %d, want 75 (3 s × 25 fps)", info.VideoPackets)
	}
	// 3 s at 48 kHz is 140.6 AAC frames plus encoder priming/padding.
	if info.AudioPackets < 141 || info.AudioPackets > 145 {
		t.Errorf("audio packets = %d, want ≈ 142", info.AudioPackets)
	}
	if info.Format != "mpegts" || info.Duration < 3*time.Second || info.Duration > 3100*time.Millisecond {
		t.Errorf("format = %q, duration = %s", info.Format, info.Duration)
	}

	// Raw ADTS (the audio-only capture) probes too: no video, audio counted.
	aac := sineADTS(t, dir, "a.aac", 44100, 2, 2, "128k")
	ai := probeVideo(t, r, aac)
	if ai.HasVideo || ai.AudioCodec != "aac" || ai.AudioPackets != scan(t, aac).Frames {
		t.Errorf("adts probe = %+v, scanned %d frames", ai, scan(t, aac).Frames)
	}
}

// TestBuildBlackVideoFromM4A: the black companion carries the .m4a's audio
// sample-exact, a 16x16 h264 picture and the metadata — including audio
// shorter than one picture frame, where -shortest would drop the video track.
func TestBuildBlackVideoFromM4A(t *testing.T) {
	r := videoTools(t)
	for _, seconds := range []float64{12.3, 0.5} {
		t.Run(fmt.Sprint(seconds), func(t *testing.T) {
			dir := t.TempDir()
			aac := sineADTS(t, dir, "in.aac", 48000, 2, seconds, "128k")
			m4a := m4aFrom(t, r, aac, filepath.Join(dir, "in.m4a"), 48000, 2)

			out := filepath.Join(dir, "out.mp4")
			meta := Metadata{Title: "Match RO vs JP", Description: runTable(), CreationTime: time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)}
			res, err := r.BuildBlackVideo(t.Context(), m4a.Path, out, meta)
			if err != nil {
				t.Fatalf("BuildBlackVideo: %v", err)
			}
			v := res.Video
			if !v.HasVideo || v.VideoCodec != "h264" || v.Width != 16 || v.Height != 16 || v.VideoPackets == 0 {
				t.Errorf("video = %s (%d packets)", describeVideo(v), v.VideoPackets)
			}
			if v.AudioPackets != m4a.Frames || res.Frames != m4a.Frames {
				t.Errorf("audio packets = %d / nb_frames %d, m4a has %d", v.AudioPackets, res.Frames, m4a.Frames)
			}
			if d := abs(res.Duration - m4a.Duration); d > blackDurationTolerance {
				t.Errorf("duration = %s, m4a %s", res.Duration, m4a.Duration)
			}
			if res.Tags["title"] != meta.Title || res.Tags["description"] != meta.Description {
				t.Errorf("tags = %v", res.Tags)
			}
			// The audio is a stream copy: extracting it yields the original ADTS.
			back := filepath.Join(dir, "back.aac")
			if err := r.Extract(t.Context(), out, back); err != nil {
				t.Fatalf("Extract: %v", err)
			}
			a, _ := os.ReadFile(aac)
			b, _ := os.ReadFile(back)
			if string(a) != string(b) {
				t.Errorf("audio of the mp4 differs from the source ADTS (%d vs %d bytes)", len(b), len(a))
			}
			mustNotExist(t, out+".part")
		})
	}
}

func TestBuildBlackVideoFailsOnNonMedia(t *testing.T) {
	r := videoTools(t)
	dir := t.TempDir()
	in := filepath.Join(dir, "junk.m4a")
	if err := os.WriteFile(in, []byte(strings.Repeat("not media ", 1000)), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out.mp4")
	if _, err := r.BuildBlackVideo(t.Context(), in, out, Metadata{}); err == nil {
		t.Fatal("BuildBlackVideo on junk succeeded")
	}
	mustNotExist(t, out, out+".part")
}

// TestBuildVideoConcatTimestampResetAndTruncatedTail is the core of the video
// path: two captures that both restart their timestamps, the first one killed
// mid-packet, joined by stream copy with every audio packet accounted for —
// and a list entry whose path needs ffconcat quoting.
func TestBuildVideoConcatTimestampResetAndTruncatedTail(t *testing.T) {
	r := videoTools(t)
	dir := t.TempDir()
	s1 := avTS(t, dir, "s1.ts", 320, 240, 6, 48000, 1)
	odd := filepath.Join(dir, `it's a \weird #dir`)
	if err := os.Mkdir(odd, 0o755); err != nil {
		t.Fatal(err)
	}
	s2 := avTS(t, odd, "s2 'run'.ts", 320, 240, 4, 48000, 1)

	// Cut inside the last audio PES: without -fflags +discardcorrupt this
	// exact input makes -xerror abort the concat (exit 183).
	if size := truncateMidPacket(t, s1, 1013); size%188 == 0 {
		t.Fatalf("size %d is a whole number of TS packets", size)
	}
	i1, i2 := probeVideo(t, r, s1), probeVideo(t, r, s2)

	out := filepath.Join(dir, "out.mp4")
	res, err := r.BuildVideoConcat(t.Context(), []string{s1, s2}, out, Metadata{Title: "concat"})
	if err != nil {
		t.Fatalf("BuildVideoConcat: %v", err)
	}
	v := res.Video
	if v.AudioPackets != i1.AudioPackets+i2.AudioPackets {
		t.Errorf("audio packets = %d, inputs %d + %d", v.AudioPackets, i1.AudioPackets, i2.AudioPackets)
	}
	if v.VideoPackets != i1.VideoPackets+i2.VideoPackets {
		t.Errorf("video packets = %d, inputs %d + %d", v.VideoPackets, i1.VideoPackets, i2.VideoPackets)
	}
	// Rebased, not overlapping: ~10 s, not ~6 s (which is what two timelines
	// both starting at 1.4 s would collapse to).
	sum := i1.Duration + i2.Duration
	if res.Duration < sum-200*time.Millisecond || res.Duration > sum+time.Second {
		t.Errorf("duration = %s, inputs sum %s", res.Duration, sum)
	}
	if v.VideoCodec != "h264" || v.Width != 320 || v.Height != 240 || res.Tags["title"] != "concat" {
		t.Errorf("result = %s, tags %v", describeVideo(v), res.Tags)
	}
	head, _ := os.ReadFile(out)
	if len(head) < 12 || string(head[4:8]) != "ftyp" {
		t.Errorf("leading box = %q", head[:min(12, len(head))])
	}
	// The temporary list is gone.
	if m, _ := filepath.Glob(filepath.Join(dir, ".tmp-concat-*")); len(m) != 0 {
		t.Errorf("list file left behind: %v", m)
	}
	mustNotExist(t, out+".part")
}

func TestBuildVideoConcatRejectsMixedInputs(t *testing.T) {
	r := videoTools(t)
	dir := t.TempDir()
	a := avTS(t, dir, "a.ts", 320, 240, 2, 48000, 1)
	b := avTS(t, dir, "b.ts", 640, 360, 2, 48000, 1)
	out := filepath.Join(dir, "out.mp4")

	if _, err := r.BuildVideoConcat(t.Context(), []string{a, b}, out, Metadata{}); !errors.Is(err, ErrParamsMismatch) {
		t.Errorf("320x240 + 640x360: err = %v, want ErrParamsMismatch", err)
	}
	aac := sineADTS(t, dir, "c.aac", 48000, 1, 2, "128k")
	if _, err := r.BuildVideoConcat(t.Context(), []string{a, aac}, out, Metadata{}); !errors.Is(err, ErrNoVideo) {
		t.Errorf("audio-only input: err = %v, want ErrNoVideo", err)
	}
	if _, err := r.BuildVideoConcat(t.Context(), nil, out, Metadata{}); err == nil {
		t.Error("no inputs: succeeded")
	}
	mustNotExist(t, out, out+".part")
}

// TestExtractVideoTSRoundTripAndMerge: a stored .mp4 goes back to TS without
// losing a packet and merges with a newer capture like any other run.
func TestExtractVideoTSRoundTripAndMerge(t *testing.T) {
	r := videoTools(t)
	dir := t.TempDir()
	s1 := avTS(t, dir, "s1.ts", 320, 240, 3, 48000, 1)
	s2 := avTS(t, dir, "s2.ts", 320, 240, 2, 48000, 1)
	stored := filepath.Join(dir, "stored.mp4")
	if _, err := r.BuildVideoConcat(t.Context(), []string{s1, s2}, stored, Metadata{}); err != nil {
		t.Fatalf("BuildVideoConcat: %v", err)
	}
	si := probeVideo(t, r, stored)

	ts := filepath.Join(dir, "remote.ts")
	if err := r.ExtractVideoTS(t.Context(), stored, ts); err != nil {
		t.Fatalf("ExtractVideoTS: %v", err)
	}
	ti := probeVideo(t, r, ts)
	if ti.Format != "mpegts" || ti.AudioPackets != si.AudioPackets || ti.VideoPackets != si.VideoPackets || !SameVideoParams(si, ti) {
		t.Errorf("extracted = %+v, stored %+v", ti, si)
	}
	mustNotExist(t, ts+".part")

	s3 := avTS(t, dir, "s3.ts", 320, 240, 2, 48000, 1)
	i3 := probeVideo(t, r, s3)
	merged := filepath.Join(dir, "merged.mp4")
	res, err := r.BuildVideoConcat(t.Context(), []string{ts, s3}, merged, Metadata{})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if res.Video.AudioPackets != si.AudioPackets+i3.AudioPackets {
		t.Errorf("merged audio packets = %d, want %d + %d", res.Video.AudioPackets, si.AudioPackets, i3.AudioPackets)
	}

	// An audio-only file is not a video object.
	aac := sineADTS(t, dir, "a.aac", 48000, 2, 1, "128k")
	m4a := m4aFrom(t, r, aac, filepath.Join(dir, "a.m4a"), 48000, 2)
	if err := r.ExtractVideoTS(t.Context(), m4a.Path, filepath.Join(dir, "x.ts")); !errors.Is(err, ErrNoVideo) {
		t.Errorf("audio-only mp4: err = %v, want ErrNoVideo", err)
	}
	mustNotExist(t, filepath.Join(dir, "x.ts"))
}

// TestBuildVideoTranscodeMixedSegments: a truncated 320x240 capture, an
// audio-only stretch (different rate and layout) and a 640x360 capture become
// one 320x240 file as long as the three together.
func TestBuildVideoTranscodeMixedSegments(t *testing.T) {
	r := videoTools(t)
	dir := t.TempDir()
	s1 := avTS(t, dir, "s1.ts", 320, 240, 4, 48000, 1)
	damageLastFrame(t, s1)
	aac := sineADTS(t, dir, "gap.aac", 44100, 2, 3, "128k")
	s3 := avTS(t, dir, "s3.ts", 640, 360, 2, 48000, 1)

	gap := scan(t, aac).Duration
	segs := []VideoSegment{{TS: s1}, {Audio: aac, Duration: gap}, {TS: s3}}
	want := probeVideo(t, r, s1).Duration + gap + probeVideo(t, r, s3).Duration

	out := filepath.Join(dir, "out.mp4")
	res, err := r.BuildVideoTranscode(t.Context(), segs, out, 320, 240, 25, "96k", Metadata{Title: "mixed"})
	if err != nil {
		t.Fatalf("BuildVideoTranscode: %v", err)
	}
	v := res.Video
	if v.VideoCodec != "h264" || v.Width != 320 || v.Height != 240 || v.FPS != 25 || v.PixFmt != "yuv420p" {
		t.Errorf("video = %s @ %g", describeVideo(v), v.FPS)
	}
	if v.AudioCodec != "aac" || v.SampleRate != 48000 || v.Channels != 1 {
		t.Errorf("audio = %s (want the first segment's 48 kHz mono)", describeVideo(v))
	}
	if d := abs(res.Duration - want); d > 300*time.Millisecond {
		t.Errorf("duration = %s, segments sum %s (off by %s)", res.Duration, want, d)
	}
	if res.Tags["title"] != "mixed" {
		t.Errorf("tags = %v", res.Tags)
	}
	mustNotExist(t, out+".part")
}

func TestBuildVideoTranscodeValidatesArguments(t *testing.T) {
	r := &Remuxer{FFmpegPath: "/nonexistent/ffmpeg", FFprobePath: "/nonexistent/ffprobe"}
	ctx := t.Context()
	seg := []VideoSegment{{TS: "x.ts"}}
	cases := map[string]func() error{
		"no segments": func() error {
			_, err := r.BuildVideoTranscode(ctx, nil, "o.mp4", 320, 240, 25, "128k", Metadata{})
			return err
		},
		"zero size": func() error {
			_, err := r.BuildVideoTranscode(ctx, seg, "o.mp4", 0, 240, 25, "128k", Metadata{})
			return err
		},
		"zero fps": func() error {
			_, err := r.BuildVideoTranscode(ctx, seg, "o.mp4", 320, 240, 0, "128k", Metadata{})
			return err
		},
		"no bitrate": func() error {
			_, err := r.BuildVideoTranscode(ctx, seg, "o.mp4", 320, 240, 25, "", Metadata{})
			return err
		},
		"both set": func() error {
			_, err := r.BuildVideoTranscode(ctx, []VideoSegment{{TS: "a", Audio: "b"}}, "o.mp4", 320, 240, 25, "128k", Metadata{})
			return err
		},
		"none set": func() error {
			_, err := r.BuildVideoTranscode(ctx, []VideoSegment{{}}, "o.mp4", 320, 240, 25, "128k", Metadata{})
			return err
		},
	}
	for name, f := range cases {
		if err := f(); err == nil {
			t.Errorf("%s: succeeded", name)
		}
	}
}

func TestVerifyVideoRejects(t *testing.T) {
	good := VideoInfo{
		HasVideo: true, VideoCodec: "h264", Width: 320, Height: 240,
		AudioCodec: "aac", AudioPackets: 100, VideoPackets: 50, Duration: 10 * time.Second, Format: "mov,mp4,m4a,3gp,3g2,mj2",
	}
	want := videoExpect{
		VideoCodec: "h264", Width: 320, Height: 240, AudioCodec: "aac", AudioPackets: 100,
		VideoPacketsMin: 48, VideoPacketsMax: 50, MinDuration: 9 * time.Second, MaxDuration: 11 * time.Second,
	}
	if err := verifyVideo("x", good, want); err != nil {
		t.Fatalf("good: %v", err)
	}
	cases := []struct {
		name string
		mut  func(*VideoInfo)
		is   error
	}{
		{"container", func(v *VideoInfo) { v.Format = "mpegts" }, ErrParamsMismatch},
		{"no video", func(v *VideoInfo) { v.HasVideo = false }, ErrNoVideo},
		{"empty video", func(v *VideoInfo) { v.VideoPackets = 0 }, ErrNoVideo},
		{"no audio", func(v *VideoInfo) { v.AudioCodec = "" }, ErrNoAudio},
		{"codec", func(v *VideoInfo) { v.VideoCodec = "hevc" }, ErrParamsMismatch},
		{"size", func(v *VideoInfo) { v.Height = 360 }, ErrParamsMismatch},
		{"audio codec", func(v *VideoInfo) { v.AudioCodec = "mp3" }, ErrParamsMismatch},
		{"audio packet lost", func(v *VideoInfo) { v.AudioPackets = 99 }, ErrDurationMismatch},
		{"video packets low", func(v *VideoInfo) { v.VideoPackets = 47 }, ErrDurationMismatch},
		{"short", func(v *VideoInfo) { v.Duration = 8 * time.Second }, ErrDurationMismatch},
		{"long", func(v *VideoInfo) { v.Duration = 12 * time.Second }, ErrDurationMismatch},
	}
	for _, c := range cases {
		v := good
		c.mut(&v)
		if err := verifyVideo("x", v, want); !errors.Is(err, c.is) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.is)
		}
	}
}

func TestSameVideoParams(t *testing.T) {
	a := VideoInfo{HasVideo: true, VideoCodec: "h264", VideoProfile: "High", PixFmt: "yuv420p", Width: 1280, Height: 720,
		FPS: 25, AudioCodec: "aac", SampleRate: 48000, Channels: 2, Duration: time.Second, AudioPackets: 47}
	b := a
	b.FPS, b.Duration, b.AudioPackets = 50, time.Hour, 1 // not part of the sample description
	if !SameVideoParams(a, b) {
		t.Error("equal params reported different")
	}
	for name, mut := range map[string]func(*VideoInfo){
		"has video": func(v *VideoInfo) { v.HasVideo = false },
		"codec":     func(v *VideoInfo) { v.VideoCodec = "hevc" },
		"profile":   func(v *VideoInfo) { v.VideoProfile = "Main" },
		"pix_fmt":   func(v *VideoInfo) { v.PixFmt = "yuv420p10le" },
		"width":     func(v *VideoInfo) { v.Width = 1920 },
		"height":    func(v *VideoInfo) { v.Height = 1080 },
		"acodec":    func(v *VideoInfo) { v.AudioCodec = "mp3" },
		"rate":      func(v *VideoInfo) { v.SampleRate = 44100 },
		"channels":  func(v *VideoInfo) { v.Channels = 1 },
	} {
		c := a
		mut(&c)
		if SameVideoParams(a, c) {
			t.Errorf("%s differs but reported same", name)
		}
	}
}

func TestFFconcatQuote(t *testing.T) {
	got, err := ffconcatQuote(`/data/it's a \dir #1/x.ts`)
	if err != nil {
		t.Fatal(err)
	}
	if want := `'/data/it'\''s a \dir #1/x.ts'`; got != want {
		t.Errorf("quote = %s, want %s", got, want)
	}
	for _, bad := range []string{"/a\nb.ts", "/a\rb.ts", "/a\x00b.ts"} {
		if _, err := ffconcatQuote(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestTranscodeTimeoutScalesWithDuration(t *testing.T) {
	r := &Remuxer{}
	if got, want := r.transcodeTimeout(0, time.Hour), defaultBaseTimeout+4*time.Hour; got != want {
		t.Errorf("1 h media = %s, want %s", got, want)
	}
	r.MinTranscodeSpeed = 1
	if got, want := r.transcodeTimeout(10<<20, 10*time.Minute), defaultBaseTimeout+10*time.Second+10*time.Minute; got != want {
		t.Errorf("10 min media / 10 MiB at 1× = %s, want %s", got, want)
	}
}

func TestParseRate(t *testing.T) {
	for in, want := range map[string]float64{"25/1": 25, "30000/1001": 30000.0 / 1001, "0/0": 0, "": 0, "50": 50, "x/y": 0} {
		if got := parseRate(in); got != want {
			t.Errorf("parseRate(%q) = %g, want %g", in, got, want)
		}
	}
}
