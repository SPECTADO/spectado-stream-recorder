package remux

// Video companion (.mp4 next to the .m4a, RECORD_VIDEO=true; see ADR 0002).
//
// Three ways to produce it, chosen by the caller:
//
//   - BuildBlackVideo: the source had no picture. A static 16x16 @ 1 fps black
//     H.264 track is put next to the stream-copied audio of the finished .m4a,
//     so the audio of the two objects is sample-identical.
//   - BuildVideoConcat: the source had video and every capture (MPEG-TS, one
//     file per ffmpeg run) has the same stream parameters. The concat demuxer
//     stream-copies them into one MP4. It rebases every file's timestamps (each
//     capture restart begins again at ~1.4 s), which a byte concatenation does
//     not: that one even fails under -xerror on the broken continuity counters.
//   - BuildVideoTranscode: anything mixed (different resolution/codec, or runs
//     with and without video) is re-encoded once through the concat filter;
//     audio-only pieces get a black picture of the target size.
//
// ExtractVideoTS turns an already stored .mp4 back into MPEG-TS so it can be
// the first input of a merge (lossless, packet counts verified).
//
// Verified with ffmpeg 9.0.1 (see the tests, which run the real binaries):
//
//   - A killed capture usually ends inside a PES packet. The MPEG-TS demuxer
//     flags that packet corrupt and -xerror then aborts the whole remux (exit
//     183) — even when the file was cut on a 188-byte boundary. Inputs are
//     therefore read with `-fflags +discardcorrupt`, which drops the flagged
//     packet before -xerror sees it; every other error still fails the run.
//     ProbeVideo counts packets with the same flag, so the audio packet count of
//     a concat is EXACTLY the sum of its inputs' counts.
//   - The last video frame of a killed capture is usually damaged without being
//     flagged (a short PES); a stream copy keeps it, a decoder conceals it.
//   - `-shortest` with a 1 fps picture drops every frame whose END lies past
//     the audio, so the video track ends up to 1 s before the audio and audio
//     shorter than ~1 s gets no video track at all. Short audio is therefore
//     given a fixed-length colour source instead.
//   - The concat demuxer offsets each file by the PREVIOUS file's probed
//     format duration. For a capture that was killed or restarted the audio
//     usually runs on past the last video frame, and the probed duration
//     follows the video: the next file's first audio packet then lands before
//     the previous file's last one and the mp4 muxer aborts ("Non-monotonic
//     DTS", exit 234). The list therefore carries an explicit `duration` for
//     every file but the last: the end of its longest stream (last packet's
//     pts + duration) minus its start_time. Each join then leaves a gap of
//     ~0.1–0.4 s at most (the shorter stream's tail); A/V sync inside each
//     file is kept.
//   - Stream-copied HEVC is muxed as `hev1` by default, which Apple players
//     (Safari, QuickTime, AVPlayer) refuse; it is tagged `hvc1` instead.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Black picture and transcode settings (constants by design, not env; ADR 0002).
const (
	BlackVideoWidth  = 16
	BlackVideoHeight = 16
	BlackVideoFPS    = 1

	// defaultMinTranscodeSpeed: libx264 veryfast handles 720p/1080p at several
	// times real time on one modern core, so 0.25× is a stuck-or-starved host.
	defaultMinTranscodeSpeed = 0.25

	// blackDurationTolerance: the black track is whole seconds long (1 fps), so
	// the container may differ from the audio by up to one frame plus rounding.
	blackDurationTolerance = 1500 * time.Millisecond
	// shortestMinAudio: below this, -shortest would drop the only video frame
	// (its end, 1 s, lies past the audio), so a fixed-length source is used.
	shortestMinAudio = 1100 * time.Millisecond
	// concatJoinSlack: the concat demuxer may add up to this per input (the gap
	// at each join, measured 0.1–0.4 s).
	concatJoinSlack = time.Second
	// transcodeSegmentSlack: the concat filter ends each segment with its
	// longer stream and the AAC encoder pads; measured < 0.1 s for 3 segments.
	transcodeSegmentSlack = 500 * time.Millisecond
	// transcodeMaxErrorRate replaces -xerror for a re-encode: a killed capture's
	// last frame is always "corrupt decoded frame" (concealed), which -xerror
	// turns into exit 183, while real decode errors above this fraction still
	// fail the run (exit 69).
	transcodeMaxErrorRate = "0.05"
)

// ErrNoVideo is returned (wrapped) when a video input or a produced file has
// no video stream.
var ErrNoVideo = errors.New("remux: no video stream")

// VideoInfo is ffprobe's view of an A/V file: the first video and the first
// audio stream, with packet counts read from the data (`-count_packets`, so it
// works for MPEG-TS, which carries no sample table, as well as for MP4).
type VideoInfo struct {
	HasVideo     bool
	VideoCodec   string // e.g. "h264", "hevc"
	VideoProfile string // e.g. "High", "Constrained Baseline"
	VideoTag     string // codec_tag_string, e.g. "avc1", "hvc1" (MP4); "[27][0][0][0]" (TS)
	PixFmt       string // e.g. "yuv420p"
	Width        int
	Height       int
	FPS          float64 // r_frame_rate (avg_frame_rate when that is unknown)

	AudioCodec string // e.g. "aac"; "" = no audio stream
	SampleRate int
	Channels   int

	Duration     time.Duration // format duration (the longer stream)
	StartTime    time.Duration // format start_time (the earliest stream's first pts)
	AudioPackets int           // packets of the first audio stream (corrupt ones discarded)
	VideoPackets int           // packets of the first video stream (corrupt ones discarded)
	Format       string        // ffprobe format_name, e.g. "mpegts"
}

// SameVideoParams reports whether a and b can share one MP4 track pair, i.e.
// be stream-copied into one file: an MP4 track has ONE sample description, so
// any difference here would play wrong after the join while ffmpeg exits 0.
func SameVideoParams(a, b VideoInfo) bool {
	return a.HasVideo == b.HasVideo &&
		a.VideoCodec == b.VideoCodec &&
		a.VideoProfile == b.VideoProfile &&
		a.Width == b.Width && a.Height == b.Height &&
		a.PixFmt == b.PixFmt &&
		a.AudioCodec == b.AudioCodec &&
		a.SampleRate == b.SampleRate &&
		a.Channels == b.Channels
}

// HasPicture reports whether the file really carries a picture: a video
// stream with a known size AND at least one packet. A capture killed before
// its first video packet (or fed by a variant that delivered no segments)
// still declares the stream in its PMT, but mapping it would fail the build
// ("at least one of its streams received no packets").
func (v VideoInfo) HasPicture() bool {
	return v.HasVideo && v.VideoPackets > 0 && v.Width > 0 && v.Height > 0
}

// videoProbeOutput mirrors the ffprobe JSON ProbeVideo asks for.
type videoProbeOutput struct {
	Streams []struct {
		CodecType     string `json:"codec_type"`
		CodecName     string `json:"codec_name"`
		CodecTag      string `json:"codec_tag_string"`
		Profile       string `json:"profile"`
		PixFmt        string `json:"pix_fmt"`
		Width         int    `json:"width"`
		Height        int    `json:"height"`
		RFrameRate    string `json:"r_frame_rate"`
		AvgFrameRate  string `json:"avg_frame_rate"`
		SampleRate    string `json:"sample_rate"`
		Channels      int    `json:"channels"`
		NbReadPackets string `json:"nb_read_packets"`
	} `json:"streams"`
	Format struct {
		FormatName string `json:"format_name"`
		Duration   string `json:"duration"`
		StartTime  string `json:"start_time"`
	} `json:"format"`
}

// ProbeVideo probes the first video and audio stream of path (.ts, .mp4, or
// raw ADTS). It reads every packet (`-count_packets`, ~1 GB/s), with
// `-fflags +discardcorrupt` so the counts are those the builders' inputs see;
// the bound therefore grows with the file size like an ffmpeg run's.
func (r *Remuxer) ProbeVideo(ctx context.Context, path string) (VideoInfo, error) {
	size, err := totalSize([]string{path})
	if err != nil {
		return VideoInfo{}, err
	}
	args := []string{
		"-v", "error", "-fflags", "+discardcorrupt", "-count_packets",
		"-show_entries", "format=format_name,duration,start_time:stream=codec_type,codec_name,codec_tag_string,profile,pix_fmt,width,height,r_frame_rate,avg_frame_rate,sample_rate,channels,nb_read_packets",
		"-of", "json", path,
	}
	// ProbeTimeout plus the byte term of timeoutFor (size / MinThroughput).
	stdout, err := r.runFFprobeFor(ctx, r.probeTimeout()+r.timeoutFor(size)-r.timeoutFor(0), args)
	if err != nil {
		return VideoInfo{}, fmt.Errorf("remux: probe %s: %w", path, err)
	}
	var out videoProbeOutput
	if err := json.Unmarshal(stdout, &out); err != nil {
		return VideoInfo{}, fmt.Errorf("remux: probe %s: parse ffprobe output: %w", path, err)
	}
	info := VideoInfo{Format: out.Format.FormatName, Duration: parseSeconds(out.Format.Duration),
		StartTime: parseSeconds(out.Format.StartTime)}
	seenV, seenA := false, false
	for _, s := range out.Streams {
		switch {
		case s.CodecType == "video" && !seenV:
			seenV = true
			info.HasVideo = true
			info.VideoCodec = s.CodecName
			info.VideoProfile = s.Profile
			info.VideoTag = s.CodecTag
			info.PixFmt = s.PixFmt
			info.Width, info.Height = s.Width, s.Height
			info.FPS = parseRate(s.RFrameRate)
			if info.FPS <= 0 {
				info.FPS = parseRate(s.AvgFrameRate)
			}
			info.VideoPackets = int(parseInt(s.NbReadPackets))
		case s.CodecType == "audio" && !seenA:
			seenA = true
			info.AudioCodec = s.CodecName
			info.SampleRate = int(parseInt(s.SampleRate))
			info.Channels = s.Channels
			info.AudioPackets = int(parseInt(s.NbReadPackets))
		}
	}
	return info, nil
}

// spanProbeWindow is how much of a file's end mediaSpan reads: the streams'
// last packets lie within a few seconds of each other (one HLS segment at
// most), so the tail is enough and a 4 h capture is not read twice.
const spanProbeWindow = 20 * time.Second

// mediaSpan is how long the file at path lasts on the concat timeline: the end
// of its longest stream (the latest pts + duration of any packet) minus its
// start_time. Only the last spanProbeWindow of media is read (the whole file
// when it is shorter, or when the seek finds no packet).
func (r *Remuxer) mediaSpan(ctx context.Context, path string, info VideoInfo) (time.Duration, error) {
	size, err := totalSize([]string{path})
	if err != nil {
		return 0, err
	}
	timeout := r.probeTimeout() + r.timeoutFor(size) - r.timeoutFor(0)
	read := func(from time.Duration) (time.Duration, bool, error) {
		args := []string{"-v", "error", "-fflags", "+discardcorrupt"}
		if from > 0 {
			// "+X%": from X after the start to the end of the file.
			args = append(args, "-read_intervals", "+"+seconds(from)+"%")
		}
		args = append(args, "-show_entries", "packet=pts_time,duration_time", "-of", "csv=p=0", path)
		stdout, err := r.runFFprobeFor(ctx, timeout, args)
		if err != nil {
			return 0, false, fmt.Errorf("remux: span of %s: %w", path, err)
		}
		var end float64
		found := false
		for _, line := range strings.Split(string(stdout), "\n") {
			f := strings.Split(strings.TrimSpace(line), ",")
			if len(f) < 2 {
				continue
			}
			pts, err := strconv.ParseFloat(f[0], 64)
			if err != nil {
				continue // N/A
			}
			d, _ := strconv.ParseFloat(f[1], 64)
			if e := pts + d; !found || e > end {
				end, found = e, true
			}
		}
		return time.Duration(end * float64(time.Second)), found, nil
	}
	from := info.Duration - spanProbeWindow
	end, found, err := read(from)
	if err == nil && !found && from > 0 {
		end, found, err = read(0)
	}
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("%w: %s has no timestamped packets", ErrNoAudio, path)
	}
	return end - info.StartTime, nil
}

// videoTagFor is the MP4 sample entry a stream copy of codec must carry.
func videoTagFor(codec string) string {
	if codec == "hevc" {
		return "hvc1"
	}
	return ""
}

// parseRate parses an ffprobe rational ("25/1", "30000/1001", "0/0").
func parseRate(s string) float64 {
	num, den, ok := strings.Cut(strings.TrimSpace(s), "/")
	if !ok {
		f, _ := strconv.ParseFloat(num, 64)
		return f
	}
	n, err1 := strconv.ParseFloat(num, 64)
	d, err2 := strconv.ParseFloat(den, 64)
	if err1 != nil || err2 != nil || d == 0 {
		return 0
	}
	return n / d
}

// ---------------------------------------------------------------------------
// builders
// ---------------------------------------------------------------------------

// BuildBlackVideo writes an .mp4 at out holding the audio of the .m4a at
// audioM4A (stream copy, sample-identical) and a static black H.264 picture
// (16x16 @ 1 fps, ultrafast/stillimage, a keyframe every 10 s), with meta.
// The picture follows the audio via -shortest; audio shorter than ~1 s (where
// -shortest would drop the only frame) gets a colour source of the audio's
// length instead.
//
// Verified: a 16x16 h264 track is present, the audio codec is the input's,
// audio packets == the input's exactly, and the duration is within 1.5 s of the
// input's. out is written atomically like Build's.
func (r *Remuxer) BuildBlackVideo(ctx context.Context, audioM4A, out string, meta Metadata) (Result, error) {
	in, err := r.ProbeVideo(ctx, audioM4A)
	if err != nil {
		return Result{}, err
	}
	if in.AudioCodec == "" || in.AudioPackets == 0 {
		return Result{}, fmt.Errorf("%w: %s", ErrNoAudio, audioM4A)
	}
	size, err := totalSize([]string{audioM4A})
	if err != nil {
		return Result{}, err
	}

	color := fmt.Sprintf("color=c=black:s=%dx%d:r=%d", BlackVideoWidth, BlackVideoHeight, BlackVideoFPS)
	shortest := in.Duration >= shortestMinAudio
	if !shortest {
		color += ":d=" + seconds(in.Duration)
	}
	args := []string{
		"-nostdin", "-hide_banner", "-nostats", "-loglevel", "level+warning", "-xerror", "-y",
		"-i", audioM4A,
		"-f", "lavfi", "-i", color,
		"-map", "1:v:0", "-map", "0:a:0", "-sn", "-dn",
		"-c:a", "copy",
		"-c:v", "libx264", "-preset", "ultrafast", "-tune", "stillimage", "-pix_fmt", "yuv420p", "-g", "10",
	}
	if shortest {
		args = append(args, "-shortest")
	}
	args = append(args, "-map_metadata", "-1")
	args = append(args, metadataArgs(meta)...)
	args = append(args, "-movflags", "+faststart", "-f", "mp4", out+".part")

	expect := videoExpect{
		VideoCodec: "h264", Width: BlackVideoWidth, Height: BlackVideoHeight,
		AudioCodec:   in.AudioCodec,
		AudioPackets: in.AudioPackets,
		MinDuration:  in.Duration - blackDurationTolerance,
		MaxDuration:  in.Duration + blackDurationTolerance,
	}
	return r.buildVideoAndVerify(ctx, args, r.timeoutFor(size), out, expect)
}

// BuildVideoConcat stream-copies the MPEG-TS captures tsFiles, in order, into
// one .mp4 at out through the concat demuxer (`-f concat -safe 0`, a
// temporary ffconcat list next to out, `-fflags +discardcorrupt`, `-xerror`,
// `-c copy`), with meta.
//
// Every input must have a picture (HasPicture) and audio and SameVideoParams
// as the first one (else ErrNoVideo/ErrParamsMismatch, nothing is run). Every
// file but the last gets an explicit `duration` in the list (mediaSpan: the
// end of its longest stream), so audio that runs on past the last video frame
// of a killed capture cannot overlap the next file. HEVC is tagged `hvc1`.
// The result is verified: same
// codecs and size as the inputs, audio packets == the sum of the inputs'
// EXACTLY, video packets within one per input below the sum, duration within
// [90 % of the inputs' sum, sum + 1 s per input]. out is written atomically.
func (r *Remuxer) BuildVideoConcat(ctx context.Context, tsFiles []string, out string, meta Metadata) (Result, error) {
	if len(tsFiles) == 0 {
		return Result{}, errors.New("remux: video concat: no input files")
	}
	size, err := totalSize(tsFiles)
	if err != nil {
		return Result{}, err
	}
	var first VideoInfo
	var audioPackets, videoPackets int
	var sum time.Duration
	spans := make([]time.Duration, len(tsFiles)) // 0 = no duration directive
	for i, p := range tsFiles {
		info, err := r.ProbeVideo(ctx, p)
		if err != nil {
			return Result{}, err
		}
		if !info.HasPicture() {
			return Result{}, fmt.Errorf("%w: %s", ErrNoVideo, p)
		}
		if info.AudioCodec == "" || info.AudioPackets == 0 {
			return Result{}, fmt.Errorf("%w: %s", ErrNoAudio, p)
		}
		if i < len(tsFiles)-1 {
			span, err := r.mediaSpan(ctx, p, info)
			if err != nil {
				return Result{}, err
			}
			spans[i] = span
		}
		if i == 0 {
			first = info
		} else if !SameVideoParams(first, info) {
			return Result{}, fmt.Errorf("%w: %s is %s, %s is %s", ErrParamsMismatch,
				tsFiles[0], describeVideo(first), p, describeVideo(info))
		}
		audioPackets += info.AudioPackets
		videoPackets += info.VideoPackets
		sum += info.Duration
	}

	list, err := writeConcatList(filepath.Dir(out), tsFiles, spans)
	if err != nil {
		return Result{}, err
	}
	defer os.Remove(list)

	args := []string{
		"-nostdin", "-hide_banner", "-nostats", "-loglevel", "level+warning", "-xerror", "-y",
		"-fflags", "+discardcorrupt",
		"-f", "concat", "-safe", "0", "-i", list,
		"-map", "0:v:0", "-map", "0:a:0", "-sn", "-dn",
		"-c", "copy",
	}
	if first.VideoCodec == "hevc" {
		args = append(args, "-tag:v", "hvc1")
	}
	args = append(args, "-map_metadata", "-1")
	args = append(args, metadataArgs(meta)...)
	args = append(args, "-movflags", "+faststart", "-f", "mp4", out+".part")

	n := len(tsFiles)
	expect := videoExpect{
		VideoCodec: first.VideoCodec, Width: first.Width, Height: first.Height,
		VideoTag:        videoTagFor(first.VideoCodec),
		AudioCodec:      first.AudioCodec,
		AudioPackets:    audioPackets,
		VideoPacketsMin: videoPackets - n,
		VideoPacketsMax: videoPackets,
		MinDuration:     sum * 9 / 10,
		MaxDuration:     sum + time.Duration(n)*concatJoinSlack,
	}
	return r.buildVideoAndVerify(ctx, args, r.timeoutFor(size), out, expect)
}

// ExtractVideoTS writes the first video and audio stream of the .mp4 at mp4In
// to tsOut as MPEG-TS (stream copy, `-xerror`; the Annex B / ADTS conversion
// is inserted by ffmpeg) so the stored object can be the first input of a
// BuildVideoConcat. Verified: per-stream packet counts equal the input's.
// tsOut is written atomically.
func (r *Remuxer) ExtractVideoTS(ctx context.Context, mp4In, tsOut string) error {
	in, err := r.ProbeVideo(ctx, mp4In)
	if err != nil {
		return err
	}
	if !in.HasVideo {
		return fmt.Errorf("%w: %s", ErrNoVideo, mp4In)
	}
	if in.AudioCodec == "" {
		return fmt.Errorf("%w: %s", ErrNoAudio, mp4In)
	}
	size, err := totalSize([]string{mp4In})
	if err != nil {
		return err
	}
	part := tsOut + ".part"
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(part)
			_ = os.Remove(tsOut)
		}
	}()

	args := []string{
		"-nostdin", "-hide_banner", "-nostats", "-loglevel", "level+warning", "-xerror", "-y",
		"-i", mp4In,
		"-map", "0:v:0", "-map", "0:a:0", "-sn", "-dn",
		"-c", "copy",
		"-f", "mpegts", part,
	}
	if err := r.runFFmpeg(ctx, r.timeoutFor(size), args, nil); err != nil {
		return err
	}
	if err := os.Rename(part, tsOut); err != nil {
		return fmt.Errorf("remux: rename %s: %w", part, err)
	}
	got, err := r.ProbeVideo(ctx, tsOut)
	if err != nil {
		return err
	}
	if got.AudioPackets != in.AudioPackets || got.VideoPackets != in.VideoPackets {
		return fmt.Errorf("%w: extracted %s has %d video / %d audio packets, %s has %d / %d",
			ErrDurationMismatch, tsOut, got.VideoPackets, got.AudioPackets, mp4In, in.VideoPackets, in.AudioPackets)
	}
	ok = true
	return nil
}

// VideoSegment is one piece of a BuildVideoTranscode timeline: exactly one of
// TS (an A/V capture, or an extracted stored .mp4) and Audio (a raw ADTS file,
// pictured black) is set. Duration is the caller's measurement (the ADTS
// scanner's for Audio); zero means "use the probed format duration".
type VideoSegment struct {
	TS       string
	Audio    string
	Duration time.Duration
}

// BuildVideoTranscode re-encodes segs, in order, into one .mp4 at out with a
// w×h @ fps H.264 picture (libx264 veryfast, crf 23, yuv420p; odd sizes are
// rounded up to even) and AAC audio at bitrate (ffmpeg syntax, e.g. "128k"),
// with meta. Each segment's picture is scaled to fit and padded (aspect kept,
// SAR 1); audio segments, and TS segments without video, get a black picture
// of their duration. Audio is resampled to the first segment's rate and
// layout (`aresample=async=1` absorbs timestamp gaps). Everything is joined by
// the concat filter.
//
// Unlike the stream copies this runs without -xerror (the damaged last frame
// of a killed capture is "corrupt" to the decoder, concealed, and would abort
// it) but with `-max_error_rate 0.05`. The timeout grows with the media
// duration: timeoutFor(input bytes) + duration / MinTranscodeSpeed (default
// 0.25 ⇒ 4× real time). Verified: h264 w×h + aac present, duration within
// 1 s + 0.5 s per segment of the segments' sum. out is written atomically.
func (r *Remuxer) BuildVideoTranscode(ctx context.Context, segs []VideoSegment, out string, w, h int, fps float64, bitrate string, meta Metadata) (Result, error) {
	if len(segs) == 0 {
		return Result{}, errors.New("remux: video transcode: no segments")
	}
	if w <= 0 || h <= 0 || fps <= 0 || math.IsInf(fps, 0) || math.IsNaN(fps) {
		return Result{}, fmt.Errorf("remux: video transcode: invalid target %dx%d @ %g fps", w, h, fps)
	}
	if bitrate == "" {
		return Result{}, errors.New("remux: video transcode: no bitrate given")
	}
	w, h = w+w%2, h+h%2 // libx264 yuv420p needs even dimensions

	type input struct {
		path      string
		adts      bool
		hasVideo  bool
		duration  time.Duration
		audioRate int
		channels  int
	}
	inputs := make([]input, len(segs))
	paths := make([]string, len(segs))
	var total time.Duration
	for i, s := range segs {
		var in input
		switch {
		case s.TS != "" && s.Audio == "":
			in.path = s.TS
		case s.Audio != "" && s.TS == "":
			in.path, in.adts = s.Audio, true
		default:
			return Result{}, fmt.Errorf("remux: video transcode: segment %d must set exactly one of TS and Audio", i)
		}
		info, err := r.ProbeVideo(ctx, in.path)
		if err != nil {
			return Result{}, err
		}
		if info.AudioCodec == "" || info.AudioPackets == 0 {
			return Result{}, fmt.Errorf("%w: %s", ErrNoAudio, in.path)
		}
		// A TS that declares a video stream without carrying a packet of it
		// is pictured black like an audio segment.
		in.hasVideo = !in.adts && info.HasPicture()
		in.duration = s.Duration
		if in.duration <= 0 {
			in.duration = info.Duration
		}
		if in.duration <= 0 {
			return Result{}, fmt.Errorf("remux: video transcode: %s has no duration", in.path)
		}
		in.audioRate, in.channels = info.SampleRate, info.Channels
		inputs[i] = in
		paths[i] = in.path
		total += in.duration
	}
	size, err := totalSize(paths)
	if err != nil {
		return Result{}, err
	}

	fpsArg := strconv.FormatFloat(fps, 'f', -1, 64)
	args := []string{
		"-nostdin", "-hide_banner", "-nostats", "-loglevel", "level+warning",
		"-max_error_rate", transcodeMaxErrorRate, "-y",
	}
	var graph strings.Builder
	idx := 0
	for i, in := range inputs {
		audioIn := idx
		videoIn := idx
		if in.adts {
			args = append(args, "-f", "aac", "-i", in.path)
		} else {
			args = append(args, "-fflags", "+discardcorrupt", "-i", in.path)
		}
		idx++
		if !in.hasVideo {
			args = append(args, "-f", "lavfi", "-i",
				fmt.Sprintf("color=c=black:s=%dx%d:r=%s:d=%s", w, h, fpsArg, seconds(in.duration)))
			videoIn = idx
			idx++
		}
		fmt.Fprintf(&graph, "[%d:v:0]scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:-1:-1,setsar=1,fps=%s,format=yuv420p[v%d];",
			videoIn, w, h, w, h, fpsArg, i)
		fmt.Fprintf(&graph, "[%d:a:0]%s[a%d];", audioIn, audioNormalizer(inputs[0].audioRate, inputs[0].channels), i)
	}
	for i := range inputs {
		fmt.Fprintf(&graph, "[v%d][a%d]", i, i)
	}
	fmt.Fprintf(&graph, "concat=n=%d:v=1:a=1[v][a]", len(inputs))

	args = append(args,
		"-filter_complex", graph.String(),
		"-map", "[v]", "-map", "[a]", "-sn", "-dn",
		"-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-pix_fmt", "yuv420p",
		"-c:a", "aac", "-b:a", bitrate,
		"-map_metadata", "-1",
	)
	args = append(args, metadataArgs(meta)...)
	args = append(args, "-movflags", "+faststart", "-f", "mp4", out+".part")

	slack := transcodeTolerance + time.Duration(len(inputs))*transcodeSegmentSlack
	expect := videoExpect{
		VideoCodec: "h264", Width: w, Height: h,
		AudioCodec:  "aac",
		MinDuration: total - slack,
		MaxDuration: total + slack,
	}
	return r.buildVideoAndVerify(ctx, args, r.transcodeTimeout(size, total), out, expect)
}

// audioNormalizer makes one transcode segment's audio concatenable: the
// concat filter needs identical rate, sample format and layout on every input.
func audioNormalizer(rate, channels int) string {
	parts := []string{"sample_fmts=fltp"}
	if rate > 0 {
		parts = append(parts, "sample_rates="+strconv.Itoa(rate))
	}
	if l := channelLayout(channels); l != "" {
		parts = append(parts, "channel_layouts="+l)
	}
	return "aresample=async=1,aformat=" + strings.Join(parts, ":")
}

// transcodeTimeout is timeoutFor(inputBytes) plus media / MinTranscodeSpeed: a
// re-encode is bound by CPU per second of picture, not by bytes moved.
func (r *Remuxer) transcodeTimeout(inputBytes int64, media time.Duration) time.Duration {
	speed := r.MinTranscodeSpeed
	if speed <= 0 {
		speed = defaultMinTranscodeSpeed
	}
	if media < 0 {
		media = 0
	}
	return r.timeoutFor(inputBytes) + time.Duration(float64(media)/speed)
}

// ---------------------------------------------------------------------------
// verification
// ---------------------------------------------------------------------------

// videoExpect is what a video build is checked against; zero fields (and a
// zero packet range) are not checked.
type videoExpect struct {
	VideoCodec      string
	VideoTag        string // codec_tag_string, e.g. "hvc1"
	Width, Height   int
	AudioCodec      string
	AudioPackets    int // exact
	VideoPacketsMin int
	VideoPacketsMax int
	MinDuration     time.Duration
	MaxDuration     time.Duration
}

// buildVideoAndVerify is buildAndVerify for the video builders: run into
// out+".part", rename, probe (stream view + container view) and verify;
// everything is removed again on failure.
func (r *Remuxer) buildVideoAndVerify(ctx context.Context, args []string, timeout time.Duration, out string, expect videoExpect) (Result, error) {
	part := out + ".part"
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(part)
			_ = os.Remove(out)
		}
	}()

	started := time.Now()
	if err := r.runFFmpeg(ctx, timeout, args, nil); err != nil {
		return Result{}, err
	}
	if err := os.Rename(part, out); err != nil {
		return Result{}, fmt.Errorf("remux: rename %s: %w", part, err)
	}
	info, err := r.ProbeVideo(ctx, out)
	if err != nil {
		return Result{}, err
	}
	if err := verifyVideo(out, info, expect); err != nil {
		return Result{}, err
	}
	res, err := r.Probe(ctx, out)
	if err != nil {
		return Result{}, err
	}
	res.Video = info
	ok = true
	r.debug("video built", "out", out, "video", describeVideo(info), "duration", info.Duration, "bytes", res.Size, "took", time.Since(started))
	return res, nil
}

// verifyVideo enforces a video build's contract; like verify, every message
// carries the measured and the expected value.
func verifyVideo(path string, got VideoInfo, want videoExpect) error {
	format := strings.ToLower(got.Format)
	if !strings.Contains(format, "mp4") {
		return fmt.Errorf("%w: %s container format is %q, want mp4", ErrParamsMismatch, path, got.Format)
	}
	if !got.HasVideo || got.VideoPackets == 0 {
		return fmt.Errorf("%w: %s", ErrNoVideo, path)
	}
	if got.AudioCodec == "" || got.AudioPackets == 0 {
		return fmt.Errorf("%w: %s", ErrNoAudio, path)
	}
	if want.VideoCodec != "" && got.VideoCodec != want.VideoCodec {
		return fmt.Errorf("%w: %s video codec is %q, want %q", ErrParamsMismatch, path, got.VideoCodec, want.VideoCodec)
	}
	if want.VideoTag != "" && got.VideoTag != want.VideoTag {
		return fmt.Errorf("%w: %s video is tagged %q, want %q", ErrParamsMismatch, path, got.VideoTag, want.VideoTag)
	}
	if want.Width > 0 && (got.Width != want.Width || got.Height != want.Height) {
		return fmt.Errorf("%w: %s picture is %dx%d, want %dx%d", ErrParamsMismatch, path, got.Width, got.Height, want.Width, want.Height)
	}
	if want.AudioCodec != "" && got.AudioCodec != want.AudioCodec {
		return fmt.Errorf("%w: %s audio codec is %q, want %q", ErrParamsMismatch, path, got.AudioCodec, want.AudioCodec)
	}
	if want.AudioPackets > 0 && got.AudioPackets != want.AudioPackets {
		return fmt.Errorf("%w: %s has %d audio packets, the inputs %d", ErrDurationMismatch, path, got.AudioPackets, want.AudioPackets)
	}
	if want.VideoPacketsMax > 0 && (got.VideoPackets < want.VideoPacketsMin || got.VideoPackets > want.VideoPacketsMax) {
		return fmt.Errorf("%w: %s has %d video packets, want %d..%d", ErrDurationMismatch, path, got.VideoPackets, want.VideoPacketsMin, want.VideoPacketsMax)
	}
	if want.MaxDuration > 0 && (got.Duration < want.MinDuration || got.Duration > want.MaxDuration) {
		return fmt.Errorf("%w: %s is %s, want %s..%s", ErrDurationMismatch, path, got.Duration, want.MinDuration, want.MaxDuration)
	}
	return nil
}

func describeVideo(v VideoInfo) string {
	video := "no video"
	if v.HasVideo {
		video = fmt.Sprintf("%s/%s %dx%d %s", v.VideoCodec, v.VideoProfile, v.Width, v.Height, v.PixFmt)
	}
	return fmt.Sprintf("%s + %s %d Hz %d ch", video, v.AudioCodec, v.SampleRate, v.Channels)
}

// ---------------------------------------------------------------------------
// ffconcat list
// ---------------------------------------------------------------------------

// writeConcatList writes an ffconcat list of paths (made absolute: the
// demuxer resolves relative entries against the list's directory) into dir as
// a `.tmp-` file, so a crash leaves nothing the recorder's temp reaper misses.
// A positive durations[i] becomes that file's `duration` directive (where the
// next file starts, relative to this one's start_time); nil means none.
func writeConcatList(dir string, paths []string, durations []time.Duration) (string, error) {
	var sb strings.Builder
	sb.WriteString("ffconcat version 1.0\n")
	for i, p := range paths {
		a, err := filepath.Abs(p)
		if err != nil {
			return "", fmt.Errorf("remux: concat list: %w", err)
		}
		line, err := ffconcatQuote(a)
		if err != nil {
			return "", err
		}
		sb.WriteString("file " + line + "\n")
		if i < len(durations) && durations[i] > 0 {
			sb.WriteString("duration " + seconds(durations[i]) + "\n")
		}
	}
	f, err := os.CreateTemp(dir, ".tmp-concat-*.ffconcat")
	if err != nil {
		return "", fmt.Errorf("remux: concat list: %w", err)
	}
	if _, err := f.WriteString(sb.String()); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", fmt.Errorf("remux: concat list: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("remux: concat list: %w", err)
	}
	return f.Name(), nil
}

// ffconcatQuote quotes a path for an ffconcat `file` directive: inside single
// quotes nothing is special, and a quote itself is written as quote, backslash,
// quote, quote (close, escaped quote, reopen) — the av_get_token rules the demuxer parses with.
// Line breaks and NUL cannot be represented in the line-based list.
func ffconcatQuote(p string) (string, error) {
	if strings.ContainsAny(p, "\n\r\x00") {
		return "", fmt.Errorf("remux: concat list: path %q contains a line break or NUL", p)
	}
	return "'" + strings.ReplaceAll(p, "'", `'\''`) + "'", nil
}

// seconds renders d for an ffmpeg option (lavfi d=, which takes seconds).
func seconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', 6, 64)
}
