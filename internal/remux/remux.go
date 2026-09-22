// Package remux turns the recorder's raw ADTS captures into playable .m4a files
// (MP4 container, stream copy — no re-encoding) and back, using the ffmpeg and
// ffprobe binaries the recorder already depends on.
//
// Why: raw ADTS carries no duration or index, so players estimate the length
// from bit rate × size and seek by byte-offset guesses ("Estimating duration
// from bitrate, this may be inaccurate"). An MP4 sample table gives the exact
// duration and sample-accurate seeking. ADTS ↔ MP4 stream copy is lossless in
// both directions (verified byte-identical), so several captures — including
// one already stored as .m4a — can be merged by extracting to ADTS,
// concatenating and remuxing once.
//
// Limits of a stream copy: an MP4 track has ONE sample description and ONE
// timescale. If the captured stream changed sample rate, channel layout,
// profile or raw-data-block count halfway (encoder failover at the origin), a
// copy silently yields a short, pitched or one-eared file with exit code 0.
// Callers detect that with the ADTS scanner (adts.Info.ParamsChanged) and use
// BuildTranscode, which re-encodes homogeneous chunks through the concat filter.
package remux

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spectado/stream-recorder/internal/adts"
)

// Remuxer wraps the ffmpeg/ffprobe binaries. The zero value is not usable;
// FFmpegPath and FFprobePath are required.
type Remuxer struct {
	FFmpegPath  string
	FFprobePath string
	Log         *slog.Logger // optional; nil disables logging

	// BaseTimeout and MinThroughput bound one ffmpeg run:
	// timeout = BaseTimeout + inputBytes / MinThroughput. Zero values use the
	// defaults (5 min, 1 MiB/s).
	BaseTimeout   time.Duration
	MinThroughput int64 // bytes per second
	// ProbeTimeout bounds one ffprobe run (default 60 s).
	ProbeTimeout time.Duration
}

// Metadata is what Build embeds in the MP4 container (iTunes ilst + the movie
// header creation time). Empty fields are omitted. Values are passed as
// separate argv elements; newlines/NUL are replaced by spaces.
//
// Note: the mov/ipod muxer ignores `-metadata encoder=…` (it always writes its
// own Lavf identification), so there is deliberately no Encoder field.
type Metadata struct {
	CreationTime time.Time // wall clock of the first sample (mvhd/tkhd/mdhd creation_time, UTC, second precision); zero = omit
	Title        string    // iTunes ©nam
	Date         string    // iTunes ©day, "YYYY-MM-DD"
	Comment      string    // iTunes ©cmt
	Description  string    // iTunes desc — the recorder puts its JSON run table here
}

// Expect is what the caller knows about the ADTS input from adts.Scan/ScanRuns;
// Build verifies the produced file against it before reporting success.
type Expect struct {
	Frames   int           // complete ADTS frames in the input (must equal the MP4 sample count)
	Duration time.Duration // media time counted from the frame headers
	Params   adts.Params   // parameters of the (uniform) stream
}

// Result describes a probed media file.
type Result struct {
	Path       string
	Size       int64
	Duration   time.Duration // container duration as reported by ffprobe (format, falling back to the stream)
	StartTime  time.Duration // stream start_time (0 for a clean remux)
	Frames     int           // stream nb_frames (from the sample table; 0 when unknown)
	Format     string        // ffprobe format_name, e.g. "mov,mp4,m4a,3gp,3g2,mj2"
	Codec      string        // first audio stream codec_name, e.g. "aac"
	Profile    string        // e.g. "LC", "HE-AAC"
	SampleRate int
	Channels   int
	Tags       map[string]string // format tags (creation_time, title, description, …)
}

// ErrDurationMismatch is returned (wrapped) when a produced file's probed
// duration or sample count differs from what the ADTS input said: the remux
// dropped, duplicated or mistimed audio.
var ErrDurationMismatch = errors.New("remux: produced file does not match the source")

// ErrNoAudio is returned (wrapped) when a produced or probed file has no AAC
// audio stream.
var ErrNoAudio = errors.New("remux: no aac audio stream")

// ErrParamsMismatch is returned (wrapped) when the produced stream's sample
// rate or channel count does not match the ADTS headers.
var ErrParamsMismatch = errors.New("remux: produced stream parameters differ from the source")

const (
	// defaultBaseTimeout/defaultMinThroughput bound one ffmpeg run. A 26 h /
	// 238 MB capture remuxes in ~8 s on a developer machine, so 1 MiB/s is
	// three orders of magnitude of headroom for a loaded host.
	defaultBaseTimeout   = 5 * time.Minute
	defaultMinThroughput = 1 << 20
	defaultProbeTimeout  = 60 * time.Second

	// waitDelay gives a killed ffmpeg a moment to die and, more importantly,
	// makes exec close the pipes so Wait cannot block on a stuck child.
	waitDelay = 10 * time.Second

	// creationTimeLayout is what the mov/ipod muxer accepts for
	// -metadata creation_time. It stores second precision only (verified), the
	// sub-second digits are simply dropped.
	creationTimeLayout = "2006-01-02T15:04:05.000000Z"

	// startTimeTolerance: a clean remux has start_time exactly 0; allow one
	// millisecond of rounding in ffprobe's decimal rendering.
	startTimeTolerance = time.Millisecond
	// minDurationTolerance is the floor of the Build tolerance (the muxer
	// rounds the movie duration to the movie timescale, 1 ms in ffmpeg 8.0.1).
	minDurationTolerance = 250 * time.Millisecond
	// transcodeTolerance is the Build tolerance for a re-encode: the AAC
	// encoder adds priming and pads the last frame.
	transcodeTolerance = time.Second
)

// ---------------------------------------------------------------------------
// Build (stream copy)
// ---------------------------------------------------------------------------

// Build remuxes the raw ADTS files parts (byte-concatenated in order and
// streamed through ffmpeg's stdin, so no temporary copy is written) into an
// .m4a at out: stream copy (`-c:a copy`), MP4 with the `M4A ` brand
// (`-f ipod`), moov at the front (`-movflags +faststart`), input metadata
// dropped (`-map_metadata -1`; this is what removes legacy in-band ID3 tags),
// `-xerror` so a corrupt input fails instead of exiting 0, and meta embedded.
//
// It then Probes the result and enforces expect exactly: format contains
// "mp4" or "m4a", codec "aac", Frames == expect.Frames, |Duration −
// expect.Duration| ≤ max(250 ms, 3 frames), StartTime ≈ 0, SampleRate ∈
// {Params.SampleRate, 2×} (implicit SBR) and Channels == Params.Channels()
// unless that is 0 (PCE). Any violation removes out and returns a wrapped
// ErrDurationMismatch / ErrParamsMismatch / ErrNoAudio.
//
// out is written atomically: ffmpeg writes out+".part", renamed on success; on
// any error neither out nor the .part remain. The returned Result is the probe
// of out.
func (r *Remuxer) Build(ctx context.Context, parts []string, out string, expect Expect, meta Metadata) (Result, error) {
	if len(parts) == 0 {
		return Result{}, errors.New("remux: build: no input parts")
	}
	size, err := totalSize(parts)
	if err != nil {
		return Result{}, err
	}

	// -f aac names ffmpeg's raw ADTS demuxer. It is given explicitly because
	// stdin cannot be seeked back: format autodetection on a pipe depends on
	// the probe buffer and on what the capture happens to start with (a legacy
	// ID3 tag, a partial frame), while the caller already knows it is ADTS.
	args := []string{
		"-nostdin", "-hide_banner", "-nostats", "-loglevel", "level+warning", "-xerror", "-y",
		"-f", "aac", "-i", "pipe:0",
		"-map", "0:a:0", "-vn", "-sn", "-dn",
		"-c:a", "copy",
		"-map_metadata", "-1",
	}
	args = append(args, metadataArgs(meta)...)
	args = append(args, "-movflags", "+faststart", "-f", "ipod", out+".part")

	src := newPartsReader(parts)
	defer src.Close()

	// expect.Frames == 0 means the caller did not count frames (the recorder
	// always does); there is then nothing to compare the sample table against.
	return r.buildAndVerify(ctx, args, src, size, out, expect, expect.Frames > 0, durationTolerance(expect.Params))
}

// Chunk is one homogeneous piece of ADTS written to its own file (the caller
// extracts adts.Chunk byte ranges into temporary files).
type Chunk struct {
	Path   string
	Params adts.Params
}

// BuildTranscode produces an .m4a from chunks whose stream parameters differ
// (a single MP4 sample description cannot carry them): one `-i` per chunk,
// each normalised with `aresample=<rate>,aformat=channel_layouts=<layout>`
// (rate/layout of the first chunk; mono/stereo layouts by channel count) and
// joined with the `concat` filter, then encoded once with `-c:a aac -b:a
// bitrate` (ffmpeg bitrate syntax, e.g. "128k"). The result is verified like
// Build but with a 1 s duration tolerance and no frame-count check (the
// encoder adds priming/padding).
func (r *Remuxer) BuildTranscode(ctx context.Context, chunks []Chunk, out string, expectedDuration time.Duration, bitrate string, meta Metadata) (Result, error) {
	if len(chunks) == 0 {
		return Result{}, errors.New("remux: transcode: no input chunks")
	}
	if bitrate == "" {
		return Result{}, errors.New("remux: transcode: no bitrate given")
	}
	paths := make([]string, len(chunks))
	for i, c := range chunks {
		if c.Path == "" {
			return Result{}, fmt.Errorf("remux: transcode: chunk %d has no path", i)
		}
		paths[i] = c.Path
	}
	size, err := totalSize(paths)
	if err != nil {
		return Result{}, err
	}

	target := chunks[0].Params
	args := []string{"-nostdin", "-hide_banner", "-nostats", "-loglevel", "level+warning", "-xerror", "-y"}
	for _, c := range chunks {
		args = append(args, "-f", "aac", "-i", c.Path)
	}
	args = append(args,
		"-filter_complex", concatFilter(len(chunks), target),
		"-map", "[out]", "-vn", "-sn", "-dn",
		"-c:a", "aac", "-b:a", bitrate,
		"-map_metadata", "-1",
	)
	args = append(args, metadataArgs(meta)...)
	args = append(args, "-movflags", "+faststart", "-f", "ipod", out+".part")

	expect := Expect{Duration: expectedDuration, Params: target}
	return r.buildAndVerify(ctx, args, nil, size, out, expect, false, transcodeTolerance)
}

// concatFilter builds the filter graph that makes n heterogeneous inputs
// concatenable: the concat filter requires identical sample rate, layout and
// format on every input, so each is resampled to the first chunk's rate and
// forced to its channel layout. Layouts are only named for the configurations
// ffmpeg's ADTS headers can express unambiguously; for anything else the
// aformat is left out and the inputs must already agree (concat fails loudly
// otherwise, which is better than a silent downmix).
func concatFilter(n int, target adts.Params) string {
	var sb strings.Builder
	for i := range n {
		fmt.Fprintf(&sb, "[%d:a]", i)
		var filters []string
		if target.SampleRate > 0 {
			filters = append(filters, "aresample="+strconv.Itoa(target.SampleRate))
		}
		if l := channelLayout(target.Channels()); l != "" {
			filters = append(filters, "aformat=channel_layouts="+l)
		}
		if len(filters) == 0 {
			filters = append(filters, "anull")
		}
		sb.WriteString(strings.Join(filters, ","))
		fmt.Fprintf(&sb, "[a%d];", i)
	}
	for i := range n {
		fmt.Fprintf(&sb, "[a%d]", i)
	}
	fmt.Fprintf(&sb, "concat=n=%d:v=0:a=1[out]", n)
	return sb.String()
}

// channelLayout names the ffmpeg layout for a channel count, or "" when the
// count has no unambiguous name worth forcing.
func channelLayout(channels int) string {
	switch channels {
	case 1:
		return "mono"
	case 2:
		return "stereo"
	case 6:
		return "5.1"
	default:
		return ""
	}
}

// buildAndVerify runs one ffmpeg build into out+".part", renames it to out and
// probes it. Everything it created is removed again on any failure, so a caller
// never has to clean up after an error and a retry never appends to a stale
// file.
func (r *Remuxer) buildAndVerify(ctx context.Context, args []string, stdin io.Reader, inputBytes int64, out string, expect Expect, checkFrames bool, tolerance time.Duration) (Result, error) {
	part := out + ".part"
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(part)
			_ = os.Remove(out)
		}
	}()

	started := time.Now()
	if err := r.runFFmpeg(ctx, r.timeoutFor(inputBytes), args, stdin); err != nil {
		return Result{}, err
	}
	if err := os.Rename(part, out); err != nil {
		return Result{}, fmt.Errorf("remux: rename %s: %w", part, err)
	}
	res, err := r.Probe(ctx, out)
	if err != nil {
		return Result{}, err
	}
	if err := verify(res, expect, checkFrames, tolerance); err != nil {
		return Result{}, err
	}
	ok = true
	r.debug("remux built", "out", out, "frames", res.Frames, "duration", res.Duration, "bytes", res.Size, "took", time.Since(started))
	return res, nil
}

// ---------------------------------------------------------------------------
// Extract / Probe / Concat
// ---------------------------------------------------------------------------

// Extract writes the raw ADTS elementary stream of the .m4a at in to out
// (stream copy through the adts muxer, `-xerror`). Used to merge an object that
// is already stored as .m4a with newer ADTS captures; callers verify the result
// with adts.Scan against the probed Frames of in. out is written atomically.
func (r *Remuxer) Extract(ctx context.Context, in, out string) error {
	size, err := totalSize([]string{in})
	if err != nil {
		return err
	}
	part := out + ".part"
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(part)
			_ = os.Remove(out)
		}
	}()

	args := []string{
		"-nostdin", "-hide_banner", "-nostats", "-loglevel", "level+warning", "-xerror", "-y",
		"-i", in,
		"-map", "0:a:0", "-vn", "-sn", "-dn",
		"-c:a", "copy",
		"-f", "adts", part,
	}
	if err := r.runFFmpeg(ctx, r.timeoutFor(size), args, nil); err != nil {
		return err
	}
	if err := os.Rename(part, out); err != nil {
		return fmt.Errorf("remux: rename %s: %w", part, err)
	}
	ok = true
	return nil
}

// probeOutput mirrors the ffprobe JSON the Probe command asks for. Numbers that
// ffprobe renders as strings ("N/A" for unknown) are kept as strings and parsed
// leniently; channels is a real JSON number.
type probeOutput struct {
	Streams []struct {
		CodecName  string `json:"codec_name"`
		Profile    string `json:"profile"`
		SampleRate string `json:"sample_rate"`
		Channels   int    `json:"channels"`
		Duration   string `json:"duration"`
		NbFrames   string `json:"nb_frames"`
		StartTime  string `json:"start_time"`
	} `json:"streams"`
	Format struct {
		FormatName string            `json:"format_name"`
		Duration   string            `json:"duration"`
		Size       string            `json:"size"`
		Tags       map[string]string `json:"tags"`
	} `json:"format"`
}

// Probe returns ffprobe's view of the media file at path: format name, size,
// duration, format tags and the first audio stream's codec, profile, sample
// rate, channels, nb_frames and start_time.
func (r *Remuxer) Probe(ctx context.Context, path string) (Result, error) {
	args := []string{
		"-v", "error",
		"-show_entries", "format=format_name,duration,size:format_tags:stream=codec_name,profile,sample_rate,channels,duration,nb_frames,start_time",
		"-of", "json", path,
	}
	stdout, err := r.runFFprobe(ctx, args)
	if err != nil {
		return Result{}, fmt.Errorf("remux: probe %s: %w", path, err)
	}
	var out probeOutput
	if err := json.Unmarshal(stdout, &out); err != nil {
		return Result{}, fmt.Errorf("remux: probe %s: parse ffprobe output: %w", path, err)
	}

	res := Result{
		Path:   path,
		Format: out.Format.FormatName,
		Size:   parseInt(out.Format.Size),
		Tags:   out.Format.Tags,
	}
	if res.Size == 0 {
		if fi, err := os.Stat(path); err == nil {
			res.Size = fi.Size()
		}
	}
	// The show_entries selection above carries no codec_type, so the audio
	// stream is picked by the fields only an audio stream has. Every file this
	// package produces or consumes holds exactly one (audio) stream.
	for _, s := range out.Streams {
		if s.SampleRate == "" && s.Channels == 0 {
			continue
		}
		res.Codec = s.CodecName
		res.Profile = s.Profile
		res.SampleRate = int(parseInt(s.SampleRate))
		res.Channels = s.Channels
		res.Frames = int(parseInt(s.NbFrames))
		res.StartTime = parseSeconds(s.StartTime)
		if d := parseSeconds(s.Duration); d > 0 {
			res.Duration = d
		}
		break
	}
	// The format duration is the container's own (mvhd) duration and is what a
	// player shows, so it wins over the stream duration when present.
	if d := parseSeconds(out.Format.Duration); d > 0 {
		res.Duration = d
	}
	return res, nil
}

// Concat byte-concatenates the ADTS files parts, in order, into out (ADTS
// frames are self-delimiting, so concatenation is the merge). It returns the
// number of bytes written. out is written atomically (out+".part", fsync,
// rename); a missing or empty part is an error.
func Concat(parts []string, out string) (int64, error) {
	if len(parts) == 0 {
		return 0, errors.New("remux: concat: no input parts")
	}
	part := out + ".part"
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(part)
		}
	}()

	f, err := os.OpenFile(part, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, fmt.Errorf("remux: concat: create %s: %w", part, err)
	}
	defer f.Close()

	var written int64
	for _, p := range parts {
		fi, err := os.Stat(p)
		if err != nil {
			return written, fmt.Errorf("remux: concat: %w", err)
		}
		if fi.Size() == 0 {
			return written, fmt.Errorf("remux: concat: part %s is empty", p)
		}
		in, err := os.Open(p)
		if err != nil {
			return written, fmt.Errorf("remux: concat: %w", err)
		}
		n, err := io.Copy(f, in)
		written += n
		in.Close()
		if err != nil {
			return written, fmt.Errorf("remux: concat: copy %s: %w", p, err)
		}
	}
	// fsync before the rename: the caller deletes the sources once this file
	// exists, so its bytes must survive a crash.
	if err := f.Sync(); err != nil {
		return written, fmt.Errorf("remux: concat: fsync %s: %w", part, err)
	}
	if err := f.Close(); err != nil {
		return written, fmt.Errorf("remux: concat: close %s: %w", part, err)
	}
	if err := os.Rename(part, out); err != nil {
		return written, fmt.Errorf("remux: concat: rename %s: %w", part, err)
	}
	ok = true
	return written, nil
}

// ---------------------------------------------------------------------------
// verification
// ---------------------------------------------------------------------------

// durationTolerance is max(250 ms, 3 frames): the muxer rounds to the movie
// timescale and the scanner counts whole frames, so a few frames of slack is
// noise while anything larger means audio was dropped or mistimed.
func durationTolerance(p adts.Params) time.Duration {
	if p.SampleRate <= 0 {
		return minDurationTolerance
	}
	blocks := p.Blocks
	if blocks <= 0 {
		blocks = 1
	}
	d := time.Duration(3*1024*blocks) * time.Second / time.Duration(p.SampleRate)
	if d < minDurationTolerance {
		return minDurationTolerance
	}
	return d
}

// verify enforces the contract of Build/BuildTranscode on a probed result. Its
// messages always carry both the measured and the expected value: the operator
// sees from the log alone whether the remux was short, long or mis-parameterised.
func verify(res Result, expect Expect, checkFrames bool, tolerance time.Duration) error {
	format := strings.ToLower(res.Format)
	if !strings.Contains(format, "mp4") && !strings.Contains(format, "m4a") {
		return fmt.Errorf("%w: container format is %q, want mp4/m4a", ErrParamsMismatch, res.Format)
	}
	if res.Codec != "aac" {
		return fmt.Errorf("%w: %s has codec %q", ErrNoAudio, res.Path, res.Codec)
	}
	if checkFrames && res.Frames != expect.Frames {
		return fmt.Errorf("%w: %d samples in the file, %d frames in the source", ErrDurationMismatch, res.Frames, expect.Frames)
	}
	if d := res.Duration - expect.Duration; d > tolerance || d < -tolerance {
		return fmt.Errorf("%w: file is %s, source is %s (off by %s, tolerance %s)",
			ErrDurationMismatch, res.Duration, expect.Duration, d, tolerance)
	}
	if res.StartTime > startTimeTolerance || res.StartTime < -startTimeTolerance {
		return fmt.Errorf("%w: start_time is %s, want 0", ErrDurationMismatch, res.StartTime)
	}
	// An implicit-SBR (HE-AAC) stream keeps the ADTS base rate in the headers
	// but ffprobe reports the doubled output rate, so both are accepted. A zero
	// expected rate means the caller did not scan the input; nothing to check.
	if sr := expect.Params.SampleRate; sr > 0 && res.SampleRate != sr && res.SampleRate != 2*sr {
		return fmt.Errorf("%w: sample rate is %d Hz, source says %d Hz", ErrParamsMismatch, res.SampleRate, sr)
	}
	// channel_configuration 0 means the layout is defined by an in-band PCE and
	// cannot be known without decoding, so it is not checked.
	if ch := expect.Params.Channels(); ch > 0 && res.Channels != ch {
		return fmt.Errorf("%w: %d channels, source says %d", ErrParamsMismatch, res.Channels, ch)
	}
	return nil
}

// ---------------------------------------------------------------------------
// process plumbing
// ---------------------------------------------------------------------------

// timeoutFor bounds one ffmpeg run by the amount of data it has to move.
func (r *Remuxer) timeoutFor(inputBytes int64) time.Duration {
	base := r.BaseTimeout
	if base <= 0 {
		base = defaultBaseTimeout
	}
	tp := r.MinThroughput
	if tp <= 0 {
		tp = defaultMinThroughput
	}
	if inputBytes < 0 {
		inputBytes = 0
	}
	return base + time.Duration(inputBytes/tp)*time.Second
}

// runFFmpeg starts ffmpeg, streams stdin (when given) from a goroutine and
// waits for the process. Classification is by exit code only: every ADTS input
// makes ffmpeg print warnings ("Estimating duration from bitrate"), so a
// non-empty stderr says nothing about success — `-xerror` is what turns a
// corrupt input into a non-zero exit.
func (r *Remuxer) runFFmpeg(ctx context.Context, timeout time.Duration, args []string, stdin io.Reader) error {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, r.FFmpegPath, args...)
	cmd.WaitDelay = waitDelay
	cmd.Stdout = io.Discard
	tail := &stderrTail{}
	cmd.Stderr = tail

	var in io.WriteCloser
	if stdin != nil {
		var err error
		if in, err = cmd.StdinPipe(); err != nil {
			return fmt.Errorf("remux: ffmpeg stdin: %w", err)
		}
	}
	r.debug("running ffmpeg", "args", redactLine(strings.Join(args, " ")))
	if err := cmd.Start(); err != nil {
		if in != nil {
			in.Close()
		}
		return fmt.Errorf("remux: start ffmpeg: %w", err)
	}

	copyDone := make(chan error, 1)
	if in != nil {
		go func() {
			_, err := io.Copy(in, stdin)
			// Closing signals end of input; ffmpeg only finishes the file once
			// it sees EOF. A write error here means ffmpeg is already gone, in
			// which case Wait's exit code is the real story.
			cerr := in.Close()
			if err == nil && cerr != nil && !isPipeClosed(cerr) {
				err = cerr
			}
			copyDone <- err
		}()
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	var copyErr error
	var waitErr error
	waited := false
	if in != nil {
		// Whichever finishes first: if ffmpeg dies early the copy would block
		// on a full pipe forever, so it must not be waited for on its own.
		select {
		case copyErr = <-copyDone:
		case waitErr = <-waitCh:
			waited = true
		}
	}
	if !waited {
		waitErr = <-waitCh
	} else if in != nil {
		// Wait closed the parent end of the pipe, so the copier now fails its
		// next write and returns; the source is regular files, which never
		// block indefinitely.
		copyErr = <-copyDone
	}

	if waitErr != nil {
		return r.classify(cctx, ctx, waitErr, tail)
	}
	if copyErr != nil && !isPipeClosed(copyErr) {
		return fmt.Errorf("remux: feeding ffmpeg: %w", copyErr)
	}
	return nil
}

// classify turns ffmpeg's exit status into an error a human can act on: the
// timeout and the cancellation are named explicitly, and the last meaningful
// stderr line (URLs redacted, the ubiquitous bitrate warning dropped) is
// attached to a plain non-zero exit.
func (r *Remuxer) classify(runCtx, parentCtx context.Context, waitErr error, tail *stderrTail) error {
	if parentCtx.Err() != nil {
		return fmt.Errorf("remux: ffmpeg cancelled: %w", parentCtx.Err())
	}
	if runCtx.Err() != nil {
		return fmt.Errorf("remux: ffmpeg timed out: %w (last output: %s)", runCtx.Err(), tail.last())
	}
	var ee *exec.ExitError
	if errors.As(waitErr, &ee) {
		return fmt.Errorf("remux: ffmpeg exited %d: %s", ee.ExitCode(), tail.last())
	}
	return fmt.Errorf("remux: ffmpeg failed: %w (last output: %s)", waitErr, tail.last())
}

// runFFprobe runs ffprobe and returns its stdout.
func (r *Remuxer) runFFprobe(ctx context.Context, args []string) ([]byte, error) {
	timeout := r.ProbeTimeout
	if timeout <= 0 {
		timeout = defaultProbeTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, r.FFprobePath, args...)
	cmd.WaitDelay = waitDelay
	var stdout strings.Builder
	cmd.Stdout = &stdout
	tail := &stderrTail{}
	cmd.Stderr = tail

	err := cmd.Run()
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("ffprobe cancelled: %w", ctx.Err())
		}
		if cctx.Err() != nil {
			return nil, fmt.Errorf("ffprobe timed out: %w", cctx.Err())
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return nil, fmt.Errorf("ffprobe exited %d: %s", ee.ExitCode(), tail.last())
		}
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}
	return []byte(stdout.String()), nil
}

func (r *Remuxer) debug(msg string, args ...any) {
	if r.Log != nil {
		r.Log.Debug(msg, args...)
	}
}

// isPipeClosed reports whether err is the normal consequence of ffmpeg having
// exited while we were still writing to its stdin (EPIPE, or exec closing the
// parent end in Wait).
func isPipeClosed(err error) bool {
	return errors.Is(err, os.ErrClosed) || errors.Is(err, syscall.EPIPE) || strings.Contains(err.Error(), "broken pipe")
}

// ---------------------------------------------------------------------------
// stderr capture
// ---------------------------------------------------------------------------

const (
	maxStderrLines   = 20
	maxStderrLineLen = 1024
)

// stderrTail keeps the last few meaningful stderr lines of one run, bounded in
// both line count and line length so a chatty ffmpeg cannot grow the process.
// Benign noise is dropped so the error a caller logs is the one that matters.
type stderrTail struct {
	mu      sync.Mutex
	partial []byte
	lines   []string
}

func (s *stderrTail) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partial = append(s.partial, p...)
	for {
		i := bytes.IndexByte(s.partial, '\n')
		if i < 0 {
			break
		}
		s.push(string(s.partial[:i]))
		s.partial = s.partial[i+1:]
	}
	// A line that never terminates must not grow without bound either.
	if len(s.partial) > maxStderrLineLen {
		s.push(string(s.partial[:maxStderrLineLen]))
		s.partial = s.partial[:0]
	}
	return len(p), nil
}

func (s *stderrTail) push(line string) {
	line = strings.TrimRight(line, "\r")
	line = strings.TrimSpace(line)
	if line == "" || benignStderr(line) {
		return
	}
	if len(line) > maxStderrLineLen {
		line = line[:maxStderrLineLen] + "…"
	}
	s.lines = append(s.lines, redactLine(line))
	if len(s.lines) > maxStderrLines {
		s.lines = s.lines[len(s.lines)-maxStderrLines:]
	}
}

// last returns the most recent meaningful line, flushing an unterminated one
// (ffmpeg's fatal message may arrive without a trailing newline).
func (s *stderrTail) last() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rest := strings.TrimSpace(string(s.partial)); rest != "" {
		s.push(rest)
		s.partial = s.partial[:0]
	}
	if len(s.lines) == 0 {
		return "(no ffmpeg output)"
	}
	return s.lines[len(s.lines)-1]
}

// benignStderr reports lines that carry no information about success. Every
// ADTS input produces the bitrate estimation warning — that is exactly the
// defect this package exists to remove, so it must not be logged as a problem.
func benignStderr(line string) bool {
	return strings.Contains(line, "Estimating duration from bitrate")
}

// urlRe/redactLine mirror the recorder's redaction (this package cannot import
// internal/recorder, which imports it): stream URLs carry credentials and
// tokens and must never reach a log line or an error message.
var urlRe = regexp.MustCompile(`https?://[^\s"'<>\]\)]+`)

func redactLine(line string) string {
	return urlRe.ReplaceAllStringFunc(line, redactURL)
}

func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return raw
	}
	if u.User != nil {
		if _, has := u.User.Password(); has {
			u.User = url.UserPassword("REDACTED", "REDACTED")
		} else {
			u.User = url.User("REDACTED")
		}
	}
	if u.RawQuery != "" {
		q := u.Query()
		keys := make([]string, 0, len(q))
		for k := range q {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte('&')
			}
			sb.WriteString(url.QueryEscape(k))
			sb.WriteString("=REDACTED")
		}
		u.RawQuery = sb.String()
	}
	return u.String()
}

// ---------------------------------------------------------------------------
// input plumbing
// ---------------------------------------------------------------------------

// partsReader concatenates the parts for ffmpeg's stdin. Files are opened
// lazily and closed as soon as they are drained, so merging twenty parts costs
// one open file descriptor, and a part that disappears mid-run names itself in
// the error.
type partsReader struct {
	files []*lazyFile
	r     io.Reader
}

func newPartsReader(parts []string) *partsReader {
	p := &partsReader{files: make([]*lazyFile, len(parts))}
	readers := make([]io.Reader, len(parts))
	for i, path := range parts {
		p.files[i] = &lazyFile{path: path}
		readers[i] = p.files[i]
	}
	p.r = io.MultiReader(readers...)
	return p
}

func (p *partsReader) Read(b []byte) (int, error) { return p.r.Read(b) }

// Close releases any file still open (the copier is guaranteed to be finished
// by then; see runFFmpeg).
func (p *partsReader) Close() error {
	for _, f := range p.files {
		f.close()
	}
	return nil
}

type lazyFile struct {
	path string
	f    *os.File
	done bool
}

func (l *lazyFile) Read(b []byte) (int, error) {
	if l.done {
		return 0, io.EOF
	}
	if l.f == nil {
		f, err := os.Open(l.path)
		if err != nil {
			l.done = true
			return 0, fmt.Errorf("remux: open part: %w", err)
		}
		l.f = f
	}
	n, err := l.f.Read(b)
	if err != nil {
		l.close()
		l.done = true
		if !errors.Is(err, io.EOF) {
			return n, fmt.Errorf("remux: read part %s: %w", l.path, err)
		}
	}
	return n, err
}

func (l *lazyFile) close() {
	if l.f != nil {
		_ = l.f.Close()
		l.f = nil
	}
}

// totalSize sums the sizes of the inputs, failing early (before ffmpeg starts)
// when one of them is missing or empty.
func totalSize(paths []string) (int64, error) {
	var total int64
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			return 0, fmt.Errorf("remux: %w", err)
		}
		if fi.Size() == 0 {
			return 0, fmt.Errorf("remux: input %s is empty", p)
		}
		total += fi.Size()
	}
	return total, nil
}

// ---------------------------------------------------------------------------
// metadata
// ---------------------------------------------------------------------------

// metadataArgs renders Metadata as ffmpeg output options. Values are passed as
// their own argv elements, so no quoting is needed; only the characters that
// would break the muxer's key=value parsing are replaced.
func metadataArgs(meta Metadata) []string {
	var args []string
	add := func(key, value string) {
		if value == "" {
			return
		}
		args = append(args, "-metadata", key+"="+sanitizeMetaValue(value))
	}
	if !meta.CreationTime.IsZero() {
		add("creation_time", meta.CreationTime.UTC().Format(creationTimeLayout))
	}
	add("title", meta.Title)
	add("date", meta.Date)
	add("comment", meta.Comment)
	add("description", meta.Description)
	return args
}

// sanitizeMetaValue replaces the control characters that would split a value
// across lines (or truncate it at a NUL) with spaces; everything else,
// including UTF-8, survives the ilst atom unchanged.
func sanitizeMetaValue(v string) string {
	return strings.NewReplacer("\n", " ", "\r", " ", "\x00", " ").Replace(v)
}

// parseInt parses an ffprobe integer field, treating "" and "N/A" as 0.
func parseInt(s string) int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// parseSeconds parses an ffprobe seconds field ("20.032000", "N/A").
func parseSeconds(s string) time.Duration {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0
	}
	return time.Duration(f * float64(time.Second))
}
