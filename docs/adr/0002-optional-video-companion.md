# ADR-0002: Optional `.mp4` video companion next to the `.m4a`

- **Status:** Accepted
- **Date:** 2026-10-05
- **Deciders:** Ladislav Soukup (requirements: an opt-in switch, the highest
  variant of a multi-variant master, stream copy with an H.264 fallback, a
  16x16 @ 1 fps black picture for audio-only sources, and both objects in the
  bucket), Claude (design)
- **Amended:** 2026-10-05 after an adversarial review (Claude; requested by
  Ladislav Soukup): failure attribution by the video output's own stderr,
  the outage-proof trial, the concat `duration` directive, `hvc1`, the
  variant's own audio, reuse of an already stored result, and the
  disk-preflight split. Amended again the same day after a second review
  round (Claude): the variant's audio must match the first audio's codec AND
  profile (with one retry on `0:a:0`), the `.mp4` put is conditional where
  the endpoint enforces it (`Overwrite` only where it does not), a video
  disk shortage is waited out for 30 minutes instead of counting as a build
  failure, the copy-rejection check reads only the current run's stderr, and
  a give-up keeps `videoUploaded` for sessions the stored `.mp4` holds.
- **Tags:** media-format, storage, ffmpeg
- **Related:** [ADR-0001](0001-m4a-single-object-per-recording.md) (the `.m4a`,
  one object per recording); this ADR adds a companion object and leaves it
  unchanged.

## Context

Some scheduled sources are video streams (HLS with H.264/HEVC). Until now
the recorder kept only their first audio stream (`-vn -map 0:a:0`). The new
requirement is to keep the picture too, as an option. When the option is on,
every recording gets a video file **in addition to** its `.m4a`. The `.m4a`
and everything ADR-0001 built around it must not change: its bytes, its key,
its merge rules and its raw-`.aac` fallback.

Requirements (Ladislav Soukup, 2026-10-05):

* An env switch, off by default. Off means behaviour is byte-for-byte what
  it is today.
* On means a `.mp4` under the same key as the `.m4a`, with the suffix
  swapped, content type `video/mp4`.
* If the source has video, record it. For a multi-variant HLS master, record
  the variant with the highest `BANDWIDTH`.
* Stream-copy the video (h264/hevc). Re-encode to H.264 only when the codec
  cannot be copied, or when the merged parts differ in codec, resolution or
  presence of video.
* If the source is audio-only, use a static black picture: 16x16, 1 fps,
  H.264. This means every recording has a `.mp4`, which keeps the consumer's
  side simple.

Verified with ffmpeg 9.0.1 locally and with the production `mwader/static-ffmpeg:8.0.1`:

* **Capture.** One ffmpeg can write the existing ADTS output to `pipe:1` and
  a second output (the video plus the same audio, `-f mpegts`) to a file.
  Options placed before an output URL apply only to that output, so the ADTS
  bytes stay identical to an audio-only run. This was checked byte-for-byte
  on both the copy path and the AAC-transcode path.
* **Multi-variant masters.** A single ffprobe call lists one program per
  variant with `tags.variant_bitrate`, and `-map 0:p:<id>:v:0` picks the
  intended variant. With only video mapped, ffmpeg downloads every segment of
  that variant and only the first segment of the others. However, the
  unchanged ADTS output's `-map 0:a:0` takes its audio from the
  **first-listed** variant. When that is not the highest one, a video run
  downloads the segments of two variants.
* **MPEG-TS is crash-safe like ADTS.** A capture cut at an arbitrary byte, on
  or off a 188-byte boundary, still probes. However, ffmpeg marks the packet
  cut mid-PES as corrupt, and `-xerror` aborts the concat with exit 183.
  Reading the inputs with `-fflags +discardcorrupt` drops just that packet.
  With it, audio packet counts add up **exactly** across a concat.
* **The concat demuxer works for TS with copy — given explicit durations.**
  Each capture restarts its timestamps, and the concat demuxer offsets every
  file by the previous file's probed duration. ADR-0001 rejected the concat
  demuxer for raw ADTS because ADTS durations are bitrate estimates; TS
  carries real PTS, so that objection does not apply. But the probed format
  duration follows the video, and a capture cut by SIGTERM or a restart
  usually has audio running on past its last video frame: the next file's
  first audio packet then lands before the previous file's last one, and the
  mp4 muxer aborts ("Non-monotonic DTS", exit 234 — found by `make
  e2e-video`). Writing an ffconcat `duration` for every file but the last
  (the end of its longest stream: last packet pts + duration − start_time,
  read from the file's last 20 s) fixes it. Every join then adds about
  0.1–0.4 s.
* **ffmpeg opens the video encoder lazily,** on the first decoded frame,
  after the ADTS output has already written audio. A video output that cannot
  start (e.g. libx264 refusing an odd size) kills runs that DID produce
  audio. Its lines are recognisable: `[vost#1:0/libx264 …]`, `[out#1/mpegts
  …]`, `[enc:libx264 …]`, `[libx264 …]`, the map spec or the TS path in an
  option error. With one input (#0) and the ADTS as output #0, `#1:`/`#1/`
  cannot match anything else.
* **Stream-copied HEVC** is muxed into MP4 as `hev1`; Safari, QuickTime and
  AVPlayer only play `hvc1` (`-tag:v hvc1`).
* **Black picture at 1 fps.** With `-shortest`, a 1 fps picture loses any
  frame whose end falls after the end of the audio. Audio under about 1 s
  therefore got no video track. For audio shorter than 1.1 s, the build uses
  `color=…:d=<audio duration>` instead.
* **Transcoding cannot use `-xerror`.** The damaged last frame of a killed
  capture decodes as "corrupt decoded frame", which `-xerror` turns into a
  failure. `-max_error_rate 0.05` still fails on genuinely broken input; a
  scrambled TS exits 69.

## Decision

We will add `RECORD_VIDEO` (default `false`). When it is on, the recorder
captures a second, MPEG-TS output per ffmpeg run and, at upload, builds a
`.mp4` companion object next to the `.m4a`, which stays exactly as it is.

* **The mode is pinned per session.** `Session.Video` is set from
  `RECORD_VIDEO` when the session is created and never changes. Toggling the
  env var affects only new sessions, and sidecars written by older versions
  are `Video=false`.
* **Decision probe, once per session.** It runs with the codec probe (one
  ffprobe call) and is persisted the same way.
  * `videoSource` is `none` for no video, `copy` for h264/hevc, and `x264`
    for any other video codec, which is re-encoded at capture.
  * `videoMap` is `0:p:<program>:v:0` for the highest `variant_bitrate` when
    two or more programs carry video, and `0:v:0` otherwise.
  * `videoAudioMap` is `0:p:<program>:a:0` when that program has an audio
    stream of the same codec AND profile as the first audio stream, and that
    profile can be stream-copied into ADTS/MPEG-TS (`copyableProfile`: not
    xHE-AAC/USAC, whose AOT 42 mpegts' ADTS sub-muxer refuses), so the
    `.mp4`'s sound comes from the playlist its picture does; else empty
    (`0:a:0`). If the video output still fails while `videoAudioMap` is set,
    it is cleared (persisted) and the next run retries with `0:a:0` before
    the second failure turns video off. The ADTS
    output keeps `0:a:0`, so the two-variant download noted above remains;
    it is documented, not avoided.
  * The audio decision is exactly what the audio-only probe would decide.
  * A recurring show inherits the previous session's decision, as it already
    does for the codec.
* **Second output in the same ffmpeg.** It is
  `-map <videoMap> -map <videoAudioMap|0:a:0> -c:v copy|libx264 -c:a <as ADTS> -f mpegts
  -flush_packets 1 file:<session>.vNNNN.ts`, one TS file per run.
  * `videoSeq` is persisted before ffmpeg starts.
  * The file is listed in `runs[].videoFiles`.
  * A run that produced no audio deletes its TS file. An empty TS file is not
    listed.
  * Stall detection still watches only the ADTS stdout.
* **Video problems never cost audio.**
  * A capture problem turns video off for the rest of the session
    (`videoSource=none`, `videoCaptureError`,
    `recorder_video_capture_disabled_total{reason}`); the audio keeps
    recording. Evidence decides, not byte counts alone:
    * Two consecutive runs that end (non-zero exit, or no audio) with an
      error line of the video output (see Context) turn video off
      (`output-error`), even when they wrote audio first.
    * Runs that produce no audio with video active and no such line are
      ambiguous (video fault or source outage). After two, the next run is a
      trial without video. An empty trial means the source was down: video
      stays on, the count restarts. A trial with audio is only a suspicion
      (an outage may have ended just then): the next run with video decides —
      audio clears it, empty again turns video off (`empty-runs`). The trial
      run is never cut short to find out (that would cost audio), so it stays
      audio-only until it ends.
    * The audio copy→transcode fallback works exactly as without video: a
      copy-rejection (`looksLikeCopyRejection` over the current run's stderr
      only, ignoring output-#1 lines) counts toward it even while video is
      active, and does not count against the video. Only the current run is
      read because the video output's own `[adts @ …]` sub-muxer error has no
      `#1` marker and must not outlive its run.
    * A source change resets the counters and clears `videoCaptureError`.
  * A build problem retries the whole upload attempt. After three
    consecutive failures for a key, the `.m4a` is uploaded without its
    `.mp4` and `videoError` is set. It is never the other way round. A
    permanent refusal of the `.mp4` put gives up the same way (no retry). A
    disk preflight that fits the audio but not the video is a retryable
    shortage, not a build failure: it is retried until it has lasted 30
    minutes without a break (`videoNoSpaceGiveUp`), then the `.m4a` is stored
    without the `.mp4`. On a give-up, sessions that a stored `.mp4` an
    earlier attempt extended already holds keep `videoUploaded`; only the
    others get `videoError`.
* **Building the companion at upload,** after the `.m4a` is built and before
  it is put:
  * The timeline is split into segments: the remote part when merging, then
    each run of each new session. A run with TS files is a **video
    segment**. A run without, or a session with `Video=false`, is an **audio
    segment**: that run's ADTS byte range.
  * **All audio segments** produce `BuildBlackVideo` from the merged `.m4a`:
    16x16, 1 fps, libx264 `ultrafast`/`stillimage`, audio stream-copied,
    `synthetic-video=true`.
  * **All video segments with identical parameters** produce
    `BuildVideoConcat`: the ffconcat demuxer, `-c copy`, no re-encode. The
    parameters compared are codec, profile, size and pix_fmt for video, and
    codec, sample rate and channels for audio.
  * **Anything mixed** produces `BuildVideoTranscode`: one `concat` filter
    pass scaled and padded to the first video segment's size and frame rate,
    libx264 `veryfast` CRF 23, AAC at the session bitrate. Audio segments
    get a black picture of that size.
  * Every output is ffprobe-verified (streams, codecs, packet counts,
    duration, and the `hvc1` tag for HEVC) before it is used.
  * A TS that declares a video stream but carries no video packet (killed
    before its first frame) has no picture: it is pictured black.
  * Run coalescing (`maxRuns`) never merges a run with TS files into one
    without (the `.mp4` uses a run's TS INSTEAD of its audio); with strictly
    alternating runs the merged pair loses its TS files (black, audio kept).
* **Same key, swapped suffix; same merge identity.** The `.mp4` carries the
  `.m4a`'s manifest metadata (`recording-id`, `sessions`, `parts`,
  `last-session-start`) plus `media=video`, `synthetic-video`, `video-codec`,
  `width`, `height` and `video-kind`. When merging, an existing `.mp4` whose
  manifest equals the remote `.m4a`'s and which is not synthetic is
  downloaded, and its video is extracted back to TS and reused. Otherwise the
  remote part becomes an audio segment, taken from the ADTS already extracted
  from the `.m4a`. Because the `.mp4` is put before the `.m4a`, a failed
  `.m4a` put leaves a `.mp4` AHEAD of the stored `.m4a`; the retry must not
  treat that as out of sync (it would rebuild the stored real picture as
  black, and the earlier TS files are gone). So a `.mp4` whose manifest is
  exactly the one this attempt will store is kept as it is (nothing built or
  put), and a real one holding the stored parts plus a prefix of this
  attempt's sessions is extended with the rest. A `.mp4` with a foreign or
  missing `recording-id` renames the group exactly as a foreign `.m4a` does.
  An attempt with nothing to add (everything already in the `.m4a`) HEADs the
  `.mp4` to record `videoUploaded`/`videoError`.
* **The `.mp4` is written conditionally where the endpoint enforces it**:
  `If-None-Match: *` when its HEAD found nothing, `If-Match` on the ETag it
  found otherwise, so a concurrent writer's companion under the same key is
  never replaced (`ErrObjectExists`/`ErrObjectChanged` restart the attempt,
  whose HEAD then finds the foreign `.mp4` and renames the group). Only on an
  endpoint known not to enforce conditional writes (the cached probe says
  no) is it written with `Uploader.Overwrite`, because an `If-Match` there
  would be refused every time. When the probe itself fails, the conditional
  put is used; an endpoint that cannot honour it refuses permanently, which
  costs the `.mp4`, never the `.m4a`.
* **Not produced:** the raw-`.aac` remux fallback uploads no `.mp4`, and this
  is logged. Kept mode (`UPLOAD_DISABLED=true`) builds `<session>.mp4` next to
  `<session>.m4a` using the same three rules (no remote part), and records it
  in `outputVideoFile`.
* **Fail fast at startup.** `RECORD_VIDEO=true` with an ffmpeg that lacks
  libx264 refuses to start. The check is
  `ffmpeg -h encoder=libx264` → `Encoder libx264`, because ffmpeg exits 0 even
  for an unknown encoder. The image's Docker smoke test builds a black
  TS→`.mp4` to keep the shipped build honest.

## Consequences

**Easier:**
* A consumer of a video show gets one `.mp4` per recording at a predictable
  key, alongside the unchanged `.m4a`. Audio-only consumers notice nothing.
* Every recording has a `.mp4`, so a player never needs a fallback for
  "no video". A synthetic one is marked `synthetic-video=true`.
* A plain video recording is a lossless stream copy, with no capture CPU and
  only a remux at upload.

**Harder / new costs:**
* Disk, egress and bucket storage grow with the video bitrate. A 2–5 Mbit/s
  source is 1–2 GB/h per stream, compared with about 58 MB/h of audio, and
  the audio is stored twice (once in the `.m4a`, once in the `.mp4`).
  `recording_bytes_total` counts only the ADTS. Disk preflight reserves
  1.2× the TS bytes, plus 3× the remote `.mp4` size when merging.
* With 16 MiB parts, the 10,000-part multipart limit caps an object at
  160 GB. Raise `UPLOAD_PART_SIZE` for very long high-bitrate recordings.
* A source whose first-listed variant is not the highest downloads two
  variants while capturing, because of the unchanged audio map.
* The `x264` capture path, and the transcode at upload, cost real CPU. The
  `x264` capture output is not limited by the global `-threads 1`, which
  applies to the input decoder only.
* On an endpoint without conditional writes the `.mp4` is overwritten
  unconditionally, so two writers racing on the same key could leave a
  `.mp4` that lags its `.m4a` (such an endpoint never merges `.m4a`s
  either). The next merge detects this through the manifest and rebuilds
  the video part as black, rather than trusting it. The same happens after a
  video give-up (the `.m4a` moves on without the `.mp4`): the next merge
  blacks out the stored part.
* A disk shortage that fits the audio but not the video holds the `.m4a`
  back for up to 30 minutes before it is stored without the `.mp4`.
* A trial run that produces audio (after empty runs with video) records
  without picture until it ends, even if the cause was an outage that ended
  just then.
* Every concat join adds about 0.1–0.4 s, so the `.mp4` can be slightly
  longer than the `.m4a`. Use the `.m4a`'s `description` table for
  wall-clock mapping.
* Capture does not check that a TS file starts on a keyframe. HLS segments
  normally do.

## Alternatives considered

- **Black picture at capture time** (a `lavfi color` input mixed into the
  ffmpeg run of audio-only sources) — rejected. It adds a second input and an
  encoder to every audio-only capture for the whole recording, couples the
  capture's liveness to a synthetic source, and stores a video track for
  hours when it can be generated from the finished `.m4a` in seconds at
  upload. Generating it at upload also makes the "no video" case the same
  code path as "video capture disabled" and "merge with a session recorded
  before the switch".
- **Byte-concatenation of the TS files** (as ADR-0001 does for ADTS) —
  rejected. Each capture restarts its PTS/PCR and continuity counters, so a
  byte-cat has timestamp jumps that the mp4 muxer turns into wrong durations
  or gaps. The concat demuxer re-bases each file on the duration the list
  gives it, computed from real PTS (not the bitrate estimate that made the
  concat demuxer wrong for ADTS, nor the video-led probed duration).
- **A separate ffmpeg process for video** (a second connection to the same
  source) — rejected. It doubles origin load and segment downloads, gives
  two independently-timed captures that drift apart and restart separately,
  and doubles the process, thread and pid budget at ~100 streams. A second
  output of the same process shares one demuxer, so the audio in the TS file
  is the same audio as the ADTS.
- **A single `.mp4` replacing the `.m4a`** — rejected. It would change the
  format of the object every existing consumer reads, break merges into
  `.m4a` objects already in the bucket (ADR-0001), and tie audio delivery to
  the riskier video path. A companion keeps the audio path identical and
  makes "video failed" degrade to "audio only", never the reverse.
