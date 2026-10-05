# ADR-0002: Optional `.mp4` video companion next to the `.m4a`

- **Status:** Accepted
- **Date:** 2026-10-05
- **Deciders:** Ladislav Soukup (requirements: an opt-in switch, the highest
  variant of a multi-variant master, stream copy with an H.264 fallback, a
  16x16 @ 1 fps black picture for audio-only sources, and both objects in the
  bucket), Claude (design)
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
* **The concat demuxer is correct for TS with copy.** Each capture restarts
  its timestamps, and the concat demuxer offsets every file by its probed
  duration. ADR-0001 rejected the concat demuxer for raw ADTS because ADTS
  durations are bitrate estimates; TS carries real PTS, so that objection
  does not apply. Every join adds about 0.1–0.4 s.
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
  * The audio decision is exactly what the audio-only probe would decide.
  * A recurring show inherits the previous session's decision, as it already
    does for the codec.
* **Second output in the same ffmpeg.** It is
  `-map <videoMap> -map 0:a:0 -c:v copy|libx264 -c:a <as ADTS> -f mpegts
  -flush_packets 1 file:<session>.vNNNN.ts`, one TS file per run.
  * `videoSeq` is persisted before ffmpeg starts.
  * The file is listed in `runs[].videoFiles`.
  * A run that produced no audio deletes its TS file. An empty TS file is not
    listed.
  * Stall detection still watches only the ADTS stdout.
* **Video problems never cost audio.**
  * A capture problem turns video off for the session; the audio keeps
    recording. After two consecutive empty runs with video active, the next
    run is a trial without video. If the trial produces audio, the session
    gets `videoSource=none` and `videoCaptureError`, and
    `recorder_video_capture_disabled_total` is incremented. If the trial is
    also empty, the source was down; video stays on and the counter resets.
    The audio copy→transcode fallback is not triggered by runs with video
    active.
  * A build problem retries the whole upload attempt. After three
    consecutive failures for a key, the `.m4a` is uploaded without its
    `.mp4` and `videoError` is set. It is never the other way round.
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
    duration) before it is used.
* **Same key, swapped suffix; same merge identity.** The `.mp4` carries the
  `.m4a`'s manifest metadata (`recording-id`, `sessions`, `parts`,
  `last-session-start`) plus `media=video`, `synthetic-video`, `video-codec`,
  `width`, `height` and `video-kind`. When merging, an existing `.mp4` whose
  manifest equals the remote `.m4a`'s and which is not synthetic is
  downloaded, and its video is extracted back to TS and reused. Otherwise the
  remote part becomes an audio segment, taken from the ADTS already extracted
  from the `.m4a`. A `.mp4` with a foreign or missing `recording-id` renames
  the group exactly as a foreign `.m4a` does.
* **The `.mp4` is written unconditionally,** not with
  `If-None-Match`/`If-Match`. It is derived data and is always rebuilt from
  the `.m4a`'s own timeline. The `.m4a`'s conditional write stays the single
  point of consistency, and a stale or out-of-sync `.mp4` shows up through
  the manifest comparison above.
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
* The `.mp4` is not protected by conditional writes. Two writers racing on
  the same key could leave a `.mp4` that lags its `.m4a`. The next merge
  detects this through the manifest and rebuilds the video part as black,
  rather than trusting it.
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
  or gaps. The concat demuxer re-bases each file on its probed duration. For
  TS that duration is real PTS, not the bitrate estimate that made the concat
  demuxer wrong for ADTS.
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
