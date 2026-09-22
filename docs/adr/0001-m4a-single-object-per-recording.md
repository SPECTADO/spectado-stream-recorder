# ADR-0001: Remux to .m4a and merge every recording into one bucket object

- **Status:** Accepted
- **Date:** 2026-09-22
- **Deciders:** Ladislav Soukup (requirements, format choice), Claude (design)
- **Tags:** storage, media-format, api

## Context

Two user reports drove this decision:

1. "Recorded files have broken timing: audio is fine, but time stamps in the
   audio file are bad — total length is mostly random, moving on the timeline
   is also partially broken."
2. "Before uploading, merge all the audio files per stream recorder into a
   single file (mostly it is a single file), upload only the audio file
   without playlist, and change the path to
   `/YYYY-MM-DD/{streamId}.m4a`" (example:
   `/2026-09-22/match-ro-jpOkle8Mp0.m4a`). "Upload to CF R2 is always a single
   file for a single recorded match."

**Root cause, verified locally with ffmpeg 9.0.1 (production ships 8.0.1).**
The recorder appended raw ADTS/AAC from `ffmpeg … -f adts pipe:1` to
`<session>.aac` and, every `CLOCK_ID3_INTERVAL` of media time, inserted an
ID3v2.4 tag between frames (Apple `PRIV`
`com.apple.streaming.transportStreamTimestamp` + `TXXX WALLCLOCK`). Raw ADTS
carries no duration or index: players and ffprobe *estimate* the length from
bit rate × size (ffprobe warns "Estimating duration from bitrate, this may be
inaccurate") and seek by byte-offset guesses. Anything that perturbs the
bytes-per-second ratio — VBR, copy vs transcode runs, the ~100 B ID3 tags,
~5 s overlaps after restarts — makes the reported length drift and seeks land
off-target. Measured: 20.032 s of audio probes as 20.11 s as `.aac`, 20.13 s
with the ID3 tags, exactly 20.032 s once remuxed into `.m4a`. HLS-aware
players additionally treat the Apple `PRIV` tag as a PTS base, so a tag
landing mid-segment jumps the timeline. **The container is the problem; the
tags make it worse.**

The second request additionally requires that a recording — which today can
already be more than one session (rotation via `MAX_SESSION_DURATION`, an
extension after the end, a removed-and-re-added item) — always ends up as
**exactly one object** in the bucket, with no playlist alongside it.

## Decision

We will remux to AAC-in-MP4 (`.m4a`) at upload time and merge every session of
a recording into a single bucket object, keeping raw ADTS as the on-disk
recording format:

* **Recording stays raw ADTS on disk.** Crash-safe append, resume, tail
  trimming and per-run records are unchanged — only the *uploaded* format
  changes. In-band ID3 wall-clock tags are removed; `CLOCK_ID3_INTERVAL` is
  retired (setting it now only logs a startup warning and is otherwise
  ignored). `CLOCK_PDT_LOOKUP` and the per-run wall-clock anchors (`runs[]`)
  stay — they now feed MP4 metadata instead of a playlist.
* **Remux with `ffmpeg -c:a copy`** (no re-encode) into `.m4a`
  (`-f ipod` → brand `M4A `, `-movflags +faststart`); the resulting sample
  table gives an exact duration and sample-accurate seeking.
* **Transcode fallback for heterogeneous streams.** A stream copy is only
  correct when every part shares the same AAC parameters (profile, sample
  rate, channel config, raw-data-blocks); measured proof: 48 kHz + 44.1 kHz
  concatenation stream-copied to 19.24 s instead of 20.03 s, exit code 0 — a
  silent corruption, not an error. When parameters are not uniform across the
  whole input, the affected chunks are re-encoded once through ffmpeg's
  `concat` filter instead, and the object is marked `transcoded=true`.
* **One object per recording:**
  `{S3_PREFIX}{YYYY-MM-DD of the scheduled start in KEY_DATE_TZ}/{safeId}.m4a`.
  No `index.m3u8`. Sessions sharing the same object key and recording id are
  merged into that object: locally when they are all still on disk (the
  upload of a finished session is deferred while a sibling of the same key is
  still recording, so rotation merges once, in order, at the end), and
  through a download → extract → concatenate → remux → conditional replace
  when the object already exists.
* **Conditional-writes probe.** Merging requires the storage endpoint to be
  proven, once at startup, to enforce `If-None-Match`/`If-Match` on both
  single-part and multipart uploads. An endpoint that silently ignores the
  header would let a merge overwrite someone else's object with no way to
  detect it, so merging is disabled — not attempted unconditionally — when the
  probe fails or is skipped (`S3_CONDITIONAL_PUT=false`).
* **Key pinning.** A session inherits an existing session's key verbatim
  (instead of recomputing one from today's date) when a same-recording-id
  session exists whose scheduled end + 1 h is still after the new session's
  start — this is what keeps an extension-after-the-end, a
  removed-and-re-added item, or a midnight-crossing rotation in the *same*
  object rather than splitting across two date folders.
* **Explicit schedule `key` names the object directly:**
  `{S3_PREFIX}{trim(key,"/")}` + `.m4a` (not duplicated if already present).
* **Fail closed on anything that isn't provably safe to merge into**: a
  foreign object (no/mismatched `recording-id` metadata), an inconsistent
  manifest, an out-of-order part, or a merge cap (20 parts) all rename the
  group to a `-2`/`-3`… key with an alertable metric, rather than risk
  corrupting or silently overwriting another object.
* **Remux failure never loses audio.** After three consecutive remux failures
  for a key, the sessions are uploaded as raw ADTS instead (marked
  `remux-failed=true`, `.aac` key, `audio/aac`), so a persistent ffmpeg
  problem degrades to "clearly marked raw file to fix by hand" rather than an
  unbounded retry loop that never gets audio into the bucket.
* **`UPLOAD_DISABLED=true` ("kept") remuxes locally** to `<session>.m4a` next
  to its sidecar; the `.aac` is deleted only after the remux succeeded and the
  sidecar durably says so. Kept mode does not merge multiple sessions.
* **Date zone of the key: UTC for now.** `KEY_DATE_TZ` (the zone used for the
  date component of the default key) defaults to `UTC`, unchanged from the
  1.0.x folder date. Decided by Ladislav Soukup on 2026-09-22 after the
  trade-off was raised (a match starting 00:00–02:00 CEST lands in the
  previous UTC day). Switching a deployment to local broadcast dates is a
  one-line `KEY_DATE_TZ=Europe/Prague` change, but it strands objects already
  written under UTC dates, so it should be done deliberately, not by default.
* **Version 1.1.0, not 2.0.0.** The bucket layout change is breaking for
  consumers of the old folder + playlist layout; Ladislav Soukup chose to ship
  it as 1.1.0 (2026-09-22), documented under "Breaking changes in 1.1.0" in
  the README.

## Consequences

**Easier:**
* Exact duration and sample-accurate seeking (the bug that started this work
  is fixed at the container level, not by tuning the ID3 cadence).
* A consumer gets exactly one URL per recording — no playlist to fetch or
  reconcile, matching "upload to CF R2 is always a single file for a single
  recorded match".
* The wall-clock → position map survives as the `description` JSON metadata,
  so restart overlaps/gaps remain inspectable even though there is no longer
  a playlist to show them as discontinuities.

**Harder / new costs:**
* Merging into an existing remote object costs a download + re-upload and
  transient local disk (~4× the object size, vs ~2× for a plain upload) —
  documented in the README's sizing section.
* The remux step's ffmpeg process uses ≈ 14 MB RSS per hour of 48 kHz audio
  (one in-memory sample-table entry per frame); a multi-hour recording's remux
  is measurably heavier than steady-state capture.
* A heterogeneous recording (source swap, codec fallback mid-session) is
  re-encoded once instead of staying a lossless copy.
* Upgrading from 1.0.x: a non-terminal session whose sidecar still has a
  `.aac` key is recomputed to the new `.m4a` key; the old-layout `.aac`
  object, if one was already uploaded under the old layout, is **not**
  migrated or deleted — it stays in the bucket alongside the new object.
* In-band ID3 wall-clock tags are removed; any downstream consumer that
  parsed them (e.g. via hls.js `FRAG_PARSING_METADATA`) must switch to
  reading the `.m4a`'s `description` metadata instead.
* Restart overlaps and gaps are still not trimmed — they were not trimmed by
  the playlist either — so a consumer must still map position through the
  `description` table rather than assume constant playback rate; this is
  unchanged behavior, only the table's format changed (JSON metadata, not
  `#EXT-X-DISCONTINUITY` entries).

## Alternatives considered

- **Keep ADTS, just fix the ID3 tags** (e.g. remove them or make the cadence
  configurable) — rejected: the root cause is raw ADTS having no
  duration/index at all, so even a tag-free ADTS file's duration is still
  bitrate-estimated and drifts with VBR or restart overlaps. This would not
  satisfy the single-file "always a single file for a single recorded match"
  requirement either.
- **fMP4 written directly by ffmpeg** (skip the ADTS intermediate, have ffmpeg
  write fragmented MP4 as it captures) — rejected: loses the crash-safe
  append/resume/tail-trim properties raw ADTS gives us for free, and would
  need a from-scratch resume story for interrupted fMP4 fragments.
- **A `-2` suffix for every later session of a recording** (never merge,
  always a new object) — rejected: does not satisfy "a single file for a
  single recorded match"; it just moves the multi-file problem from folders
  with a playlist to bare numbered objects with no index at all.
- **Keep local copies of everything until retention** instead of remuxing and
  uploading per session — rejected on capacity: at ~100 streams and 128
  kbit/s, that is ≈ 5.8 GB/h ≈ 17 GB/day of ADTS just for capture, and
  merging still has to happen eventually, so this only delays the same work.
- **ffmpeg's `concat` demuxer with `-c copy`** for merging ADTS parts —
  measured wrong: it offsets segments by ffmpeg's own bitrate-estimated
  durations (the same failure mode as the root cause), producing the same
  drifted timing the remux is meant to fix. Byte concatenation of the raw ADTS
  parts followed by one remux is exact instead (verified: 0 decode errors,
  exact summed duration).
