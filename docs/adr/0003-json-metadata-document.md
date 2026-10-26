# ADR-0003: `.json` metadata document next to every recording

- **Status:** Accepted
- **Date:** 2026-10-26
- **Deciders:** Ladislav Soukup (requirement: a `.json` with the structured
  metadata already injected into the media file, for every uploaded
  recording), Claude (design)
- **Tags:** storage, metadata
- **Related:** [ADR-0001](0001-m4a-single-object-per-recording.md) (object
  metadata, MP4 tags, run table), [ADR-0002](0002-optional-video-companion.md)
  (the `.mp4` companion and the "goes first" ordering this reuses)

## Context

Everything the recorder knows about a recording lives in two places that are
awkward to consume: `x-amz-meta-*` on the `.m4a` (strings only, RFC 2047-encoded
names, a 2 KB budget, needs a `HEAD` per object) and the MP4 tags (needs an MP4
parser; the run table is a JSON string inside the `description` tag). The
requirement is a `.json` file with the same metadata stored with every
recording.

## Decision

* Every upload also stores `{base}.json` (the `.m4a` key with the suffix
  swapped; the raw `.aac` fallback shares the base), content type
  `application/json`, built from the same `objectSummary`/probe facts as the
  object metadata and from the exact `remux.Metadata` embedded in the
  container. The run table is decoded from the embedded `description`, so the
  document and the `.m4a` can never disagree. A `schema` field versions it.
* It is written **right before the `.m4a`** (after the `.mp4`). Once the `.m4a`
  lists a session no later attempt writes anything for it, so a document
  written after the audio could be lost for good by one failed PUT; written
  before, a failure simply retries the attempt, which rebuilds and rewrites it.
* It is **derived**, so it is written unconditionally (`Uploader.Overwrite`):
  every merge replaces it with the merged object's view. No manifest, no
  `If-Match`, no extra `HEAD`.
* **Audio always wins**, as for the `.mp4`: a transfer error retries the
  attempt (like one on the audio itself); a `PermanentError` is logged, counted
  (`recorder_metadata_uploads_total{result="failure"}`) and the audio is stored
  without the document.
* Always on — no switch. Kept mode (`UPLOAD_DISABLED=true`) does not write one
  (the local `<session>.json` name is already the session sidecar).

## Consequences

* One extra small PUT per upload attempt; the bucket gains one object per
  recording.
* If an attempt writes the document and then fails on the `.m4a`, the
  document briefly describes an object that is not stored yet; the retry
  rewrites it. If the group is then renamed (`-2`), the old key's document is
  orphaned. Both need a failed or raced `.m4a` put and are accepted.
* Recordings uploaded before this change have no document; nothing backfills
  them.
* Consumers should treat unknown fields as additive; `schema` changes only on
  an incompatible change.
