# ADR-0004: Opaque `streamInfo` from the schedule into the metadata document

- **Status:** Accepted
- **Date:** 2026-10-29
- **Deciders:** Ladislav Soukup (requirement VAT-858: the LiveSport API passes
  match/commentator metadata in `/recorderSchedule` as `streamInfo`, the
  recorder stores it 1:1, no schema check), Claude (design)
- **Tags:** schedule, metadata
- **Related:** [ADR-0003](0003-json-metadata-document.md)

## Context

Recordings of LiveSport matches need to be findable by match, commentator,
language, competition, sport and teams. The schedule source knows all of that;
the recorder does not and should not. The set of fields will grow on the API
side.

## Decision

* A schedule item may carry `streamInfo` (alias `stream_info`): any JSON
  value. The recorder keeps the raw bytes (`json.RawMessage`) on the item and
  on the session sidecar and writes them into the `.json` metadata document as
  `streamInfo`. It is **not validated** and **not interpreted** — the API owns
  its schema; the recorder never rejects an item because of it.
* Like every other item field it follows the schedule while recording (latest
  value wins). When several sessions are merged into one object, the newest
  session that has a value wins (same match, so they normally agree).
* Only the `.json` document carries it: not `x-amz-meta-*` (2 KB budget,
  strings only) and not the MP4 tags.
* No size cap: the source is our own API.

## Consequences

* New fields need no recorder release; `schema` of the document stays 1 (an
  additive field).
* If every local session of a recording is gone and only the stored object
  remains, a later merge rebuilds the document without `streamInfo` (it is not
  in the object metadata). That needs a session of the same id without
  `streamInfo`, i.e. the API dropping it, and is accepted.
* `streamInfo` is visible in `/api/schedule` and the sidecars; the API must not
  put secrets into it.
