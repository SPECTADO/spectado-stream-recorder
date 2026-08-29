# spectado-stream-recorder

Records many live audio streams (HLS or Icecast/HTTP) in parallel — one `ffmpeg`
per stream — into a single `.aac` file per scheduled show, then uploads each file
to Cloudflare R2 (or any S3-compatible bucket). Designed to run ~100 concurrent
recordings in one container.

* Schedule is a JSON document fetched periodically from a URL (default every 60 s).
  A failed fetch never changes anything: the last known good schedule stays in force.
* A recording starts when an item's window opens and stops **only** when its
  (latest known) end time has passed or the item disappears from the list.
* Crash/restart safe: unfinished recordings resume (appending to the same file),
  finished ones are uploaded, nothing is deleted before the upload is verified.
* Heartbeat, readiness, JSON state API and Prometheus metrics (recorder + system
  + container cgroup + per-stream ffmpeg CPU/RSS).

---

## Contents

1. [How it works](#how-it-works)
2. [Quick start (docker compose)](#quick-start-docker-compose)
3. [Schedule document](#schedule-document)
4. [Recording rules](#recording-rules)
5. [Output files and object keys](#output-files-and-object-keys)
6. [Configuration](#configuration)
7. [HTTP API](#http-api)
8. [Prometheus metrics](#prometheus-metrics)
9. [Deployment notes](#deployment-notes)
10. [CI / image tags](#ci--image-tags)
11. [Development](#development)
12. [Troubleshooting](#troubleshooting)

---

## How it works

```
 schedule URL ──poll──▶ Manager ──reconcile (1 s)──▶ per-item session
                          │                              │
                          │   sidecar <session>.json     ├─ supervisor goroutine
                          │   (state, bytes, key, …)     │    └─ ffmpeg … -f adts pipe:1 ──▶ append <session>.aac
                          │                              │       (restart with backoff, stall watchdog)
                          └─ finalize ─▶ upload queue ──▶ S3/R2 (multipart, verified) ─▶ delete local file
```

* **One process per stream.** `ffmpeg` reads the source (HLS or Icecast) and writes
  raw ADTS/AAC to stdout; the recorder appends it to the session file. ADTS frames
  are self-contained, so the file is playable at any moment and survives crashes.
* **Copy when possible.** With `AUDIO_CODEC=auto` the source is probed once; AAC
  sources are stream-copied (no re-encode, ~0 CPU), everything else (MP3, Vorbis, …)
  is transcoded to AAC at `AUDIO_BITRATE`. If copy produces no data twice in a row
  the session falls back to transcoding.
* **Sidecar per session.** Every recording session is described by a small JSON
  file next to the audio (`/data/recordings/<id>/<session>.json`). It is the
  source of truth after a restart: unfinished sessions resume, finished ones upload.
* **Upload after the end.** When the window closes the file is fsynced, closed,
  uploaded with the AWS SDK multipart uploader, verified with `HeadObject`
  (size must match) and only then deleted locally. Failures retry forever with
  backoff; permanent-looking errors (credentials, bucket) retry slowly and are
  flagged as *blocked*.

State machine per session:

```
recording ─▶ finalized ─▶ uploading ─▶ uploaded          (local file deleted)
    │            │                          
    │            └────────────────────────▶ kept          (UPLOAD_DISABLED=true)
    │                                       failed        (empty file / file missing)
    └── shutdown: stays "recording" with suspendedAt, resumed on next start
```

## Quick start (docker compose)

`docker-compose.example.yml` is a self-contained test setup: the recorder, a tiny
schedule server that serves a schedule whose times are relative to *now*, and an
ffmpeg container generating a live HLS test tone. Uploads are disabled so no R2
account is needed; recordings stay in the `recordings` volume.

```bash
cp .env.example .env                 # UPLOAD_DISABLED=true is active in the example
docker compose -f docker-compose.example.yml up --build

curl -s localhost:8080/api/state | jq .recordings
curl -s localhost:8080/metrics | grep recorder_recordings_active
curl -s localhost:8081/schedule.json       # what the recorder sees
```

Add Prometheus + Grafana (provisioned datasource, dashboard and alert rules from
`example/`):

```bash
docker compose -f docker-compose.example.yml --profile monitoring up --build
# Prometheus http://localhost:9090   Grafana http://localhost:3000 (admin/admin)
```

To record real streams, edit `example/schedule-server/extra-items.json` (two public
SomaFM streams are included but `"disabled": true`) or point `SCHEDULE_URL` at your
own document. To upload to R2 set the `S3_*` variables in `.env` and remove
`UPLOAD_DISABLED`.

## Schedule document

`GET SCHEDULE_URL` must return JSON — either an array of items or an object with
an `items` array (`streams` / `recordings` are accepted aliases). Unknown fields
are ignored.

```json
[
  {
    "id": "radio1-morning",
    "name": "Radio 1 – Morning Show",
    "type": "hls",
    "source": "https://cdn.example.com/radio1/live.m3u8",
    "start": "2026-08-30T06:00:00+02:00",
    "end":   "2026-08-30T09:00:00+02:00"
  },
  {
    "id": "radio2-evening",
    "type": "icecast",
    "source": "https://ice.example.com/radio2.mp3",
    "start": 1756576800,
    "end":   1756587600,
    "key": "radio2/2026-08-30/evening.aac",
    "codec": "aac",
    "bitrate": "96k",
    "headers": { "Authorization": "Bearer …" },
    "startEarly": "15s",
    "stopLate": "60s"
  }
]
```

| Field | Required | Meaning |
|---|---|---|
| `id` | yes | Stable, unique identifier. Used for the local directory, default object key and metric labels. Any string (or number); unsafe characters are replaced and a hash suffix added for file names. |
| `name` / `title` | no | Human readable, stored in object metadata. |
| `source` / `url` | yes | `http(s)` URL of an HLS playlist or an Icecast/HTTP stream. Prefer *media* playlists over master playlists. |
| `type` | no | `hls`, `icecast` or `auto` (default). Only used to pick HLS-specific ffmpeg options; `.m3u8` URLs are detected automatically. |
| `start`, `end` | yes | RFC 3339 (`2026-08-30T06:00:00+02:00`, fractional seconds allowed) or unix seconds (number or numeric string; > 1e11 is treated as milliseconds). Timestamps **without an offset** are interpreted in `SCHEDULE_DEFAULT_TZ` (default UTC). `end` must be after `start`. Aliases: `startTime`, `start_time`, `from`; `endTime`, `end_time`, `to`. |
| `key` / `objectKey` | no | Explicit object key in the bucket (`S3_PREFIX` is still prepended). Without it the default key is used (see below). |
| `codec` | no | `auto` (default, probe and copy AAC sources), `copy`, or `aac` (always transcode). |
| `bitrate` | no | Transcode bitrate (`"128k"` or a number in bit/s). Default `AUDIO_BITRATE`. |
| `headers` | no | Extra HTTP headers sent by ffmpeg (auth tokens, referer …). Never exposed by the API. |
| `startEarly`, `stopLate` | no | Per-item override of `RECORD_START_EARLY` / `RECORD_STOP_LATE` (`"15s"` or seconds). |
| `stallTimeout` | no | Per-item override of `FFMPEG_STALL_TIMEOUT` (≥ 5 s). |
| `insecureTLS` | no | Skip TLS verification for this source (only relevant with `FFMPEG_TLS_VERIFY=true`). |

Validation happens per item: an invalid entry is skipped and reported in
`/api/state` (`schedule.invalidItems`) and `recorder_schedule_invalid_items`,
while the valid ones are applied. A document in which **every** item is invalid is
treated as a failed fetch (a schema change upstream must not stop all recordings).
Duplicate `id`s: the first wins. Duplicate explicit `key`s: the second is invalid.

## Recording rules

* **Start**: an item starts recording when `now ≥ start − startEarly` and
  `now < end + stopLate`, unless a session that *ended normally* already covers
  this window (protects against duplicate sessions after a clock step-back).
* **Stop**: an active recording stops only when
  1. the **latest known** `end + stopLate` has passed — end changes made while
     recording are applied immediately (later end → keeps recording, earlier end → stops); or
  2. the item is **absent** from a *successfully fetched* schedule.
* A changed `start` never affects a running recording. A changed `source` is
  applied at the next ffmpeg restart. An item that becomes *invalid* in a later
  fetch (typo) is **not** treated as removed — the recording continues with its last
  valid definition and `scheduleNote` explains why.
* **Failed fetch** (network error, HTTP error, invalid JSON, all items invalid):
  logged and counted; the last known schedule stays in force. On startup the last
  good schedule is also loaded from `DATA_DIR/schedule.cache.json`.
* **Extension after the end**: if `end` is moved later after the session already
  finished, a new session (new file, new key with the session timestamp) records
  the extension. A removed and re-added item records again.
* **Restart / crash**: unfinished sessions found in `DATA_DIR` are resumed from
  their sidecar (no schedule needed), appending to the same file after trimming a
  possibly partial trailing ADTS frame. Finished sessions are queued for upload.
* **Graceful shutdown** (SIGTERM): all ffmpeg processes are stopped concurrently
  (SIGINT ×2, then SIGKILL after `FFMPEG_STOP_GRACE`), sessions are left in state
  `recording` with `suspendedAt` and resume on the next start; in-flight uploads
  get the rest of `SHUTDOWN_TIMEOUT`.
* **ffmpeg failures**: exits, stalls (no data for `FFMPEG_STALL_TIMEOUT`) and network
  errors restart ffmpeg with exponential backoff (`FFMPEG_RESTART_BACKOFF_MIN..MAX`)
  for as long as the window is open. The output keeps appending to the same file.
  If ffmpeg rejects one of the optional tuning options (older build, unusual
  source) the session automatically falls back to a minimal command line.
* **Optional rotation**: `MAX_SESSION_DURATION=6h` splits very long windows into
  parts that are uploaded as they complete.
* **Disk protection**: below `MIN_FREE_DISK` no new recordings start
  (`recorder_disk_low=1`, `/readyz` → 503); running ones continue. A write error
  (disk full) pauses the affected recording and retries every minute.

## Output files and object keys

* Local: `DATA_DIR/recordings/<safeId>/<safeId>_<sessionStartUTC>.aac` + `.json`.
* Default object key: `{S3_PREFIX}{safeId}/{YYYY-MM-DD of scheduled start, UTC}/{safeId}_{sessionStart YYYYMMDDTHHMMSSZ}.aac`.
* Explicit `key`: used as given (with `S3_PREFIX`); later parts of the same item get
  `_<sessionStart>` inserted before the extension. Uploads use `If-None-Match: *`
  so an existing object is never overwritten silently (a different object under the
  same key makes the recorder pick `-2`, `-3`, …).
* Object metadata (`x-amz-meta-*`): `recording-id`, `session-id`, `name`
  (RFC 2047 encoded when non-ASCII), `source` (credentials redacted),
  `scheduled-start`, `scheduled-end`, `session-start`, `session-end`, `codec`,
  `ffmpeg-restarts`, `finish-reason`, `recorder-version`. Content type `audio/aac`.
* Format: raw ADTS (`.aac`). Every frame is self-describing, so a file made of
  several ffmpeg runs (restarts, resume) plays in any decoder. Notes for consumers:
  live HLS is delivered a few segments behind real time — the first run starts
  ~3 segments back (natural pre-roll, why `RECORD_STOP_LATE` defaults to 30 s), and
  restarts start at the live edge, so a restart may leave a small gap or, for
  Icecast burst-on-connect, a few seconds of overlap.

## Configuration

Everything is configured through environment variables (see `.env.example`).
Durations accept Go syntax (`90s`, `5m`, `1h30m`) or plain seconds; sizes accept
`16MiB`, `2GiB`, `512k`.

**Schedule**

| Variable | Default | Description |
|---|---|---|
| `SCHEDULE_URL` | *required* | URL of the JSON schedule. |
| `SCHEDULE_POLL_INTERVAL` | `60s` | Fetch interval (min 5 s). Until the first schedule is loaded the recorder retries every 5 s. `ETag`/`If-None-Match` is used when the server supports it. |
| `SCHEDULE_FETCH_TIMEOUT` | `20s` | HTTP timeout for one fetch. |
| `SCHEDULE_AUTH_HEADER` | – | Extra header for the fetch, e.g. `Authorization: Bearer …`. |
| `SCHEDULE_DEFAULT_TZ` | `UTC` | Zone for timestamps without an offset (e.g. `Europe/Prague`). |

**Storage / upload**

| Variable | Default | Description |
|---|---|---|
| `DATA_DIR` | `/data` | Recordings, sidecars, schedule cache, lock file. |
| `S3_ENDPOINT` | *required*¹ | `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` |
| `S3_REGION` | `auto` | R2 uses `auto`. |
| `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` | *required*¹ | R2 API token with *Object Read & Write* on the bucket. |
| `S3_PREFIX` | – | Key prefix (`recordings/`); normalised to end with `/`. |
| `S3_FORCE_PATH_STYLE` | `true` | Path-style URLs (works for every bucket name on R2). |
| `S3_CHECKSUM_ALGORITHM` | `none` | `none`, `crc32` or `crc32c`. Checksums are only sent when required (R2 compatibility); size is always verified with `HeadObject`. |
| `S3_CONDITIONAL_PUT` | `true` | Use `If-None-Match: *` to avoid overwriting objects. Automatically disabled if the endpoint answers `NotImplemented`. |
| `UPLOAD_CONCURRENCY` | `4` | Parallel uploads (each uses up to 3 part uploads). |
| `UPLOAD_PART_SIZE` | `16MiB` | Multipart part size (5 MiB – 5 GiB). |
| `UPLOAD_BACKOFF_MAX` | `5m` | Cap for the retry backoff (blocked uploads retry every 15 min). |
| `UPLOAD_MIN_THROUGHPUT` | `128KiB` | Assumed minimum upload speed (bytes/s); one attempt may take 10 min + size ÷ throughput before it is retried. |
| `UPLOAD_DISABLED` | `false` | Keep recordings locally (state `kept`). ¹ S3 variables are then optional. |
| `RETENTION_UPLOADED` | `24h` | How long uploaded/failed sidecars stay visible in the API before they are removed. |
| `MIN_FREE_DISK` | `2GiB` | Refuse to start new recordings below this free space. |

**Recording**

| Variable | Default | Description |
|---|---|---|
| `RECORD_START_EARLY` | `10s` | Start this much before `start`. |
| `RECORD_STOP_LATE` | `30s` | Stop this much after `end` (covers live-stream latency). |
| `MAX_SESSION_DURATION` | `0` | Rotate very long sessions into parts (`0` = never, min `1m`). |
| `MAX_RECORDINGS` | `0` | Hard cap on simultaneous recordings (`0` = unlimited). |
| `AUDIO_CODEC` | `auto` | `auto` (copy AAC, transcode the rest), `copy`, `aac`. |
| `AUDIO_BITRATE` | `128k` | Transcode bitrate. |
| `FFMPEG_PATH`, `FFPROBE_PATH` | `ffmpeg`, `ffprobe` | Binaries (the image ships a static ffmpeg 8.0). |
| `FFPROBE_TIMEOUT` | `15s` | Probe timeout in `auto` mode. |
| `PROBE_CONCURRENCY` | `32` | Max concurrent probes at mass start (recurring shows reuse the previous session's codec decision). |
| `FFMPEG_USER_AGENT` | `spectado-stream-recorder/1.0` | Sent to stream servers. |
| `FFMPEG_RW_TIMEOUT` | `10s` | Network I/O timeout inside ffmpeg. |
| `FFMPEG_STALL_TIMEOUT` | `60s` | Restart ffmpeg when no data arrived for this long (≥ 4× HLS segment duration). |
| `FFMPEG_RESTART_BACKOFF_MIN` / `_MAX` | `1s` / `30s` | Restart backoff (reset after a run that lasted > 1 min). |
| `FFMPEG_STOP_GRACE` | `5s` | Time between SIGINT and SIGKILL when stopping ffmpeg. |
| `FFMPEG_STDERR_LOG` | `warn` | `warn` (warnings/errors, rate-limited 10/min per stream), `debug`, `off`. The last 20 lines are always kept in the API. |
| `FFMPEG_TLS_VERIFY` | `false` | Verify TLS certificates of `https` sources (ffmpeg's default is off; only the initial request is covered for HLS, ffmpeg does not pass TLS options to segment downloads). |

**HTTP / monitoring / lifecycle**

| Variable | Default | Description |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Listen address. |
| `API_TOKEN` | – | Bearer token for `/api/*` (`Authorization: Bearer …` or `?token=`). |
| `METRICS_REQUIRE_AUTH` | `false` | Also protect `/metrics` with `API_TOKEN`. |
| `SYSMON_INTERVAL` | `10s` | System sampling interval. |
| `SHUTDOWN_TIMEOUT` | `45s` | Total budget for graceful shutdown (set the container stop grace period higher). |
| `LOG_LEVEL`, `LOG_FORMAT` | `info`, `json` | Structured logs to stdout (`text` for humans). |
| `GOMEMLIMIT` | `512MiB` (image) | Go heap limit; leaves the container memory to ffmpeg children. |

## HTTP API

| Endpoint | Purpose |
|---|---|
| `GET /healthz` (`/health`, `/heartbeat`) | **Heartbeat / liveness.** `200 {"status":"ok",…}` while the internal reconcile loop is ticking; `503` if it stalled. Use for Docker/Kubernetes liveness. |
| `GET /readyz` | `200` once a schedule is loaded (URL or cache); `503` while none is loaded, during shutdown, or while disk is low. |
| `GET /metrics` | Prometheus / OpenMetrics text format. |
| `GET /api/state` | Everything: version, uptime, health checks, schedule fetch state (last attempt/success/error, item counts, invalid items, clock skew), all sessions with live byte counts, ffmpeg PID/running flag, last stderr lines, upload state and errors, upload queue summary, system snapshot. |
| `GET /api/schedule` | Fetch state + the normalised last known items (header values redacted). |
| `GET /api/recordings` | Sessions only (active first). |
| `GET /api/system` | Host view via `/proc` (CPU %, load, memory, process count), container cgroup view (memory usage/limit, CPU cores used/quota, throttling, pids, OOM kills), disk of `DATA_DIR`, and the recorder process itself (RSS, CPU, goroutines, FDs). |

Secrets never leave the process: URLs are shown with credentials and query values
redacted, header values are replaced by `***`.

Example (abridged):

```json
{
  "ready": true, "healthy": true,
  "schedule": { "loaded": true, "origin": "url", "itemCount": 87, "consecutiveFailures": 0, "lastSuccessAt": "…" },
  "recordings": {
    "active": 42, "ffmpeg": 42, "diskLow": false,
    "uploads": { "pending": 1, "inProgress": 1, "blocked": 0, "uploaded": 130, "failed": 0 },
    "items": [
      { "id": "radio1-morning", "state": "recording", "resolvedCodec": "copy", "bytes": 48213904,
        "ffmpegRunning": true, "pid": 4711, "restarts": 1, "lastDataAt": "…", "key": "radio1-morning/2026-08-30/…aac" }
    ]
  },
  "system": { "container": { "memoryUsage": 1932735283, "memoryLimit": 4294967296, "cpuUsageCores": 1.4 }, "…": "…" }
}
```

## Prometheus metrics

All metrics carry the `recorder_` prefix. Highlights (see `/metrics` for the full,
documented list):

| Metric | Type | Meaning |
|---|---|---|
| `schedule_fetch_total{result}` | counter | `success` / `failure` / `not_modified` |
| `schedule_last_success_timestamp_seconds`, `schedule_consecutive_failures`, `schedule_items`, `schedule_invalid_items`, `schedule_clock_skew_seconds` | gauge | Schedule health |
| `recordings_active`, `ffmpeg_processes` | gauge | Live recordings / ffmpeg children |
| `recordings_started_total`, `recordings_finished_total{reason}`, `recordings_suspended_total`, `recordings_skipped_total{reason}` | counter | Lifecycle (`reason`: ended, removed, rotated, superseded, error / max_recordings, disk_low) |
| `recording_bytes_total{id}`, `recording_ffmpeg_running{id}`, `recording_last_data_timestamp_seconds{id}` | per stream | Throughput and liveness per stream (alert on `time() - last_data > 90`) |
| `ffmpeg_restarts_total{id}`, `ffmpeg_stalls_total`, `ffmpeg_killed_total`, `ffmpeg_spawn_failures_total`, `codec_fallbacks_total`, `write_errors_total` | counter | Stream trouble |
| `ffmpeg_cpu_seconds_total{id}`, `ffmpeg_memory_rss_bytes{id}` | per stream | Resource use of the ffmpeg children |
| `uploads_total{result}`, `upload_bytes_total`, `upload_duration_seconds` (histogram 1 s – 1 h), `uploads_pending`, `upload_pending_bytes`, `uploads_in_progress`, `uploads_blocked`, `upload_oldest_pending_age_seconds`, `recordings_failed` | | Upload pipeline |
| `recordings_on_disk_bytes`, `disk_free_bytes{path}`, `disk_used_percent{path}`, `disk_low` | gauge | Local storage |
| `host_cpu_percent`, `host_load1/5/15`, `host_memory_*`, `system_processes` | gauge | Host as seen through `/proc` |
| `cgroup_memory_usage_bytes` (incl. page cache), `cgroup_memory_working_set_bytes` (what OOM acts on), `cgroup_memory_limit_bytes`, `cgroup_cpu_usage_seconds_total`, `cgroup_cpu_usage_cores`, `cgroup_cpu_quota_cores`, `cgroup_cpu_throttled_seconds_total`, `cgroup_pids_current`, `cgroup_oom_kills_total` | | Container (what the limits actually apply to) |
| `self_rss_bytes`, `self_cpu_percent`, `self_open_fds`, `self_threads` + Go/process collectors | | The recorder itself |
| `build_info{version,goversion}`, `ffmpeg_info{version,path}` | gauge | Versions |

Per-`id` series are removed once the last session of an id has been uploaded and
its retention expired, so cardinality stays bounded to recent streams.
Ready-made alert rules and a Grafana dashboard are in `example/alerts.yml` and
`example/grafana/`.

## Deployment notes

**Cloudflare R2.** Create a bucket, then an R2 API token with *Object Read & Write*
limited to that bucket. `S3_ENDPOINT=https://<ACCOUNT_ID>.r2.cloudflarestorage.com`,
`S3_REGION=auto`, access key / secret from the token. Add a lifecycle rule
*"Abort incomplete multipart uploads after 1 day"* to the bucket (the recorder also
aborts its own interrupted uploads and cleans stale ones older than 1 h at startup).

**Sizing for ~100 streams.**
* CPU: stream copy ≈ 0; transcoding ≈ 1–3 % of a core per stream → 1–3 cores.
* Memory: ~20–50 MB RSS per ffmpeg → plan 2 GiB (copy) to 4 GiB (transcode) for the
  container; Go itself stays well under `GOMEMLIMIT`.
* Threads: ffmpeg keeps several threads even with `-threads 1`; set `pids_limit`
  (or the Kubernetes pod pids limit) to ≥ 8192.
* Disk: 128 kbit/s ≈ 58 MB/h per stream → 100 streams ≈ 5.8 GB/h. Files stay on disk
  until the upload is confirmed, so size the volume for the longest R2 outage you
  want to survive (plus the longest show). Watch `recorder_upload_pending_bytes`.
* Network: 100 × 128 kbit/s ≈ 13 Mbit/s inbound.

**Volume permissions.** The image runs as uid/gid `10001`. A named volume inherits
the right ownership; for a bind mount run `mkdir -p data && sudo chown 10001:10001 data`
(or set `user:` in compose). The recorder exits with an explicit message when
`DATA_DIR` is not writable.

**One instance per data directory.** A lock file (`DATA_DIR/recorder.lock`) prevents
two recorders from appending to the same files.

**Shutdown.** Set the container stop grace period above `SHUTDOWN_TIMEOUT`
(`stop_grace_period: 60s` in compose, `terminationGracePeriodSeconds: 60` in k8s).
Recordings are not uploaded on shutdown — they resume when the container is back
and upload when their window closes.

**Health checks.** The image ships `HEALTHCHECK … recorder healthcheck` (probes
`/healthz` on `HTTP_ADDR`). In Kubernetes use `/healthz` as liveness and `/readyz`
as readiness probe.

**Logs.** JSON to stdout. ffmpeg's own warnings are forwarded rate-limited
(10 lines/min/stream); configure log rotation on the Docker daemon
(`max-size`, see the compose example).

**Security.** Put `API_TOKEN` on `/api/*` when the port is reachable by others
(`/api/state` contains stream URLs — redacted — and file names). `/metrics` can be
protected too (`METRICS_REQUIRE_AUTH=true`). Sources are restricted to
`http`/`https` (ffmpeg protocol whitelist), so a schedule entry can never make the
recorder read local files.

## CI / image tags

`.github/workflows/docker.yml` builds a multi-arch image (linux/amd64, linux/arm64)
on every push and publishes it to GHCR as `ghcr.io/<owner>/<repo>`:

| Branch | Tags |
|---|---|
| `main` | `:main`, `:<version>`, `:<version>-main`, `:sha-<short>` |
| any other branch | `:develop`, `:<version>-develop`, `:sha-<short>` |

`<version>` is read from the `VERSION` file at the repository root (bump it for a
release; it is also baked into the binary: `recorder --version`, `/healthz`,
`recorder_build_info`). `dependabot/**` and `renovate/**` branches are excluded so a
dependency bump cannot overwrite `:develop`. `ci.yml` runs `gofmt`/`go vet`/`go test
-race` and a no-push Docker build on pull requests.

The first push creates a *private* GHCR package — make it public or grant pull
access in the package settings.

## Development

```bash
go build ./... && go test ./... -race          # unit tests (fake ffmpeg, no network)
make e2e                                       # scripts/e2e-local.sh: real ffmpeg, local HLS tone,
                                               # SIGTERM suspend/resume, ffprobe validation (~90 s)
go run ./cmd/recorder                          # needs SCHEDULE_URL etc. in the environment

# run against a local test stream without Docker (or simply `make e2e`)
mkdir -p /tmp/www/hls
ffmpeg -re -f lavfi -i "sine=frequency=440" -c:a aac -f hls -hls_time 2 -hls_list_size 6 \
       -hls_flags delete_segments /tmp/www/hls/test.m3u8 &
# the example schedule server serves /tmp/www and a schedule whose window is "now"
PORT=8081 WWW_ROOT=/tmp/www STREAM_BASE=http://127.0.0.1:8081 RECORD_MINUTES=10 \
  python3 example/schedule-server/server.py &
SCHEDULE_URL=http://127.0.0.1:8081/schedule.json UPLOAD_DISABLED=true DATA_DIR=/tmp/rec \
  LOG_FORMAT=text go run ./cmd/recorder
```

Layout: `cmd/recorder` (main, subcommands) · `internal/config` (env) ·
`internal/schedule` (fetch/parse/cache) · `internal/recorder` (state machine, ffmpeg
supervisor, sidecars, upload queue, API views) · `internal/storage` (S3/R2) ·
`internal/adts` (tail trimming) · `internal/sysmon` (system/cgroup sampling) ·
`internal/metrics` · `internal/httpapi` · `example/` (compose assets, schedule
server, Prometheus rules, Grafana dashboard).

## Troubleshooting

| Symptom | Where to look |
|---|---|
| Recording never starts | `/api/schedule` → is the item valid (`info.invalidItems`)? Is `now` inside `start−startEarly … end+stopLate`? `recorder_recordings_skipped_total{reason}` (disk low, max recordings). |
| `ffmpegRunning: false`, `restarts` climbing | `lastError` / `lastStderr` in `/api/recordings` (404s, DNS, `matches no streams` = not an audio stream, `not in allowed_segment_extensions` = unusual HLS segment names — handled automatically on ffmpeg ≥ 7.1). |
| File grows but upload never happens | Uploads happen only after the window closes. Then check `uploads.blocked`, `lastError` (`AccessDenied`, `NoSuchBucket` …), `recorder_upload_oldest_pending_age_seconds`. |
| `/readyz` 503 | No schedule loaded yet (URL unreachable and no cache), disk below `MIN_FREE_DISK`, or shutting down. |
| Recording stopped unexpectedly | `finishReason`: `ended` (end passed — check `end`/`stopLate`), `removed` (item absent from a successful fetch), `rotated` (`MAX_SESSION_DURATION`). |
| `another recorder instance is already using DATA_DIR` | Two containers share the volume — run one per data directory. |
