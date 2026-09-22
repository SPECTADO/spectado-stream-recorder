#!/usr/bin/env bash
# Local end-to-end smoke test with the real ffmpeg (no Docker, no R2):
#   1. generates a live HLS test tone and serves it with python's http.server
#   2. serves a schedule whose window is [now-5s, now+50s]
#   3. runs the recorder with uploads disabled (kept mode: remuxed to .m4a)
#   4. sends SIGTERM mid-recording and restarts it (suspend / resume)
#   5. waits for the end, waits for the kept remux, validates the .m4a with
#      ffprobe and a full decode, and checks the source .aac was deleted
#
# Requirements: go, ffmpeg/ffprobe, python3, curl. Optional: EXTERNAL_STREAM=<url>
# adds a second (public) stream, e.g. https://ice1.somafm.com/groovesalad-128-aac
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
WWW=$WORK/www; DATA=$WORK/data; mkdir -p "$WWW/hls" "$DATA"
PORT=${E2E_HTTP_PORT:-28081}; RPORT=${E2E_RECORDER_PORT:-28080}
FF=""; SRV=""; REC=""
cleanup() { kill $FF $SRV $REC 2>/dev/null; sleep 1; kill -9 $FF $SRV $REC 2>/dev/null; echo "work dir: $WORK"; }
trap cleanup EXIT
say() { echo; echo "===== $*"; }
iso() { # $1 = seconds offset from now, RFC3339 UTC (GNU and BSD date)
  local off=$1
  if date -u -d "@0" >/dev/null 2>&1; then
    date -u -d "@$(( $(date +%s) + off ))" +%Y-%m-%dT%H:%M:%SZ
  elif [ "$off" -ge 0 ]; then
    date -u -v+"${off}"S +%Y-%m-%dT%H:%M:%SZ
  else
    date -u -v"${off}"S +%Y-%m-%dT%H:%M:%SZ
  fi
}
fail() { echo "FAIL: $*"; exit 1; }
port_free() { ! (echo > "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
port_free "$PORT" || fail "port $PORT is already in use (set E2E_HTTP_PORT)"
port_free "$RPORT" || fail "port $RPORT is already in use (set E2E_RECORDER_PORT)"

say "build"
(cd "$ROOT" && go build -o "$WORK/recorder" ./cmd/recorder) || fail "build"

say "start local HLS test stream"
ffmpeg -hide_banner -loglevel warning -re -f lavfi -i "sine=frequency=440:sample_rate=44100" -ac 2 -c:a aac -b:a 96k \
  -f hls -hls_time 2 -hls_list_size 6 -hls_flags delete_segments+append_list+program_date_time \
  -hls_segment_filename "$WWW/hls/test_%05d.ts" "$WWW/hls/test.m3u8" >"$WORK/teststream.log" 2>&1 &
FF=$!
sleep 7

START=$(iso -5); END=$(iso 50); END_EPOCH=$(( $(date +%s) + 50 ))
EXTRA=""
[ -n "${EXTERNAL_STREAM:-}" ] && EXTRA=",{\"id\":\"external\",\"source\":\"$EXTERNAL_STREAM\",\"start\":\"$START\",\"end\":\"$END\"}"
echo "[{\"id\":\"test-hls\",\"type\":\"hls\",\"source\":\"http://127.0.0.1:$PORT/hls/test.m3u8\",\"start\":\"$START\",\"end\":\"$END\"}$EXTRA]" > "$WWW/schedule.json"
(cd "$WWW" && exec python3 -m http.server "$PORT" --bind 127.0.0.1 >"$WORK/http.log" 2>&1) &
SRV=$!
sleep 1

run_recorder() {
  SCHEDULE_URL=http://127.0.0.1:$PORT/schedule.json SCHEDULE_POLL_INTERVAL=5s DATA_DIR="$DATA" HTTP_ADDR=127.0.0.1:$RPORT \
  UPLOAD_DISABLED=true RECORD_START_EARLY=0 RECORD_STOP_LATE=0 LOG_FORMAT=text LOG_LEVEL=info MIN_FREE_DISK=1MiB \
  "$WORK/recorder" >>"$WORK/recorder.log" 2>&1 &
  REC=$!
}
api() { curl -sf "http://127.0.0.1:$RPORT$1"; }
bytes_of() { api /api/recordings | python3 -c "import json,sys; print(sum(r['bytes'] for r in json.load(sys.stdin) if r['id']=='$1'))"; }

say "run 1"
REC1_START=$(date +%s)
run_recorder; sleep 15
api /healthz | grep -q '"status": "ok"' || fail "healthz"
api /readyz | grep -q ready || fail "readyz"
B1=$(bytes_of test-hls); echo "bytes after 15s: $B1"; [ "$B1" -gt 0 ] || fail "no data captured"
api /metrics | grep -E '^recorder_(recordings_active|ffmpeg_processes) '

say "SIGTERM -> suspend"
kill -TERM $REC; wait $REC
grep -q "recording suspended for shutdown" "$WORK/recorder.log" || fail "not suspended"
python3 -c "import json,glob; [print(' sidecar', json.load(open(f))['state']) for f in glob.glob('$DATA/recordings/*/*.json')]"

say "run 2 -> resume"
run_recorder; sleep 12
grep -q "resuming recording" "$WORK/recorder.log" || fail "not resumed"
B2=$(bytes_of test-hls); echo "bytes after resume: $B2"; [ "$B2" -gt "$B1" ] || fail "resume did not append"

say "wait for the end"
NOW=$(date +%s); WAIT=$((END_EPOCH - NOW + 6)); [ $WAIT -gt 0 ] && sleep $WAIT
api /api/recordings | python3 -c "
import json,sys
for r in json.load(sys.stdin): print(f\"  {r['id']}: state={r['state']} reason={r.get('finishReason')} bytes={r['bytes']} restarts={r['restarts']}\")"
grep -q "recording finished" "$WORK/recorder.log" || fail "not finished"

say "wait for the kept remux (.m4a)"
# Kept mode (UPLOAD_DISABLED=true) remuxes each finished session to
# <session>.m4a next to its sidecar and deletes the source .aac only once the
# remux succeeded and the sidecar says so; poll up to 90s for that to land.
DEADLINE=$(( $(date +%s) + 90 ))
M4A=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  M4A=$(ls "$DATA"/recordings/*/*.m4a 2>/dev/null | head -1 || true)
  [ -n "$M4A" ] && break
  sleep 2
done
[ -n "$M4A" ] || fail "no .m4a appeared for test-hls within 90s"

say "validate with ffprobe"
EXPECTED_DURATION=$(( END_EPOCH - REC1_START ))
for f in "$DATA"/recordings/*/*.m4a; do
  echo "--- $f"
  FORMAT=$(ffprobe -v error -show_entries format=format_name -of default=nw=1:nk=1 "$f") || fail "ffprobe format"
  echo "format_name: $FORMAT"
  case "$FORMAT" in *mp4*|*m4a*|*ipod*) ;; *) fail "unexpected format_name $FORMAT in $f" ;; esac
  CODEC=$(ffprobe -v error -select_streams a:0 -show_entries stream=codec_name -of default=nw=1:nk=1 "$f") || fail "ffprobe codec"
  [ "$CODEC" = "aac" ] || fail "expected codec aac, got $CODEC in $f"
  DURATION=$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$f") || fail "ffprobe duration"
  echo "duration: $DURATION (expected ~$EXPECTED_DURATION = END_EPOCH($END_EPOCH) - recorder first start($REC1_START), +/- 8s)"
  python3 -c "
import sys
d, exp = float('$DURATION'), float('$EXPECTED_DURATION')
sys.exit(0 if abs(d - exp) <= 8 else 1)
" || fail "duration $DURATION outside +/-8s of expected $EXPECTED_DURATION in $f"
  ERRS=$(ffmpeg -v error -i "$f" -f null - 2>&1 | wc -l | tr -d ' ')
  echo "decode errors: $ERRS"; [ "$ERRS" -eq 0 ] || fail "decode errors in $f"
  # Wall clock now lives in MP4 metadata, not in-band ID3 tags.
  ffprobe -v error -show_format "$f" | grep -q "TAG:creation_time=" || fail "no creation_time tag in $f"
  ffprobe -v error -show_format "$f" | grep -q "TAG:title=" || fail "no title tag in $f"
  AAC="${f%.m4a}.aac"
  [ -e "$AAC" ] && fail "source .aac still present after successful remux: $AAC"
done
kill -TERM $REC; wait $REC
echo; echo "E2E OK"
