#!/usr/bin/env bash
# Local end-to-end smoke test with the real ffmpeg (no Docker, no R2):
#   1. generates a live HLS test tone and serves it with python's http.server
#   2. serves a schedule whose window is [now-5s, now+50s]
#   3. runs the recorder with uploads disabled (kept mode: remuxed to .m4a)
#   4. sends SIGTERM mid-recording and restarts it (suspend / resume)
#   5. waits for the end, waits for the kept remux, validates the .m4a with
#      ffprobe and a full decode, and checks the source .aac was deleted
#
# E2E_VIDEO=1 (or `make e2e-video`) runs the same with RECORD_VIDEO=true and a
# second, audio+video HLS stream (testsrc2 320x240 + tone) recorded as item
# test-video next to the audio-only test-hls. Every .m4a must still be
# audio-only and pass the checks above; every recording must also have a kept
# .mp4 (h264 320x240 for test-video, the 16x16 black picture for test-hls) with
# aac audio, a duration close to its .m4a's and no decode errors, and no video
# capture (.ts) files may be left behind.
#
# Requirements: go, ffmpeg/ffprobe (with libx264 for E2E_VIDEO=1), python3, curl.
# Optional: EXTERNAL_STREAM=<url> adds a second (public) stream, e.g.
# https://ice1.somafm.com/groovesalad-128-aac
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
WWW=$WORK/www; DATA=$WORK/data; mkdir -p "$WWW/hls" "$DATA"
PORT=${E2E_HTTP_PORT:-28081}; RPORT=${E2E_RECORDER_PORT:-28080}
VIDEO=${E2E_VIDEO:-0}; [ "$VIDEO" = "1" ] || VIDEO=0
FF=""; FFV=""; SRV=""; REC=""
cleanup() { kill $FF $FFV $SRV $REC 2>/dev/null; sleep 1; kill -9 $FF $FFV $SRV $REC 2>/dev/null; echo "work dir: $WORK"; }
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
if [ "$VIDEO" = 1 ]; then
  say "start local A/V HLS test stream (E2E_VIDEO=1)"
  ffmpeg -hide_banner -loglevel warning -re -f lavfi -i "testsrc2=size=320x240:rate=25" \
    -f lavfi -i "sine=frequency=880:sample_rate=44100" -map 0:v:0 -map 1:a:0 \
    -c:v libx264 -preset ultrafast -g 50 -sc_threshold 0 -pix_fmt yuv420p -ac 2 -c:a aac -b:a 96k \
    -f hls -hls_time 2 -hls_list_size 6 -hls_flags delete_segments+append_list+program_date_time \
    -hls_segment_filename "$WWW/hls/video_%05d.ts" "$WWW/hls/video.m3u8" >"$WORK/videostream.log" 2>&1 &
  FFV=$!
fi
sleep 7

START=$(iso -5); END=$(iso 50); END_EPOCH=$(( $(date +%s) + 50 ))
EXTRA=""
[ -n "${EXTERNAL_STREAM:-}" ] && EXTRA=",{\"id\":\"external\",\"source\":\"$EXTERNAL_STREAM\",\"start\":\"$START\",\"end\":\"$END\"}"
[ "$VIDEO" = 1 ] && EXTRA="$EXTRA,{\"id\":\"test-video\",\"type\":\"hls\",\"source\":\"http://127.0.0.1:$PORT/hls/video.m3u8\",\"start\":\"$START\",\"end\":\"$END\"}"
# Local items only (test-hls, + test-video): the optional external stream may fail.
EXPECTED_RECORDINGS=$(( 1 + VIDEO ))
STREAM_INFO='{"matchId":4711,"commentator":{"id":42,"name":"Jan Novák"},"language":"cs"}'
echo "[{\"id\":\"test-hls\",\"type\":\"hls\",\"source\":\"http://127.0.0.1:$PORT/hls/test.m3u8\",\"start\":\"$START\",\"end\":\"$END\",\"streamInfo\":$STREAM_INFO}$EXTRA]" > "$WWW/schedule.json"
(cd "$WWW" && exec python3 -m http.server "$PORT" --bind 127.0.0.1 >"$WORK/http.log" 2>&1) &
SRV=$!
sleep 1

run_recorder() {
  SCHEDULE_URL=http://127.0.0.1:$PORT/schedule.json SCHEDULE_POLL_INTERVAL=5s DATA_DIR="$DATA" HTTP_ADDR=127.0.0.1:$RPORT \
  UPLOAD_DISABLED=true RECORD_START_EARLY=0 RECORD_STOP_LATE=0 LOG_FORMAT=text LOG_LEVEL=info MIN_FREE_DISK=1MiB \
  RECORD_VIDEO=$([ "$VIDEO" = 1 ] && echo true || echo false) \
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
# streamInfo from the schedule is carried 1:1 in the session sidecar (and from there into the .json document).
python3 -c "
import json,glob,sys
want = json.loads(sys.argv[1])
got = [json.load(open(f)).get('streamInfo') for f in glob.glob('$DATA/recordings/test-hls*/*.json')]
if not got or any(g != want for g in got): sys.exit(f'sidecar streamInfo {got}, want {want}')
" "$STREAM_INFO" || fail "streamInfo not carried into the sidecar"

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
if [ "$VIDEO" = 1 ]; then
  # The capture decision: the A/V source is stream-copied, the tone has no video.
  api /api/recordings | python3 -c "
import json,sys
want = {'test-hls': 'none', 'test-video': 'copy'}
got = {}
for r in json.load(sys.stdin):
    if r['id'] in want:
        if not r.get('video'): sys.exit(f\"{r['id']}: session not in video mode\")
        got.setdefault(r['id'], set()).add(r.get('videoSource'))
for i, src in want.items():
    if got.get(i) != {src}: sys.exit(f'{i}: videoSource {got.get(i)}, want {src}')
" || fail "unexpected video capture decision"
fi

say "wait for the kept remux (.m4a$([ "$VIDEO" = 1 ] && echo " + .mp4"))"
# Kept mode (UPLOAD_DISABLED=true) remuxes each finished session to
# <session>.m4a next to its sidecar and deletes the source .aac only once the
# remux succeeded and the sidecar says so; poll up to 90s for that to land.
# Each recording's sessions (here: one, the resume appends) give one file each.
count() { find "$DATA/recordings" -name "*.$1" -type f 2>/dev/null | wc -l | tr -d ' '; }
DEADLINE=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  if [ "$(count m4a)" -ge "$EXPECTED_RECORDINGS" ] && { [ "$VIDEO" = 0 ] || [ "$(count mp4)" -ge "$EXPECTED_RECORDINGS" ]; }; then
    break
  fi
  sleep 2
done
[ "$(count m4a)" -ge "$EXPECTED_RECORDINGS" ] || fail "only $(count m4a) of $EXPECTED_RECORDINGS .m4a files appeared within 90s"
if [ "$VIDEO" = 1 ]; then
  [ "$(count mp4)" -ge "$EXPECTED_RECORDINGS" ] || fail "only $(count mp4) of $EXPECTED_RECORDINGS .mp4 files appeared within 90s"
fi

say "validate with ffprobe"
EXPECTED_DURATION=$(( END_EPOCH - REC1_START ))
for f in "$DATA"/recordings/*/*.m4a; do
  echo "--- $f"
  FORMAT=$(ffprobe -v error -show_entries format=format_name -of default=nw=1:nk=1 "$f") || fail "ffprobe format"
  echo "format_name: $FORMAT"
  case "$FORMAT" in *mp4*|*m4a*|*ipod*) ;; *) fail "unexpected format_name $FORMAT in $f" ;; esac
  CODEC=$(ffprobe -v error -select_streams a:0 -show_entries stream=codec_name -of default=nw=1:nk=1 "$f") || fail "ffprobe codec"
  [ "$CODEC" = "aac" ] || fail "expected codec aac, got $CODEC in $f"
  # The .m4a is audio-only in every mode (RECORD_VIDEO puts video in the .mp4).
  [ -z "$(ffprobe -v error -select_streams v -show_entries stream=index -of csv=p=0 "$f")" ] || fail "video stream in $f"
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

if [ "$VIDEO" = 1 ]; then
  say "validate the .mp4 video companions"
  for m in "$DATA"/recordings/*/*.m4a; do
    f="${m%.m4a}.mp4"
    echo "--- $f"
    [ -f "$f" ] || fail "no .mp4 companion for $m"
    case "$(basename "$(dirname "$f")")" in
      test-video) WANT=h264,320,240 ;;   # stream-copied from the A/V source
      test-hls) WANT=h264,16,16 ;;       # black picture for the audio-only source
      *) WANT=h264 ;;                    # EXTERNAL_STREAM: whatever it carries
    esac
    V=$(ffprobe -v error -select_streams v:0 -show_entries stream=codec_name,width,height -of csv=p=0 "$f") || fail "ffprobe video"
    echo "video: $V (want $WANT)"
    case "$V" in "$WANT"|"$WANT",*) ;; *) fail "video stream $V, want $WANT in $f" ;; esac
    CODEC=$(ffprobe -v error -select_streams a:0 -show_entries stream=codec_name -of default=nw=1:nk=1 "$f") || fail "ffprobe codec"
    [ "$CODEC" = "aac" ] || fail "expected audio aac, got $CODEC in $f"
    VD=$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$f") || fail "ffprobe duration"
    AD=$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$m") || fail "ffprobe duration"
    echo "duration: $VD (.m4a: $AD, +/- 3s)"
    python3 -c "import sys; sys.exit(0 if abs(float('$VD') - float('$AD')) <= 3 else 1)" \
      || fail "duration $VD of $f is not within 3s of its .m4a ($AD)"
    ERRS=$(ffmpeg -v error -i "$f" -f null - 2>&1 | wc -l | tr -d ' ')
    echo "decode errors: $ERRS"; [ "$ERRS" -eq 0 ] || fail "decode errors in $f"
  done
  LEFT=$(count ts)
  [ "$LEFT" -eq 0 ] || fail "$LEFT video capture (.ts) files left after the kept .mp4 build"
fi
kill -TERM $REC; wait $REC
echo; echo "E2E OK"
