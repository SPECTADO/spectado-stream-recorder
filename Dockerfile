# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Build stage: cross-compiles the static Go binary on the build platform
# (no QEMU for the Go toolchain; TARGETOS/TARGETARCH select the output).
# ---------------------------------------------------------------------------
ARG GO_VERSION=1.26
# Pinned, fully static, multi-arch ffmpeg build (amd64/arm64); see the ffmpeg stage below.
ARG FFMPEG_IMAGE=mwader/static-ffmpeg:8.0.1

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
# VERSION is passed by CI (read from the VERSION file); falls back to the file itself.
ARG VERSION
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    V="${VERSION:-}"; \
    if [ -z "$V" ]; then V="$(tr -d '[:space:]' < VERSION 2>/dev/null || echo dev)"; fi; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
    go build -trimpath -ldflags="-s -w -X main.version=${V}" -o /out/recorder ./cmd/recorder

# ---------------------------------------------------------------------------
# ffmpeg: a pinned, fully static, multi-arch build (amd64/arm64) so production
# runs the same ffmpeg major version the recorder was tested with, instead of
# whatever Alpine ships (6.1 in Alpine 3.22).
# ---------------------------------------------------------------------------
FROM ${FFMPEG_IMAGE} AS ffmpeg

# ---------------------------------------------------------------------------
# Runtime stage.
# ---------------------------------------------------------------------------
FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata tini \
    && addgroup -g 10001 recorder \
    && adduser -D -H -u 10001 -G recorder -s /sbin/nologin recorder \
    && install -d -o 10001 -g 10001 -m 755 /data

COPY --from=ffmpeg /ffmpeg /ffprobe /usr/local/bin/
COPY --from=build /out/recorder /usr/local/bin/recorder

# Smoke test: the shipped ffmpeg must encode AAC into ADTS, speak https, and
# remux ADTS into the .m4a (ipod muxer) the upload path relies on. The video
# companion (RECORD_VIDEO=true) additionally needs libx264 (the same check the
# recorder runs at startup), the mpegts muxer of the capture and the mp4 muxer
# of the upload: a 1 s black 16x16 picture + sine tone goes TS -> .mp4.
RUN ffmpeg -hide_banner -loglevel error -f lavfi -i "sine=frequency=440:duration=0.2" -c:a aac -f adts - > /dev/null \
    && ffmpeg -hide_banner -protocols 2>/dev/null | grep -q https \
    && ffmpeg -hide_banner -loglevel error -f lavfi -i "sine=frequency=440:duration=0.2" -c:a aac -f adts /tmp/smoke.aac \
    && ffmpeg -hide_banner -loglevel error -xerror -i /tmp/smoke.aac -c:a copy -movflags +faststart -f ipod /tmp/smoke.m4a \
    && ffprobe -v error -show_entries format=format_name -of default=nw=1 /tmp/smoke.m4a | grep -q mp4 \
    && ffmpeg -hide_banner -h encoder=libx264 2>/dev/null | grep -q "Encoder libx264" \
    && ffmpeg -hide_banner -loglevel error -f lavfi -i "color=c=black:s=16x16:r=1:d=1" -f lavfi -i "sine=frequency=440:duration=1" \
         -map 0:v:0 -map 1:a:0 -c:v libx264 -preset ultrafast -tune stillimage -pix_fmt yuv420p -g 10 -c:a aac -f mpegts /tmp/smoke.ts \
    && ffmpeg -hide_banner -loglevel error -xerror -i /tmp/smoke.ts -map 0:v:0 -map 0:a:0 -c copy -movflags +faststart -f mp4 /tmp/smoke.mp4 \
    && ffprobe -v error -select_streams v:0 -show_entries stream=codec_name,width,height -of csv=p=0 /tmp/smoke.mp4 | grep -qx "h264,16,16" \
    && rm -f /tmp/smoke.aac /tmp/smoke.m4a /tmp/smoke.ts /tmp/smoke.mp4 \
    && /usr/local/bin/recorder --version

ENV DATA_DIR=/data \
    HTTP_ADDR=:8080 \
    GOMEMLIMIT=512MiB

USER 10001:10001
VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/recorder", "healthcheck"]

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/recorder"]
