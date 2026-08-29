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

# Smoke test: the shipped ffmpeg must encode AAC into ADTS and speak https.
RUN ffmpeg -hide_banner -loglevel error -f lavfi -i "sine=frequency=440:duration=0.2" -c:a aac -f adts - > /dev/null \
    && ffmpeg -hide_banner -protocols 2>/dev/null | grep -q https \
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
