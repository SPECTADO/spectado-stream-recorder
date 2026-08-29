#!/usr/bin/env python3
"""Tiny schedule + static file server for the spectado-stream-recorder example stack.

Standard library only (Python 3.12).

  GET /schedule.json   dynamic schedule; times are relative to server start
  GET /<anything>      static files from /www (the HLS test stream lives in /www/hls)

Environment:
  PORT                 listen port                         (default 8000)
  RECORD_MINUTES       length of the built-in test item     (default 10)
  STREAM_BASE          base URL of the test stream          (default http://schedule:8000)
  EXTRA_SCHEDULE_FILE  optional JSON array of extra items   (default /app/extra-items.json)
  WWW_ROOT             static root                          (default /www)
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import sys
from urllib.parse import unquote
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PORT = int(os.environ.get("PORT", "8000"))
RECORD_MINUTES = int(os.environ.get("RECORD_MINUTES", "10"))
STREAM_BASE = os.environ.get("STREAM_BASE", "http://schedule:8000").rstrip("/")
EXTRA_SCHEDULE_FILE = os.environ.get("EXTRA_SCHEDULE_FILE", "/app/extra-items.json")
WWW_ROOT = Path(os.environ.get("WWW_ROOT", "/www")).resolve()

SERVER_START = datetime.now(timezone.utc).replace(microsecond=0)

CONTENT_TYPES = {
    ".m3u8": "application/vnd.apple.mpegurl",
    ".ts": "video/mp2t",
    ".aac": "audio/aac",
    ".json": "application/json",
    ".txt": "text/plain; charset=utf-8",
}


def rfc3339(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    print(f"{rfc3339(datetime.now(timezone.utc))} {msg}", flush=True)


def load_extra_items() -> list[dict]:
    """Load extra schedule items, converting relative offsets to absolute times.

    Items whose id starts with "_" or that have "disabled": true are skipped.
    Optional integer fields startOffsetSeconds / endOffsetSeconds (relative to
    server start) are converted to RFC3339 start / end; absolute start / end
    values pass through unchanged.
    """
    path = Path(EXTRA_SCHEDULE_FILE)
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, ValueError) as exc:
        log(f"WARN cannot load {path}: {exc}")
        return []
    if isinstance(raw, dict):
        raw = raw.get("items", [])
    if not isinstance(raw, list):
        log(f"WARN {path}: expected a JSON array or {{\"items\": [...]}}")
        return []

    items: list[dict] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        item_id = str(entry.get("id", ""))
        if not item_id or item_id.startswith("_") or entry.get("disabled") is True:
            continue
        item = {k: v for k, v in entry.items() if k not in ("startOffsetSeconds", "endOffsetSeconds", "disabled")}
        if "startOffsetSeconds" in entry:
            item["start"] = rfc3339(SERVER_START + timedelta(seconds=int(entry["startOffsetSeconds"])))
        if "endOffsetSeconds" in entry:
            item["end"] = rfc3339(SERVER_START + timedelta(seconds=int(entry["endOffsetSeconds"])))
        if "start" not in item or "end" not in item:
            log(f"WARN skipping item {item_id!r}: missing start/end")
            continue
        items.append(item)
    return items


def build_schedule() -> bytes:
    items = [
        {
            "id": "test-hls",
            "name": "Local HLS test tone",
            "type": "hls",
            "source": f"{STREAM_BASE}/hls/test.m3u8",
            "start": rfc3339(SERVER_START - timedelta(seconds=60)),
            "end": rfc3339(SERVER_START + timedelta(minutes=RECORD_MINUTES)),
        }
    ]
    items.extend(load_extra_items())
    return json.dumps(items, indent=2).encode("utf-8") + b"\n"


class Handler(BaseHTTPRequestHandler):
    server_version = "schedule-example/1.0"
    protocol_version = "HTTP/1.1"

    # ----- logging -----------------------------------------------------------
    def log_message(self, fmt: str, *args) -> None:  # noqa: N802 (stdlib name)
        # Suppress default stderr logging; we log once per request in _send*.
        pass

    def _log_request(self, status: int, length: int) -> None:
        log(f"{self.client_address[0]} {self.command} {self.path} -> {status} {length}B")

    # ----- helpers -----------------------------------------------------------
    def _send_body(self, status: int, body: bytes, headers: dict[str, str]) -> None:
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
        self._log_request(status, len(body))

    def _send_error(self, status: HTTPStatus, text: str | None = None) -> None:
        body = ((text or status.phrase) + "\n").encode("utf-8")
        self._send_body(int(status), body, {"Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-cache"})

    # ----- handlers ----------------------------------------------------------
    def do_HEAD(self) -> None:  # noqa: N802
        self.do_GET()

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0].split("#", 1)[0]
        if path == "/schedule.json":
            self._serve_schedule()
        else:
            self._serve_static(path)

    def _serve_schedule(self) -> None:
        body = build_schedule()
        etag = '"' + hashlib.sha1(body).hexdigest() + '"'
        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "ETag": etag,
        }
        inm = self.headers.get("If-None-Match")
        if inm and etag in [tag.strip() for tag in inm.split(",")]:
            self.send_response(HTTPStatus.NOT_MODIFIED)
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self._log_request(HTTPStatus.NOT_MODIFIED, 0)
            return
        self._send_body(HTTPStatus.OK, body, headers)

    def _serve_static(self, url_path: str) -> None:
        # Resolve inside WWW_ROOT and refuse anything that escapes it.
        rel = unquote(url_path).lstrip("/")
        if not rel or "\x00" in rel:
            self._send_error(HTTPStatus.NOT_FOUND)
            return
        try:
            target = (WWW_ROOT / rel).resolve()
        except (OSError, RuntimeError):
            self._send_error(HTTPStatus.NOT_FOUND)
            return
        if target != WWW_ROOT and WWW_ROOT not in target.parents:
            self._send_error(HTTPStatus.FORBIDDEN)
            return
        if not target.is_file():
            self._send_error(HTTPStatus.NOT_FOUND)
            return
        try:
            data = target.read_bytes()
        except OSError:
            self._send_error(HTTPStatus.NOT_FOUND)
            return

        ext = target.suffix.lower()
        ctype = CONTENT_TYPES.get(ext) or mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        headers = {"Content-Type": ctype}
        if ext == ".m3u8":
            headers["Cache-Control"] = "no-cache"
        self._send_body(HTTPStatus.OK, data, headers)


def main() -> int:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    log(
        f"listening on 0.0.0.0:{PORT} www={WWW_ROOT} stream_base={STREAM_BASE} "
        f"record_minutes={RECORD_MINUTES} extra={EXTRA_SCHEDULE_FILE} start={rfc3339(SERVER_START)}"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
