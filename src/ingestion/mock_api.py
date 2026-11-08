"""Tiny local REST API that serves deterministic daily sales (demo/testing only).

    GET /health                 -> {"status": "ok"}
    GET /sales?date=YYYY-MM-DD  -> {"date": ..., "records": [...]}

Run: ``python -m src.ingestion.mock_api --port 8000``
"""
from __future__ import annotations

import argparse
import json
import tempfile
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from src.config import get_settings
from src.ingestion.generator import generate_daily_batch


def records_for(day: date) -> list[dict]:
    settings = get_settings()
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sales.jsonl"
        generate_daily_batch(path, day, records=settings.daily_records, seed=settings.generator_seed,
                             dirty_rate=settings.dirty_rate, fmt="jsonl")
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        url = urlparse(self.path)
        if url.path == "/health":
            return self._send(200, {"status": "ok"})
        if url.path != "/sales":
            return self._send(404, {"error": "not found"})
        try:
            day = date.fromisoformat(parse_qs(url.query).get("date", [""])[0])
        except ValueError:
            return self._send(400, {"error": "query parameter date=YYYY-MM-DD is required"})
        return self._send(200, {"date": day.isoformat(), "records": records_for(day)})

    def log_message(self, fmt, *args) -> None:  # quieter logs
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"mock sales API listening on {args.host}:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
