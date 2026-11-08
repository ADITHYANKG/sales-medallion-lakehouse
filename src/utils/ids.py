"""Deterministic identifiers and checksums used for idempotency."""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

_CHUNK = 1024 * 1024


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(_CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def batch_id_for_checksum(checksum: str) -> str:
    """Content-addressed batch id: re-ingesting identical bytes yields the same batch."""
    return f"b{checksum[:16]}"


def new_run_id(prefix: str = "cli") -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{prefix}__{stamp}"


def safe_token(value: str) -> str:
    """Make an arbitrary run id safe for object-store keys and file names."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")[:120]
