"""Optional REST API source.

Expected contract: ``GET {SALES_API_URL}?date=YYYY-MM-DD`` returns either a JSON
list of sales records or ``{"records": [...]}`` using the source column names.
Responses are stored as JSON Lines sorted by ``order_item_id`` so a retried fetch
of the same data yields byte-identical files (=> same checksum and batch id).
"""
from __future__ import annotations

import json
import time
from datetime import date
from pathlib import Path

from src.utils.logging import get_logger
from src.validation.schema import SOURCE_COLUMNS

log = get_logger(__name__)


class SourceUnavailableError(RuntimeError):
    pass


def fetch_sales(url: str, logical_date: date, target: Path, attempts: int = 4,
                timeout: float = 30.0) -> int:
    import requests

    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, params={"date": logical_date.isoformat()}, timeout=timeout)
            if response.status_code >= 500:
                raise SourceUnavailableError(f"HTTP {response.status_code}")
            response.raise_for_status()
            payload = response.json()
            break
        except (requests.RequestException, SourceUnavailableError, ValueError) as exc:
            last_error = exc
            if attempt == attempts:
                raise SourceUnavailableError(f"sales API failed after {attempts} attempts: {exc}") from exc
            delay = 2 ** attempt
            log.warning("sales API attempt %s failed (%s); retrying in %ss", attempt, exc, delay)
            time.sleep(delay)
    else:  # pragma: no cover
        raise SourceUnavailableError(str(last_error))

    records = payload.get("records") if isinstance(payload, dict) else payload
    if not isinstance(records, list) or not all(isinstance(r, dict) for r in records):
        raise ValueError("sales API returned an unexpected payload shape")
    records = sorted(records, key=lambda r: (str(r.get("order_item_id", "")), json.dumps(r, sort_keys=True)))
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            row = {c: "" if record.get(c) is None else str(record.get(c)) for c in SOURCE_COLUMNS}
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")
    log.info("fetched %s records from sales API for %s", len(records), logical_date)
    return len(records)
