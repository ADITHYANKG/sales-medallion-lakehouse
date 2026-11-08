"""Idempotent Metabase provisioning through its REST API.

1. first-run setup (admin user) if Metabase is not initialised yet
2. warehouse connection using the READ-ONLY ``metabase_reader`` account
3. "Sales Analytics" collection, native-SQL questions and two dashboards

Re-running updates the existing questions/dashboards in place (matched by name).
"""
from __future__ import annotations

import os
import time
import uuid
from urllib.parse import urlparse

from src.analytics.dashboards import DASHBOARDS
from src.config import Settings
from src.utils.logging import get_logger

log = get_logger(__name__)
DB_NAME = "Sales Warehouse"
COLLECTION = "Sales Analytics"
GRID_WIDTH = 24


class MetabaseClient:
    def __init__(self, base_url: str):
        import requests

        self.base = base_url.rstrip("/")
        self.http = requests.Session()

    def call(self, method: str, path: str, **kwargs):
        response = self.http.request(method, f"{self.base}/api{path}", timeout=120, **kwargs)
        if response.status_code >= 400:
            raise RuntimeError(f"Metabase {method} {path} -> {response.status_code}: {response.text[:500]}")
        return response.json() if response.content else None

    def wait_until_ready(self, timeout: int = 300) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if self.http.get(f"{self.base}/api/health", timeout=5).json().get("status") == "ok":
                    return
            except Exception:
                pass
            time.sleep(5)
        raise TimeoutError(f"Metabase at {self.base} did not become healthy in {timeout}s")


def _login(mb: MetabaseClient, email: str, password: str) -> None:
    props = mb.call("GET", "/session/properties")
    if not props.get("has-user-setup"):
        log.info("running Metabase first-time setup for %s", email)
        mb.call("POST", "/setup", json={
            "token": props["setup-token"],
            "user": {"email": email, "password": password, "first_name": "Pipeline",
                     "last_name": "Admin", "site_name": "Sales Data Engineering"},
            "prefs": {"site_name": "Sales Data Engineering", "site_locale": "en", "allow_tracking": False},
        })
    session = mb.call("POST", "/session", json={"username": email, "password": password})
    mb.http.headers["X-Metabase-Session"] = session["id"]


def _ensure_database(mb: MetabaseClient, settings: Settings) -> int:
    url = urlparse(settings.database_url)
    details = {
        "host": url.hostname, "port": url.port or 5432, "dbname": url.path.lstrip("/"),
        "user": settings.reader_user, "password": settings.reader_password, "ssl": False,
        "schema-filters-type": "inclusion", "schema-filters-patterns": "mart,monitoring",
    }
    databases = mb.call("GET", "/database")
    databases = databases.get("data", databases) if isinstance(databases, dict) else databases
    existing = next((d for d in databases if d["name"] == DB_NAME), None)
    if existing:
        mb.call("PUT", f"/database/{existing['id']}", json={"details": details})
        db_id = existing["id"]
    else:
        db_id = mb.call("POST", "/database", json={"engine": "postgres", "name": DB_NAME, "details": details,
                                                   "is_full_sync": True})["id"]
    mb.call("POST", f"/database/{db_id}/sync_schema")
    return db_id


def _ensure_collection(mb: MetabaseClient) -> int:
    for collection in mb.call("GET", "/collection"):
        if collection.get("name") == COLLECTION and not collection.get("archived"):
            return collection["id"]
    return mb.call("POST", "/collection", json={"name": COLLECTION,
                                                 "description": "Provisioned by src.cli provision-metabase"})["id"]


def _items(mb: MetabaseClient, collection_id: int, model: str) -> dict[str, int]:
    items = mb.call("GET", f"/collection/{collection_id}/items", params={"models": model})
    items = items.get("data", items) if isinstance(items, dict) else items
    return {i["name"]: i["id"] for i in items}


def _template_tags(tags: tuple[str, ...]) -> dict:
    kinds = {"start_date": ("date", "Start date"), "end_date": ("date", "End date"),
             "category": ("text", "Category")}
    return {t: {"id": str(uuid.uuid5(uuid.NAMESPACE_URL, t)), "name": t, "display-name": kinds[t][1],
                "type": kinds[t][0]} for t in tags}


def _upsert_card(mb: MetabaseClient, spec: dict, db_id: int, collection_id: int, existing: dict) -> int:
    body = {
        "name": spec["name"], "display": spec["display"], "collection_id": collection_id,
        "visualization_settings": spec.get("viz", {}),
        "dataset_query": {"type": "native", "database": db_id,
                          "native": {"query": spec["sql"].strip(),
                                     "template-tags": _template_tags(spec.get("tags", ()))}},
    }
    if spec["name"] in existing:
        mb.call("PUT", f"/card/{existing[spec['name']]}", json=body)
        return existing[spec["name"]]
    return mb.call("POST", "/card", json=body)["id"]


def _layout(cards: list[dict]) -> list[tuple[int, int]]:
    positions, row, col, row_height = [], 0, 0, 0
    for spec in cards:
        width, height = spec["size"]
        if col + width > GRID_WIDTH:
            row, col, row_height = row + row_height, 0, 0
        positions.append((row, col))
        col += width
        row_height = max(row_height, height)
    return positions


def provision(settings: Settings) -> dict:
    base = os.environ.get("METABASE_URL", "http://127.0.0.1:3000")
    email = os.environ.get("METABASE_ADMIN_EMAIL", "admin@example.com")
    password = os.environ.get("METABASE_ADMIN_PASSWORD", "")
    if not password or not settings.reader_password:
        raise ValueError("METABASE_ADMIN_PASSWORD and METABASE_DB_PASSWORD must be set")
    mb = MetabaseClient(base)
    mb.wait_until_ready()
    _login(mb, email, password)
    db_id = _ensure_database(mb, settings)
    collection_id = _ensure_collection(mb)
    existing_cards = _items(mb, collection_id, "card")
    existing_dashboards = _items(mb, collection_id, "dashboard")
    result = {"metabase_url": base, "database_id": db_id, "collection_id": collection_id, "dashboards": {}}
    for dash in DASHBOARDS:
        card_ids = [_upsert_card(mb, spec, db_id, collection_id, existing_cards) for spec in dash["cards"]]
        dash_id = existing_dashboards.get(dash["name"]) or mb.call("POST", "/dashboard", json={
            "name": dash["name"], "collection_id": collection_id, "description": dash["description"]})["id"]
        dashcards = []
        for index, (spec, card_id, (row, col)) in enumerate(zip(dash["cards"], card_ids, _layout(dash["cards"]))):
            mappings = [{"parameter_id": tag, "card_id": card_id, "target": ["variable", ["template-tag", tag]]}
                        for tag in spec.get("tags", ())]
            dashcards.append({"id": -(index + 1), "card_id": card_id, "row": row, "col": col,
                              "size_x": spec["size"][0], "size_y": spec["size"][1],
                              "parameter_mappings": mappings, "visualization_settings": {}})
        mb.call("PUT", f"/dashboard/{dash_id}", json={
            "name": dash["name"], "description": dash["description"],
            "parameters": dash["parameters"], "dashcards": dashcards})
        result["dashboards"][dash["name"]] = {"id": dash_id, "url": f"{base}/dashboard/{dash_id}",
                                              "cards": len(card_ids)}
        log.info("provisioned dashboard %s (%s cards)", dash["name"], len(card_ids))
    # Archive questions in our collection that are no longer defined (renamed/removed).
    wanted = {spec["name"] for dash in DASHBOARDS for spec in dash["cards"]}
    for name, card_id in existing_cards.items():
        if name not in wanted:
            mb.call("PUT", f"/card/{card_id}", json={"archived": True})
            log.info("archived obsolete question %s", name)
    return result
