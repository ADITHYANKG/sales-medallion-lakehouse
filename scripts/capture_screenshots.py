"""Capture UI screenshots (Metabase dashboards, Airflow DAG, MinIO console) for the docs.

Runs in the official Playwright image on the compose network (no host install needed):

    docker run --rm --network sales-data-engineering-starter_default --env-file .env \
      -v "${PWD}:/work" -w /work mcr.microsoft.com/playwright/python:v1.63.0-noble \
      bash -c "pip install -q playwright==1.63.0 requests && python scripts/capture_screenshots.py"

Images are written to docs/images/.
"""
from __future__ import annotations

import os
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright

OUT = Path("docs/images")
METABASE = os.environ.get("METABASE_URL", "http://metabase:3000")
AIRFLOW = os.environ.get("AIRFLOW_URL", "http://airflow-apiserver:8080")
MINIO = os.environ.get("MINIO_CONSOLE_URL", "http://minio:9001")


def metabase_dashboards() -> dict[str, int]:
    session = requests.post(f"{METABASE}/api/session", timeout=30, json={
        "username": os.environ["METABASE_ADMIN_EMAIL"], "password": os.environ["METABASE_ADMIN_PASSWORD"]}).json()["id"]
    dashboards = requests.get(f"{METABASE}/api/dashboard", headers={"X-Metabase-Session": session}, timeout=30).json()
    return session, {d["name"]: d["id"] for d in dashboards if not d.get("archived")}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    token, dashboards = metabase_dashboards()
    with sync_playwright() as p:
        browser = p.chromium.launch()
        # ---- Metabase
        ctx = browser.new_context(viewport={"width": 1600, "height": 1000}, device_scale_factor=1)
        ctx.add_cookies([{"name": "metabase.SESSION", "value": token, "url": METABASE}])
        page = ctx.new_page()
        for name, slug in (("Sales Analytics", "metabase_sales_dashboard"),
                           ("Pipeline Monitoring", "metabase_monitoring_dashboard")):
            if name not in dashboards:
                continue
            # empty query parameters reset any remembered filter values
            reset = "?start_date=&end_date=&category=" if name == "Sales Analytics" else ""
            page.goto(f"{METABASE}/dashboard/{dashboards[name]}{reset}", wait_until="networkidle", timeout=120_000)
            page.wait_for_timeout(6000)  # let charts render
            page.screenshot(path=str(OUT / f"{slug}.png"), full_page=True)
            print("saved", slug)
        ctx.close()

        # ---- Airflow (FAB login form)
        ctx = browser.new_context(viewport={"width": 1600, "height": 950})
        page = ctx.new_page()
        page.goto(f"{AIRFLOW}/auth/login/", wait_until="networkidle", timeout=120_000)
        page.fill("input[name=username]", os.environ.get("AIRFLOW_ADMIN_USER", "admin"))
        page.fill("input[name=password]", os.environ["AIRFLOW_ADMIN_PASSWORD"])
        page.click("input[type=submit], button[type=submit]")
        page.wait_for_load_state("networkidle")
        page.goto(f"{AIRFLOW}/dags/sales_data_pipeline", wait_until="networkidle", timeout=120_000)
        page.wait_for_timeout(5000)
        page.screenshot(path=str(OUT / "airflow_dag_runs.png"))
        print("saved airflow_dag_runs")
        ctx.close()

        # ---- MinIO console
        ctx = browser.new_context(viewport={"width": 1500, "height": 900})
        page = ctx.new_page()
        try:
            page.goto(f"{MINIO}/login", wait_until="networkidle", timeout=60_000)
            page.fill("#accessKey", os.environ["MINIO_ROOT_USER"])
            page.fill("#secretKey", os.environ["MINIO_ROOT_PASSWORD"])
            page.keyboard.press("Enter")
            page.wait_for_timeout(4000)
            page.goto(f"{MINIO}/browser/bronze/sales%2F", wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(3000)
            ack = page.get_by_role("button", name="Acknowledge")
            if ack.count():
                ack.first.click()
                page.wait_for_timeout(1500)
            page.screenshot(path=str(OUT / "minio_bronze_layer.png"))
            print("saved minio_bronze_layer")
        except Exception as exc:
            print("minio console not captured:", exc)
        browser.close()


if __name__ == "__main__":
    main()
