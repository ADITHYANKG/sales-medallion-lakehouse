"""Render docs/diagrams/architecture_linkedin.html to a PNG (1080x1350, 4:5, rendered at 2x).

Run inside the Playwright image (fonts: Windows Segoe UI mounted read-only, optional):

    docker run --rm -v "${PWD}:/work" -v "C:/Windows/Fonts:/winfonts:ro" -w /work \
      mcr.microsoft.com/playwright/python:v1.63.0-noble \
      bash -c "pip install -q playwright==1.63.0 && python scripts/render_diagram.py"
"""
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "diagrams" / "architecture_linkedin.html"
TARGET = ROOT / "docs" / "images" / "architecture_linkedin.png"


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1080, "height": 1350}, device_scale_factor=2)
        page.goto(SOURCE.as_uri())
        page.evaluate("document.fonts.ready")
        page.screenshot(path=str(TARGET), full_page=False)
        browser.close()
    print(f"wrote {TARGET}")


if __name__ == "__main__":
    main()
