"""Create .env from .env.example, replacing every ``change_me`` with a random secret.

Cross-platform (Windows/macOS/Linux), standard library only:

    python scripts/setup_env.py           # creates .env if it does not exist
    python scripts/setup_env.py --force   # regenerate (WARNING: existing volumes keep old passwords)
"""
from __future__ import annotations

import argparse
import base64
import os
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    target, template = ROOT / ".env", ROOT / ".env.example"
    if target.exists() and not args.force:
        print(f"{target} already exists (use --force to regenerate)")
        return 0
    lines = []
    for line in template.read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and value.strip() == "change_me":
            if key.strip() == "AIRFLOW_FERNET_KEY":
                value = base64.urlsafe_b64encode(os.urandom(32)).decode()
            else:
                value = secrets.token_urlsafe(24).replace("-", "x").replace("_", "y")
            line = f"{key}={value}"
        lines.append(line)
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {target} with freshly generated secrets")
    return 0


if __name__ == "__main__":
    sys.exit(main())
