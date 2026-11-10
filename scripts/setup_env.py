"""Create .env from .env.example, replacing every ``change_me`` with a random secret.

Cross-platform (Windows/macOS/Linux), standard library only:

    python scripts/setup_env.py           # creates .env if it does not exist
    python scripts/setup_env.py --force   # regenerate (WARNING: existing volumes keep old passwords)

    # AWS (EC2 + S3 lake, see docs/DEPLOY_AWS.md):
    python3 scripts/setup_env.py --aws --region ap-south-1 --bucket-prefix yourname-sales
"""
from __future__ import annotations

import argparse
import base64
import os
import re
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,48}[a-z0-9]$")


def _secret() -> str:
    return secrets.token_urlsafe(24).replace("-", "x").replace("_", "y")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--aws", action="store_true", help="use Amazon S3 (IAM role) instead of MinIO")
    parser.add_argument("--region", default="ap-south-1", help="AWS region of the S3 buckets")
    parser.add_argument("--bucket-prefix", help="globally unique prefix, e.g. yourname-sales")
    args = parser.parse_args()
    if args.aws and not (args.bucket_prefix and BUCKET_RE.match(args.bucket_prefix)):
        parser.error("--aws needs --bucket-prefix: lower-case letters, digits and '-', 3-50 characters")

    target, template = ROOT / ".env", ROOT / ".env.example"
    if target.exists() and not args.force:
        print(f"{target} already exists (use --force to regenerate)")
        return 0

    values: dict[str, str] = {}
    lines = []
    for line in template.read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        key = key.strip()
        if sep and value.strip() == "change_me":
            value = base64.urlsafe_b64encode(os.urandom(32)).decode() if key == "AIRFLOW_FERNET_KEY" else _secret()
            line = f"{key}={value}"
        if args.aws and sep:
            # A new server has empty volumes, so the warehouse password can be random too.
            if key == "POSTGRES_PASSWORD":
                value = _secret()
                line = f"{key}={value}"
            elif key == "DATABASE_URL":
                user, db, port = values["POSTGRES_USER"], values["POSTGRES_DB"], values["POSTGRES_PORT"]
                line = f"{key}=postgresql://{user}:{values['POSTGRES_PASSWORD']}@127.0.0.1:{port}/{db}"
            elif key == "LAKE_STORAGE":
                line = f"{key}=aws"
            elif key == "S3_REGION":
                line = f"{key}={args.region}"
            elif key == "S3_ENDPOINT_URL":
                continue  # Amazon S3: boto3/Spark use the regional endpoint
            elif key in ("LAKE_BRONZE_BUCKET", "LAKE_SILVER_BUCKET", "LAKE_GOLD_BUCKET"):
                layer = key.split("_")[1].lower()
                line = f"{key}={args.bucket_prefix}-{layer}"
        if sep:
            values[key] = line.partition("=")[2]
        lines.append(line)

    if args.aws:
        lines += ["", "# Makes every `docker compose ...` command on the server include the AWS override.",
                  "COMPOSE_FILE=compose.yaml:compose.aws.yaml"]
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {target} with freshly generated secrets")
    if args.aws:
        print(f"buckets: {args.bucket_prefix}-bronze, {args.bucket_prefix}-silver, {args.bucket_prefix}-gold "
              f"in {args.region}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
