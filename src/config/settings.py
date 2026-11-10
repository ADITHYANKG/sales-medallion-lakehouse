"""Environment-based configuration.

Every tunable value is read from environment variables (optionally loaded from a
local ``.env`` file) so that business logic never contains credentials or
host-specific paths. Defaults target the local Docker Compose stack when the code
runs on the host machine; the containers override hosts via compose.yaml.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv(path: Path) -> None:
    """Minimal .env loader (KEY=VALUE lines). Existing env vars always win."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


@dataclass(frozen=True)
class Settings:
    # PostgreSQL warehouse (writer account used by the pipeline).
    database_url: str
    # Read-only account provisioned for Metabase.
    reader_user: str
    reader_password: str
    # Object storage: "minio" (local, S3-compatible, static keys) or "aws" (Amazon S3).
    # With "aws", empty keys mean "use the default AWS credential chain" (EC2 IAM role).
    lake_storage: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    bronze_bucket: str
    silver_bucket: str
    gold_bucket: str
    # Spark.
    spark_master: str
    spark_shuffle_partitions: int
    spark_driver_memory: str
    spark_jars_dir: str
    # Pipeline behaviour.
    data_dir: Path
    daily_records: int
    generator_seed: int
    dirty_rate: float
    max_reject_rate: float
    versions_to_keep: int
    sales_api_url: str
    alert_webhook_url: str
    log_level: str
    allowed_currencies: tuple[str, ...] = field(default=("INR",))

    @property
    def rejected_dir(self) -> Path:
        return self.data_dir / "rejected"

    @property
    def generated_dir(self) -> Path:
        return self.data_dir / "generated"

    @property
    def incoming_dir(self) -> Path:
        return self.data_dir / "incoming"

    @property
    def uses_aws_s3(self) -> bool:
        return self.lake_storage == "aws"

    @property
    def s3_static_keys(self) -> bool:
        return bool(self.s3_access_key and self.s3_secret_key)

    @property
    def s3_configured(self) -> bool:
        if self.uses_aws_s3:  # credentials come from the IAM role when no keys are given
            return True
        return bool(self.s3_endpoint and self.s3_static_keys)


def load_settings(env_file: Path | None = None) -> Settings:
    _load_dotenv(env_file or PROJECT_ROOT / ".env")
    lake_storage = _env("LAKE_STORAGE", "minio").strip().lower()
    if lake_storage not in ("minio", "aws"):
        raise ValueError(f"LAKE_STORAGE must be 'minio' or 'aws', got {lake_storage!r}")
    aws = lake_storage == "aws"
    return Settings(
        lake_storage=lake_storage,
        database_url=_env(
            "DATABASE_URL", "postgresql://sales_user:local_dev_only@127.0.0.1:5434/sales_dw"
        ),
        reader_user=_env("METABASE_DB_USER", "metabase_reader"),
        reader_password=_env("METABASE_DB_PASSWORD", ""),
        # AWS: no endpoint (boto3/S3A pick the regional one) and no MinIO key fallback.
        s3_endpoint=_env("S3_ENDPOINT_URL", "" if aws else "http://127.0.0.1:9000"),
        s3_access_key=_env("S3_ACCESS_KEY", "" if aws else _env("MINIO_ROOT_USER", "")),
        s3_secret_key=_env("S3_SECRET_KEY", "" if aws else _env("MINIO_ROOT_PASSWORD", "")),
        s3_region=_env("S3_REGION", "us-east-1"),
        bronze_bucket=_env("LAKE_BRONZE_BUCKET", "bronze"),
        silver_bucket=_env("LAKE_SILVER_BUCKET", "silver"),
        gold_bucket=_env("LAKE_GOLD_BUCKET", "gold"),
        spark_master=_env("SPARK_MASTER", "local[*]"),
        spark_shuffle_partitions=_env_int("SPARK_SHUFFLE_PARTITIONS", 8),
        spark_driver_memory=_env("SPARK_DRIVER_MEMORY", "2g"),
        spark_jars_dir=_env("SPARK_JARS_DIR", "/opt/spark-jars"),
        data_dir=Path(_env("DATA_DIR", str(PROJECT_ROOT / "data"))),
        daily_records=_env_int("SALES_DAILY_RECORDS", 1000),
        generator_seed=_env_int("SALES_GENERATOR_SEED", 42),
        dirty_rate=_env_float("SALES_DIRTY_RATE", 0.02),
        max_reject_rate=_env_float("MAX_REJECT_RATE", 0.10),
        versions_to_keep=_env_int("LAKE_VERSIONS_TO_KEEP", 5),
        sales_api_url=_env("SALES_API_URL", ""),
        alert_webhook_url=_env("ALERT_WEBHOOK_URL", ""),
        log_level=_env("LOG_LEVEL", "INFO"),
        allowed_currencies=tuple(
            c.strip().upper() for c in _env("ALLOWED_CURRENCIES", "INR").split(",") if c.strip()
        ),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()
