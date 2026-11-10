"""S3-compatible object storage helpers (MinIO locally, Amazon S3 in the cloud).

Versioned datasets
------------------
Silver and Gold datasets are written to an immutable version prefix
(``<dataset>/version=<id>/``) and then made visible by atomically replacing a tiny
pointer object ``<dataset>/_CURRENT.json``. Readers resolve the pointer, so they
never observe a half-written dataset, and a failed or retried run cannot corrupt
the published version. Old versions are pruned after ``LAKE_VERSIONS_TO_KEEP``.
"""
from __future__ import annotations

import hashlib
import io
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from src.config import Settings
from src.utils.logging import get_logger

log = get_logger(__name__)
POINTER = "_CURRENT.json"


def client_kwargs(settings: Settings) -> dict[str, str]:
    """boto3 arguments. Without static keys boto3 uses its default credential chain
    (environment, ~/.aws, EC2 instance profile), which is how an IAM role is used on AWS."""
    kwargs = {"region_name": settings.s3_region}
    if settings.s3_endpoint:
        kwargs["endpoint_url"] = settings.s3_endpoint
    if settings.s3_static_keys:
        kwargs["aws_access_key_id"] = settings.s3_access_key
        kwargs["aws_secret_access_key"] = settings.s3_secret_key
    return kwargs


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    etag: str
    last_modified: datetime


class ObjectStore:
    def __init__(self, settings: Settings, client=None):
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client("s3", **client_kwargs(settings), config=Config(
                s3={"addressing_style": "virtual" if settings.uses_aws_s3 else "path"},
                retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=5, read_timeout=60))
        self.client = client
        self.settings = settings

    # ---- buckets --------------------------------------------------------------------
    def ensure_bucket(self, bucket: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_bucket(Bucket=bucket)
            return False
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchBucket", "NotFound"):
                raise
        region = self.settings.s3_region
        if region and region != "us-east-1":  # AWS rejects an explicit us-east-1 constraint
            self.client.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
        else:
            self.client.create_bucket(Bucket=bucket)
        log.info("created bucket %s", bucket)
        return True

    def ensure_lake_buckets(self) -> list[str]:
        s = self.settings
        return [b for b in (s.bronze_bucket, s.silver_bucket, s.gold_bucket) if self.ensure_bucket(b)]

    # ---- objects ----------------------------------------------------------------------
    def upload_file(self, path: Path, bucket: str, key: str, metadata: dict[str, str] | None = None) -> None:
        self.client.upload_file(str(path), bucket, key,
                                ExtraArgs={"Metadata": {k: str(v) for k, v in (metadata or {}).items()}})

    def put_bytes(self, data: bytes, bucket: str, key: str, content_type: str = "application/octet-stream",
                  metadata: dict[str, str] | None = None) -> None:
        self.client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type,
                               Metadata={k: str(v) for k, v in (metadata or {}).items()})

    def put_json(self, payload: Any, bucket: str, key: str) -> None:
        self.put_bytes(json.dumps(payload, indent=2, default=str).encode(), bucket, key, "application/json")

    def get_bytes(self, bucket: str, key: str) -> bytes:
        return self.client.get_object(Bucket=bucket, Key=key)["Body"].read()

    def get_json(self, bucket: str, key: str) -> Any:
        return json.loads(self.get_bytes(bucket, key))

    def download_file(self, bucket: str, key: str, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(bucket, key, str(path))
        return path

    def exists(self, bucket: str, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def metadata(self, bucket: str, key: str) -> dict[str, Any]:
        head = self.client.head_object(Bucket=bucket, Key=key)
        return {"size": head["ContentLength"], "etag": head["ETag"].strip('"'),
                "last_modified": head["LastModified"], "content_type": head.get("ContentType"),
                "metadata": head.get("Metadata", {})}

    def list_objects(self, bucket: str, prefix: str = "") -> Iterator[ObjectInfo]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                yield ObjectInfo(item["Key"], item["Size"], item["ETag"].strip('"'), item["LastModified"])

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        keys = [o.key for o in self.list_objects(bucket, prefix)]
        for start in range(0, len(keys), 1000):
            self.client.delete_objects(Bucket=bucket, Delete={
                "Objects": [{"Key": k} for k in keys[start:start + 1000]], "Quiet": True})
        return len(keys)

    def checksum(self, bucket: str, key: str) -> str:
        """sha256 of the object's content (streamed)."""
        digest = hashlib.sha256()
        body = self.client.get_object(Bucket=bucket, Key=key)["Body"]
        for block in iter(lambda: body.read(1024 * 1024), b""):
            digest.update(block)
        return digest.hexdigest()

    # ---- parquet ------------------------------------------------------------------------
    def write_parquet(self, frame: pd.DataFrame, bucket: str, key: str) -> None:
        buffer = io.BytesIO()
        frame.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
        self.put_bytes(buffer.getvalue(), bucket, key, "application/vnd.apache.parquet")

    def read_parquet(self, bucket: str, prefix: str) -> pd.DataFrame:
        """Read one Parquet object or every ``*.parquet`` object under a prefix."""
        keys = [prefix] if prefix.endswith(".parquet") else sorted(
            o.key for o in self.list_objects(bucket, prefix) if o.key.endswith(".parquet"))
        if not keys:
            raise FileNotFoundError(f"no parquet objects at s3://{bucket}/{prefix}")
        frames = [pd.read_parquet(io.BytesIO(self.get_bytes(bucket, k)), engine="pyarrow") for k in keys]
        return pd.concat(frames, ignore_index=True)

    # ---- versioned datasets -------------------------------------------------------------
    def publish_version(self, bucket: str, dataset: str, version: str, info: dict | None = None) -> dict:
        pointer = {"dataset": dataset, "version": version,
                   "prefix": f"{dataset}/version={version}/",
                   "published_at": datetime.now(timezone.utc).isoformat(), **(info or {})}
        self.put_json(pointer, bucket, f"{dataset}/{POINTER}")
        return pointer

    def current_version(self, bucket: str, dataset: str) -> dict | None:
        key = f"{dataset}/{POINTER}"
        return self.get_json(bucket, key) if self.exists(bucket, key) else None

    def prune_versions(self, bucket: str, dataset: str, keep: int) -> list[str]:
        current = self.current_version(bucket, dataset)
        versions: dict[str, datetime] = {}
        for obj in self.list_objects(bucket, f"{dataset}/version="):
            version = obj.key[len(dataset) + len("/version="):].split("/", 1)[0]
            versions[version] = max(versions.get(version, obj.last_modified), obj.last_modified)
        ordered = sorted(versions, key=lambda v: versions[v], reverse=True)
        doomed = [v for v in ordered[keep:] if not current or v != current["version"]]
        for version in doomed:
            self.delete_prefix(bucket, f"{dataset}/version={version}/")
        return doomed
