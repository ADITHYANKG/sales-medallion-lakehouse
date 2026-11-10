"""LAKE_STORAGE=aws (Amazon S3 + IAM role) vs the default MinIO configuration."""
import importlib.util
import shutil
from pathlib import Path

import pytest

from src.config.settings import load_settings
from src.storage.object_store import ObjectStore, client_kwargs
from src.transformations.spark_session import spark_conf

ROOT = Path(__file__).resolve().parents[2]
S3_VARS = ("LAKE_STORAGE", "S3_ENDPOINT_URL", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_REGION",
           "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")


@pytest.fixture()
def env(monkeypatch, tmp_path):
    for name in S3_VARS:
        monkeypatch.delenv(name, raising=False)

    def load(**values):
        for key, value in values.items():
            monkeypatch.setenv(key, value)
        return load_settings(env_file=tmp_path / "missing.env")
    return load


def test_minio_is_the_default(env):
    s = env(MINIO_ROOT_USER="lake_admin", MINIO_ROOT_PASSWORD="pw")
    assert s.lake_storage == "minio" and not s.uses_aws_s3
    assert s.s3_endpoint == "http://127.0.0.1:9000"
    assert (s.s3_access_key, s.s3_secret_key) == ("lake_admin", "pw") and s.s3_configured


def test_aws_mode_uses_iam_role_and_ignores_minio_keys(env):
    s = env(LAKE_STORAGE="AWS", S3_REGION="ap-south-1", S3_ENDPOINT_URL="",
            MINIO_ROOT_USER="lake_admin", MINIO_ROOT_PASSWORD="pw")
    assert s.uses_aws_s3 and s.s3_configured
    assert s.s3_endpoint == "" and not s.s3_static_keys


def test_invalid_lake_storage(env):
    with pytest.raises(ValueError, match="LAKE_STORAGE"):
        env(LAKE_STORAGE="gcs")


def test_boto3_kwargs(env):
    aws = env(LAKE_STORAGE="aws", S3_REGION="ap-south-1")
    assert client_kwargs(aws) == {"region_name": "ap-south-1"}  # default credential chain
    minio = env(LAKE_STORAGE="minio", S3_REGION="us-east-1", S3_ENDPOINT_URL="http://minio:9000",
                S3_ACCESS_KEY="k", S3_SECRET_KEY="s")
    assert client_kwargs(minio) == {"region_name": "us-east-1", "endpoint_url": "http://minio:9000",
                                    "aws_access_key_id": "k", "aws_secret_access_key": "s"}


def test_spark_s3a_aws_uses_instance_profile(env):
    conf = spark_conf(env(LAKE_STORAGE="aws", S3_REGION="ap-south-1"))
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "s3.ap-south-1.amazonaws.com"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "false"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"
    assert conf["spark.hadoop.fs.s3a.aws.credentials.provider"].endswith("auth.IAMInstanceCredentialsProvider")
    assert "spark.hadoop.fs.s3a.access.key" not in conf
    assert "spark.hadoop.fs.s3a.change.detection.mode" not in conf


def test_spark_s3a_aws_with_explicit_keys(env):
    conf = spark_conf(env(LAKE_STORAGE="aws", S3_ACCESS_KEY="k", S3_SECRET_KEY="s"))
    assert conf["spark.hadoop.fs.s3a.aws.credentials.provider"].endswith("SimpleAWSCredentialsProvider")
    assert conf["spark.hadoop.fs.s3a.access.key"] == "k"


def test_spark_s3a_minio_unchanged(env):
    conf = spark_conf(env(S3_ENDPOINT_URL="http://minio:9000", S3_ACCESS_KEY="k", S3_SECRET_KEY="s"))
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.hadoop.fs.s3a.change.detection.mode"] == "none"
    assert conf["spark.hadoop.fs.s3a.aws.credentials.provider"].endswith("SimpleAWSCredentialsProvider")


class _MissingBucketClient:
    def __init__(self):
        self.created = []

    def head_bucket(self, Bucket):
        from botocore.exceptions import ClientError
        raise ClientError({"Error": {"Code": "404"}}, "HeadBucket")

    def create_bucket(self, **kwargs):
        self.created.append(kwargs)


@pytest.mark.parametrize("region, expected", [
    ("ap-south-1", {"Bucket": "b", "CreateBucketConfiguration": {"LocationConstraint": "ap-south-1"}}),
    ("us-east-1", {"Bucket": "b"}),
])
def test_bucket_creation_is_region_aware(env, region, expected):
    client = _MissingBucketClient()
    assert ObjectStore(env(LAKE_STORAGE="aws", S3_REGION=region), client=client).ensure_bucket("b")
    assert client.created == [expected]


def _load_setup_env(tmp_path, monkeypatch):
    shutil.copy(ROOT / ".env.example", tmp_path / ".env.example")
    spec = importlib.util.spec_from_file_location("setup_env", ROOT / "scripts" / "setup_env.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ROOT", tmp_path)
    return module


def _read_env(path):
    pairs = [line.split("=", 1) for line in path.read_text().splitlines() if "=" in line and not line.startswith("#")]
    keys = [k for k, _ in pairs]
    assert len(keys) == len(set(keys)), "duplicate keys in generated .env"
    return dict(pairs)


def test_setup_env_aws(tmp_path, monkeypatch):
    module = _load_setup_env(tmp_path, monkeypatch)
    monkeypatch.setattr("sys.argv", ["setup_env.py", "--aws", "--region", "ap-south-1",
                                     "--bucket-prefix", "jane-sales"])
    assert module.main() == 0
    env = _read_env(tmp_path / ".env")
    assert env["LAKE_STORAGE"] == "aws" and env["S3_REGION"] == "ap-south-1"
    assert env["COMPOSE_FILE"] == "compose.yaml:compose.aws.yaml"
    assert "S3_ENDPOINT_URL" not in env
    assert (env["LAKE_BRONZE_BUCKET"], env["LAKE_SILVER_BUCKET"], env["LAKE_GOLD_BUCKET"]) == (
        "jane-sales-bronze", "jane-sales-silver", "jane-sales-gold")
    assert env["POSTGRES_PASSWORD"] not in ("local_dev_only", "change_me")
    assert env["DATABASE_URL"] == f"postgresql://sales_user:{env['POSTGRES_PASSWORD']}@127.0.0.1:5434/sales_dw"
    assert "change_me" not in env.values()


def test_setup_env_default_is_minio(tmp_path, monkeypatch):
    module = _load_setup_env(tmp_path, monkeypatch)
    monkeypatch.setattr("sys.argv", ["setup_env.py"])
    assert module.main() == 0
    env = _read_env(tmp_path / ".env")
    assert env["LAKE_STORAGE"] == "minio" and env["LAKE_BRONZE_BUCKET"] == "bronze"
    assert env["POSTGRES_PASSWORD"] == "local_dev_only" and "COMPOSE_FILE" not in env


def test_setup_env_aws_requires_valid_prefix(tmp_path, monkeypatch):
    module = _load_setup_env(tmp_path, monkeypatch)
    monkeypatch.setattr("sys.argv", ["setup_env.py", "--aws", "--bucket-prefix", "Bad_Name"])
    with pytest.raises(SystemExit):
        module.main()
