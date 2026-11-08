import pandas as pd
import pytest

from src.utils.ids import sha256_file

pytestmark = pytest.mark.integration


def test_upload_download_exists_metadata_checksum(clean_lake, test_settings, tmp_path):
    source = tmp_path / "sales.csv"
    source.write_text("a,b\n1,2\n", encoding="utf-8")
    bucket, key = test_settings.bronze_bucket, "sales/ingestion_date=2026-01-01/batch_id=b1/sales.csv"
    assert not clean_lake.exists(bucket, key)
    clean_lake.upload_file(source, bucket, key, {"checksum-sha256": sha256_file(source)})
    assert clean_lake.exists(bucket, key)
    meta = clean_lake.metadata(bucket, key)
    assert meta["size"] == source.stat().st_size
    assert meta["metadata"]["checksum-sha256"] == sha256_file(source)
    assert clean_lake.checksum(bucket, key) == sha256_file(source)
    target = clean_lake.download_file(bucket, key, tmp_path / "copy.csv")
    assert target.read_bytes() == source.read_bytes()
    assert [o.key for o in clean_lake.list_objects(bucket, "sales/")] == [key]


def test_parquet_round_trip(clean_lake, test_settings):
    frame = pd.DataFrame({"id": [1, 2], "amount": [10.5, 20.25]})
    clean_lake.write_parquet(frame, test_settings.gold_bucket, "t/part-0.parquet")
    clean_lake.write_parquet(frame, test_settings.gold_bucket, "t/part-1.parquet")
    back = clean_lake.read_parquet(test_settings.gold_bucket, "t/")
    assert len(back) == 4 and back["amount"].sum() == 61.5


def test_versioned_publish_and_prune(clean_lake, test_settings):
    bucket = test_settings.silver_bucket
    for version in ("v1", "v2", "v3"):
        clean_lake.put_bytes(b"x", bucket, f"sales/version={version}/part.parquet")
        clean_lake.publish_version(bucket, "sales", version)
    assert clean_lake.current_version(bucket, "sales")["prefix"] == "sales/version=v3/"
    pruned = clean_lake.prune_versions(bucket, "sales", keep=1)
    assert set(pruned) == {"v1", "v2"}
    assert clean_lake.exists(bucket, "sales/version=v3/part.parquet")
    assert not clean_lake.exists(bucket, "sales/version=v1/part.parquet")
