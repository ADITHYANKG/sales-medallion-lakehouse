"""SparkSession factory with MinIO (S3A) and PostgreSQL (JDBC) support."""
from __future__ import annotations

import glob
import os

from src.config import Settings
from src.utils.logging import get_logger

log = get_logger(__name__)


def spark_conf(settings: Settings) -> dict[str, str]:
    conf = {
        "spark.sql.session.timeZone": "UTC",
        "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.sql.shuffle.partitions": str(settings.spark_shuffle_partitions),
        "spark.default.parallelism": str(settings.spark_shuffle_partitions),
        "spark.driver.memory": settings.spark_driver_memory,
        "spark.sql.adaptive.enabled": "true",
        # Keep partition values (batch ids, dates) as strings; types come from our schema.
        "spark.sql.sources.partitionColumnTypeInference.enabled": "false",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.ui.showConsoleProgress": "false",
    }
    jars = sorted(glob.glob(os.path.join(settings.spark_jars_dir, "*.jar")))
    if jars:
        conf["spark.jars"] = ",".join(jars)
    if settings.s3_configured:
        conf.update({
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": settings.s3_endpoint,
            "spark.hadoop.fs.s3a.endpoint.region": settings.s3_region,
            "spark.hadoop.fs.s3a.access.key": settings.s3_access_key,
            "spark.hadoop.fs.s3a.secret.key": settings.s3_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(settings.s3_endpoint.startswith("https")).lower(),
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            # MinIO does not need the analytics accelerator / checksums used by AWS.
            "spark.hadoop.fs.s3a.input.stream.type": "classic",
            "spark.hadoop.fs.s3a.change.detection.mode": "none",
        })
    driver_host = os.environ.get("SPARK_DRIVER_HOST")
    if driver_host:  # needed when executors run in other containers (standalone cluster)
        conf["spark.driver.host"] = driver_host
        conf["spark.driver.bindAddress"] = "0.0.0.0"
    return conf


def build_spark(settings: Settings, app_name: str, master: str | None = None):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).master(master or settings.spark_master)
    for key, value in spark_conf(settings).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark %s session '%s' on %s (shuffle partitions=%s)", spark.version, app_name,
             spark.sparkContext.master, settings.spark_shuffle_partitions)
    return spark
