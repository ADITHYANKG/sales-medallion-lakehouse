"""Consistent logging configuration for CLI, Spark driver and Airflow tasks."""
from __future__ import annotations

import logging
import os

_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def configure_logging(level: str | None = None) -> None:
    level_name = (level or os.environ.get("LOG_LEVEL") or "INFO").upper()
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=level_name, format=_FORMAT)
    else:  # Airflow already installs handlers; only adjust the level.
        root.setLevel(level_name)
    # Third-party libraries are noisy at INFO.
    for noisy in ("botocore", "boto3", "urllib3", "py4j", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
