"""Reproducible Pandas vs PySpark benchmark of the Bronze -> Silver -> Gold logic.

Both engines run the *same* business rules on the *same* generated CSV:
read -> validate -> de-duplicate -> write Silver Parquet -> build 6 Gold tables.

Method
------
* Each (engine, size) runs in a fresh subprocess so peak-memory numbers are not
  polluted by earlier runs. Peak RSS is sampled every 50 ms (for Spark: the
  Python driver process + its JVM child).
* Each worker executes the pipeline ``repeats`` times. Run 1 is reported as the
  *cold* time (Spark: includes JVM/JIT warm-up, but not SparkSession start-up,
  which is reported separately); the best of the remaining runs is the *warm* time.
* Local files only (no MinIO) so storage latency does not dominate.

Run: ``python -m src.cli benchmark --sizes 10000,100000,1000000``
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil

from src.config import PROJECT_ROOT, load_settings


class PeakMemory:
    """Samples RSS of this process and its children (JVM) in a background thread."""

    def __init__(self, interval: float = 0.05):
        self.interval, self.peak, self._stop = interval, 0, threading.Event()
        self._proc = psutil.Process()

    def _rss(self) -> int:
        total = self._proc.memory_info().rss
        for child in self._proc.children(recursive=True):
            try:
                total += child.memory_info().rss
            except psutil.Error:
                pass
        return total

    def __enter__(self):
        self.peak = self._rss()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def _run(self):
        while not self._stop.is_set():
            self.peak = max(self.peak, self._rss())
            time.sleep(self.interval)

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()
        self.peak = max(self.peak, self._rss())


def _pandas_once(csv_path: Path, out_dir: Path) -> dict:
    import pandas as pd

    from src.transformations import pandas_transforms as PT

    raw = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    silver, rejected, stats = PT.bronze_to_silver([(raw, "bench", "2026-01-01")])
    silver.to_parquet(out_dir / "silver.parquet", index=False)
    gold = PT.build_gold(silver)
    for name, frame in gold.items():
        frame.to_parquet(out_dir / f"{name}.parquet", index=False)
    return {"silver_records": int(len(silver)), "rejected_records": int(len(rejected)),
            "gold_rows": {k: int(len(v)) for k, v in gold.items()}, "partitions": 1}


def _spark_once(spark, csv_path: Path, out_dir: Path, partitions: int) -> dict:
    from src.transformations import spark_jobs as SJ

    bronze_dir = csv_path.parent  # .../ingestion_date=*/batch_id=*/sales.csv layout
    raw = SJ.read_bronze(spark, str(bronze_dir.parent.parent))
    silver, rejected, _valid, annotated = SJ.bronze_to_silver(raw, ("INR",))
    SJ.write_silver(silver, str(out_dir / "silver"), partitions)
    silver_written = SJ.read_silver(spark, str(out_dir / "silver"))
    gold_rows = {}
    for name, frame in SJ.build_gold(silver_written).items():
        SJ.write_small(frame, str(out_dir / name))
        gold_rows[name] = spark.read.parquet(str(out_dir / name)).count()
    result = {"silver_records": silver_written.count(), "rejected_records": rejected.count(),
              "gold_rows": gold_rows, "partitions": raw.rdd.getNumPartitions()}
    annotated.unpersist()
    return result


def worker(engine: str, csv_path: Path, repeats: int) -> dict:
    settings = load_settings()
    timings, details, startup = [], {}, None
    spark = None
    with PeakMemory() as memory:
        if engine == "spark":
            from src.transformations.spark_session import build_spark

            t0 = time.perf_counter()
            spark = build_spark(settings, "benchmark", master=settings.spark_master)
            startup = time.perf_counter() - t0
        for _ in range(repeats):
            out_dir = Path(tempfile.mkdtemp(prefix=f"bench_{engine}_"))
            t0 = time.perf_counter()
            if engine == "pandas":
                details = _pandas_once(csv_path, out_dir)
            else:
                details = _spark_once(spark, csv_path, out_dir, settings.spark_shuffle_partitions)
            timings.append(time.perf_counter() - t0)
            shutil.rmtree(out_dir, ignore_errors=True)
    result = {"engine": engine, "timings": timings, "startup_seconds": startup,
              "peak_rss_mb": round(memory.peak / 1024 ** 2, 1), **details}
    if spark is not None:
        result["spark_master"] = spark.sparkContext.master
        result["shuffle_partitions"] = settings.spark_shuffle_partitions
        spark.stop()
    return result


def _prepare_input(size: int) -> Path:
    from src.ingestion.generator import generate_dataset

    path = (PROJECT_ROOT / "data" / "generated" / "benchmark" / f"size={size}"
            / "ingestion_date=2026-01-01" / "batch_id=bench" / "sales.csv")
    if not path.exists():
        generate_dataset(path, records=size, seed=42, days=90, dirty_rate=0.02)
    return path


def run_benchmark(sizes: list[int], output: Path, repeats: int = 2) -> list[dict]:
    rows = []
    for size in sizes:
        csv_path = _prepare_input(size)
        for engine in ("pandas", "spark"):
            cmd = [sys.executable, "-m", "src.benchmarks.pandas_vs_spark", "--worker", engine,
                   "--input", str(csv_path), "--repeats", str(repeats)]
            print(f"running {engine} on {size:,} records ...", flush=True)
            proc = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)
            if proc.returncode != 0:
                raise RuntimeError(f"{engine} worker failed:\n{proc.stderr[-3000:]}")
            result = json.loads(proc.stdout.strip().splitlines()[-1])
            result.update({"size": size, "input_mb": round(csv_path.stat().st_size / 1024 ** 2, 2),
                           "input_rows": sum(1 for _ in csv_path.open(encoding="utf-8")) - 1})
            rows.append(result)
            print(f"  cold={result['timings'][0]:.2f}s warm={min(result['timings'][1:] or result['timings']):.2f}s "
                  f"peak={result['peak_rss_mb']}MB silver={result['silver_records']}", flush=True)
    _write_report(rows, output, repeats)
    return rows


def _write_report(rows: list[dict], output: Path, repeats: int) -> None:
    import pandas as pd
    import pyspark

    vm = psutil.virtual_memory()
    lines = [
        "# Pandas vs PySpark benchmark results",
        "",
        f"Generated by `python -m src.cli benchmark` on {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}.",
        "These are **measured** numbers from one machine; re-run the command to reproduce on yours.",
        "",
        "## Environment",
        "",
        f"* Host: {platform.platform()} | CPUs visible: {os.cpu_count()} | RAM visible: {vm.total / 1024 ** 3:.1f} GiB",
        f"* Python {platform.python_version()} | pandas {pd.__version__} | PySpark {pyspark.__version__}",
        f"* Spark master: `{next((r.get('spark_master') for r in rows if r.get('spark_master')), 'n/a')}` | "
        f"shuffle partitions: {next((r.get('shuffle_partitions') for r in rows if r.get('shuffle_partitions')), 'n/a')}",
        f"* Repeats per engine/size: {repeats} (cold = run 1; warm = best of later runs)",
        "* Workload: read CSV -> validate (11 rules) -> de-duplicate -> write Silver Parquet -> 6 Gold aggregates",
        "",
        "## Results",
        "",
        "| Records (clean) | Input rows | Input MB | Engine | Partitions | Spark start-up s | Cold s | Warm s | Peak RSS MB | Silver rows | Rejected |",
        "|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        warm = min(r["timings"][1:]) if len(r["timings"]) > 1 else r["timings"][0]
        startup = f"{r['startup_seconds']:.1f}" if r.get("startup_seconds") else "-"
        lines.append(f"| {r['size']:,} | {r['input_rows']:,} | {r['input_mb']} | {r['engine']} | {r['partitions']} | "
                     f"{startup} | {r['timings'][0]:.2f} | {warm:.2f} | {r['peak_rss_mb']} | "
                     f"{r['silver_records']:,} | {r['rejected_records']:,} |")
    lines += ["", "Both engines produced identical Silver and rejected row counts for every size."
              if _counts_match(rows) else
              "**WARNING: engine outputs differ - investigate before trusting timings.**"]
    lines += [
        "",
        "## Limitations",
        "",
        "* Single machine, `local[*]` Spark: no network shuffle, so this measures engine overhead and "
        "parallelism on one host, not cluster scalability.",
        "* Spark timings include Parquet writes and the extra actions used to count outputs.",
        "* Peak RSS for Spark includes the whole JVM heap reservation (bounded by `SPARK_DRIVER_MEMORY`), "
        "so it reflects configuration as much as data size.",
        "* The pandas implementation hashes rows in Python (`sha256` per row), which is a deliberate "
        "parity choice, not the fastest possible pandas code.",
        "* Results vary with CPU, disk, Docker Desktop VM limits and background load.",
        "",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")
    (output.with_suffix(".json")).write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"wrote {output}")


def _counts_match(rows: list[dict]) -> bool:
    by_size: dict[int, set] = {}
    for r in rows:
        by_size.setdefault(r["size"], set()).add((r["silver_records"], r["rejected_records"]))
    return all(len(v) == 1 for v in by_size.values())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", choices=("pandas", "spark"), required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=2)
    args = parser.parse_args()
    import logging

    logging.disable(logging.CRITICAL)
    print(json.dumps(worker(args.worker, args.input, args.repeats)))
