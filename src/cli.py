"""Command-line entry point: ``python -m src.cli <command>``.

Run inside Docker (recommended, works the same on Windows/macOS/Linux):

    docker compose run --rm app python -m src.cli <command> [options]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path

from src.config import PROJECT_ROOT, get_settings
from src.utils.ids import new_run_id
from src.utils.logging import configure_logging, get_logger

log = get_logger("src.cli")
ANALYTICS_SQL = PROJECT_ROOT / "sql" / "analytics" / "sales_kpis.sql"


def _print(payload) -> None:
    print(json.dumps(payload, indent=2, default=str))


def cmd_init(args) -> int:
    from src.warehouse.migrate import ensure_reader_role, migrate

    settings = get_settings()
    applied = migrate(settings)
    ensure_reader_role(settings)
    buckets = []
    if not args.skip_s3 and settings.s3_configured:
        from src.storage.object_store import ObjectStore

        buckets = ObjectStore(settings).ensure_lake_buckets()
    for folder in (settings.generated_dir, settings.rejected_dir, settings.incoming_dir):
        folder.mkdir(parents=True, exist_ok=True)
    _print({"migrations_applied": applied, "buckets_created": buckets})
    return 0


def cmd_generate(args) -> int:
    from src.ingestion.generator import generate_daily_batch, generate_dataset

    settings = get_settings()
    seed = settings.generator_seed if args.seed is None else args.seed
    if args.daily:
        stats = generate_daily_batch(args.output, date.fromisoformat(args.daily), args.records, seed,
                                     args.dirty_rate, fmt=args.format)
    else:
        stats = generate_dataset(args.output, args.records, seed, date.fromisoformat(args.start_date),
                                 args.days, args.dirty_rate, args.update_rate, fmt=args.format)
    _print({"output": str(args.output), **stats.as_dict()})
    return 0


def cmd_ingest(args) -> int:
    from src.ingestion.csv_ingest import ingest_file

    summary = ingest_file(args.input, get_settings())
    _print(summary)
    print("Rerun the command to verify idempotency (fact_rows and completed_revenue stay unchanged).")
    return 0


def cmd_lake_run(args) -> int:
    from src.orchestration.lake_pipeline import run_lake_pipeline

    settings = get_settings()
    start = date.fromisoformat(args.date)
    days = [start + timedelta(days=i) for i in range(args.days)]
    for day in days:
        run_id = args.run_id or new_run_id(f"cli_{day.isoformat()}")
        result = run_lake_pipeline(settings, day, run_id, args.source)
        _print({"logical_date": day, "run_id": run_id,
                "silver": {k: result["transform_silver"].get(k) for k in (
                    "source_records", "valid_records", "rejected_records", "duplicate_records", "silver_records")},
                "warehouse": result["load_data_warehouse"]})
    return 0


def cmd_verify(args) -> int:
    from src.validation.quality import GOLD_RECONCILIATION_CHECKS, WAREHOUSE_CHECKS, run_sql_checks
    from src.warehouse.db import connect

    with connect(get_settings()) as conn:
        cur = conn.cursor()
        checks = WAREHOUSE_CHECKS + (GOLD_RECONCILIATION_CHECKS if args.gold else [])
        results = run_sql_checks(cur, checks)
    for r in results:
        print(f"{'PASS' if r.passed else 'FAIL'}  {r.name:<40} observed={r.observed:g} expected {r.expected}")
    return 0 if all(r.passed for r in results) else 1


def load_named_queries(path: Path = ANALYTICS_SQL) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    parts = re.split(r"^-- name: (\w+)\s*$", text, flags=re.M)
    return {parts[i]: parts[i + 1].strip().rstrip(";") for i in range(1, len(parts), 2)}


def cmd_report(args) -> int:
    from src.warehouse.db import connect

    queries = load_named_queries()
    names = args.query or list(queries)
    with connect(get_settings()) as conn:
        for name in names:
            cur = conn.execute(queries[name])
            columns = [c.name for c in cur.description]
            rows = cur.fetchmany(args.limit)
            print(f"\n== {name} ({len(rows)} rows shown)")
            widths = [max(len(c), *(len(str(r[i])) for r in rows)) if rows else len(c) for i, c in enumerate(columns)]
            print("  ".join(c.ljust(w) for c, w in zip(columns, widths)))
            for row in rows:
                print("  ".join(str(v).ljust(w) for v, w in zip(row, widths)))
    return 0


def cmd_lake_inspect(args) -> int:
    from src.storage.object_store import ObjectStore

    settings = get_settings()
    store = ObjectStore(settings)
    for bucket in (settings.bronze_bucket, settings.silver_bucket, settings.gold_bucket):
        objects = list(store.list_objects(bucket))
        print(f"\n== s3://{bucket}: {len(objects)} objects, {sum(o.size for o in objects):,} bytes")
        for obj in objects[: args.limit]:
            print(f"   {obj.size:>12,}  {obj.key}")
        if len(objects) > args.limit:
            print(f"   ... {len(objects) - args.limit} more")
    pointer = store.current_version(settings.silver_bucket, "sales")
    if pointer:
        sample = store.read_parquet(settings.silver_bucket, pointer["prefix"]).head(args.sample)
        print(f"\n== silver/sales current version {pointer['version']} (sample)")
        print(sample.to_string(index=False))
    return 0


def cmd_benchmark(args) -> int:
    from src.benchmarks.pandas_vs_spark import run_benchmark

    run_benchmark([int(s) for s in args.sizes.split(",")], args.output, args.repeats)
    return 0


def cmd_provision_metabase(args) -> int:
    from src.analytics.metabase_provision import provision

    _print(provision(get_settings()))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.cli", description="Sales data engineering pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init", help="apply SQL migrations, create read-only role and lake buckets")
    p.add_argument("--skip-s3", action="store_true")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("generate", help="generate synthetic sales data")
    p.add_argument("--records", type=int, default=1000, help="clean line items (1k/10k/100k/1M...)")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--dirty-rate", type=float, default=0.0, help="share of rows turned into test cases")
    p.add_argument("--update-rate", type=float, default=0.03, help="share of orders that receive a status update")
    p.add_argument("--start-date", default="2026-01-01")
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--daily", metavar="YYYY-MM-DD", help="generate a single daily batch instead")
    p.add_argument("--format", choices=("csv", "jsonl"), default="csv")
    p.add_argument("--output", type=Path, default=Path("data/generated/sales.csv"))
    p.set_defaults(func=cmd_generate)

    p = sub.add_parser("ingest", help="warehouse-only path: CSV -> raw -> staging -> mart (pandas)")
    p.add_argument("--input", type=Path, default=Path("data/sample/sales_sample.csv"))
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("lake-run", help="full lake pipeline for one or more logical dates (no Airflow)")
    p.add_argument("--date", default=date.today().isoformat())
    p.add_argument("--days", type=int, default=1, help="run consecutive dates starting at --date")
    p.add_argument("--source", choices=("auto", "generate", "file", "api"), default="auto")
    p.add_argument("--run-id", default=None)
    p.set_defaults(func=cmd_lake_run)

    p = sub.add_parser("verify", help="run warehouse data-quality checks")
    p.add_argument("--gold", action="store_true", help="also reconcile gold aggregates")
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("report", help="run analytics queries")
    p.add_argument("--query", action="append", help="query name (repeatable); default: all")
    p.add_argument("--limit", type=int, default=15)
    p.set_defaults(func=cmd_report)

    p = sub.add_parser("lake-inspect", help="list bronze/silver/gold objects and sample silver rows")
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--sample", type=int, default=5)
    p.set_defaults(func=cmd_lake_inspect)

    p = sub.add_parser("benchmark", help="Pandas vs PySpark benchmark")
    p.add_argument("--sizes", default="10000,100000,1000000")
    p.add_argument("--repeats", type=int, default=2)
    p.add_argument("--output", type=Path, default=Path("docs/benchmark_results.md"))
    p.set_defaults(func=cmd_benchmark)

    p = sub.add_parser("provision-metabase", help="create Metabase admin, database connection and dashboards")
    p.set_defaults(func=cmd_provision_metabase)
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging(get_settings().log_level)
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except Exception as exc:  # concise error for CLI users, full trace in logs
        log.exception("command failed")
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
