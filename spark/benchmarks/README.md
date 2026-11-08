# Pandas vs PySpark benchmark

The benchmark implementation lives in [`src/benchmarks/pandas_vs_spark.py`](../../src/benchmarks/pandas_vs_spark.py)
so it can share the exact transformation code used by the pipeline.

```bash
docker compose run --rm app python -m src.cli benchmark --sizes 10000,100000,1000000 --repeats 3
```

Results (measured, with environment details and limitations) are written to
[`docs/benchmark_results.md`](../../docs/benchmark_results.md) and `docs/benchmark_results.json`.
