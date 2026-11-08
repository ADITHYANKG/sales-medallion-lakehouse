# Phase-1 starter SQL (superseded)

These are the original starter scripts, kept for reference only. They are no
longer executed. The current schema is created by the versioned migrations in
`sql/migrations/` (`python -m src.cli init`); migration `V001` moves any existing
Phase-1 tables (`raw.sales`, `mart.fact_sales`, `mart.dim_*`) into the `legacy`
schema instead of dropping them.
