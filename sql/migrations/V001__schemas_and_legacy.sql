-- V001: schemas + non-destructive move of the Phase-1 starter tables.
-- The starter created raw.sales and mart.{dim_customer,dim_product,dim_date,fact_sales}
-- with a different (single-line transaction) layout. Instead of dropping them, they
-- are moved to the "legacy" schema so existing data stays queryable.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS monitoring;

DO $$
DECLARE
    t RECORD;
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'mart' AND table_name = 'fact_sales' AND column_name = 'revenue'
    ) OR EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = 'sales'
    ) THEN
        CREATE SCHEMA IF NOT EXISTS legacy;
        FOR t IN
            SELECT table_schema, table_name FROM information_schema.tables
            WHERE (table_schema, table_name) IN (
                ('raw', 'sales'), ('mart', 'fact_sales'), ('mart', 'dim_customer'),
                ('mart', 'dim_product'), ('mart', 'dim_date'))
        LOOP
            EXECUTE format('ALTER TABLE %I.%I SET SCHEMA legacy', t.table_schema, t.table_name);
            RAISE NOTICE 'moved %.% to legacy schema', t.table_schema, t.table_name;
        END LOOP;
    END IF;
END $$;
