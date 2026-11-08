"""Versioned SQL migrations (``sql/migrations/V###__name.sql``) and role setup.

Each migration runs once, in its own transaction, and is recorded with a checksum
in ``public.schema_migrations``. Editing an applied migration is detected and
reported instead of being silently re-applied.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path

from src.config import PROJECT_ROOT, Settings
from src.utils.logging import get_logger
from src.warehouse.db import acquire_warehouse_lock, connect

log = get_logger(__name__)
MIGRATIONS_DIR = PROJECT_ROOT / "sql" / "migrations"
_NAME = re.compile(r"^V(\d{3})__([a-z0-9_]+)\.sql$")


class MigrationError(RuntimeError):
    pass


def discover(directory: Path = MIGRATIONS_DIR) -> list[tuple[int, str, Path]]:
    found = []
    for path in sorted(directory.glob("V*.sql")):
        match = _NAME.match(path.name)
        if not match:
            raise MigrationError(f"bad migration file name: {path.name}")
        found.append((int(match.group(1)), match.group(2), path))
    versions = [v for v, _, _ in found]
    if len(versions) != len(set(versions)):
        raise MigrationError("duplicate migration version numbers")
    return found


def migrate(settings: Settings) -> list[str]:
    applied_now: list[str] = []
    with connect(settings) as conn:
        with conn.transaction():
            cur = conn.cursor()
            acquire_warehouse_lock(cur)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.schema_migrations (
                    version INTEGER PRIMARY KEY, name TEXT NOT NULL,
                    checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())
            """)
            cur.execute("SELECT version, checksum FROM public.schema_migrations")
            applied = dict(cur.fetchall())
        for version, name, path in discover():
            sql_text = path.read_text(encoding="utf-8")
            checksum = hashlib.sha256(sql_text.encode()).hexdigest()
            if version in applied:
                if applied[version] != checksum:
                    log.warning("migration V%03d (%s) changed after being applied; "
                                "create a new migration instead of editing it", version, name)
                continue
            with conn.transaction():
                cur = conn.cursor()
                acquire_warehouse_lock(cur)
                cur.execute(sql_text)
                cur.execute(
                    "INSERT INTO public.schema_migrations (version, name, checksum) VALUES (%s, %s, %s)",
                    (version, name, checksum),
                )
            log.info("applied migration V%03d__%s", version, name)
            applied_now.append(f"V{version:03d}__{name}")
    if not applied_now:
        log.info("database schema is up to date")
    return applied_now


def ensure_reader_role(settings: Settings) -> bool:
    """Create/refresh the least-privilege read-only role used by Metabase."""
    if not settings.reader_password:
        log.warning("METABASE_DB_PASSWORD not set; skipping read-only role creation")
        return False
    from psycopg import sql

    role = sql.Identifier(settings.reader_user)
    password = sql.Literal(settings.reader_password)
    with connect(settings) as conn, conn.transaction():
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (settings.reader_user,))
        verb = "ALTER" if cur.fetchone() else "CREATE"
        cur.execute(sql.SQL(verb + " ROLE {} WITH LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                            "NOCREATEROLE NOINHERIT").format(role, password))
        cur.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(conn.info.dbname), role))
        for schema in ("mart", "monitoring"):
            ident = sql.Identifier(schema)
            cur.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(ident, role))
            cur.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(ident, role))
            cur.execute(sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}")
                        .format(ident, role))
        # Explicitly no access to raw/staging (PII-heavy, not needed for reporting).
        for schema in ("raw", "staging", "public"):
            cur.execute(sql.SQL("REVOKE ALL ON SCHEMA {} FROM {}").format(sql.Identifier(schema), role))
        cur.execute(sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(role))
    log.info("read-only role %s is configured", settings.reader_user)
    return True
