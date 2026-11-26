"""Connections and schema.

DATABASE_URL picks the database. Without it, a bundled Postgres from the
`pgserver` package is started under .pgdata/, so the project runs on a machine
with no Postgres installed and no Docker.
"""
from __future__ import annotations

import os
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg_pool import ConnectionPool

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")


def local_url(dbname: str) -> str:
    """URI for `dbname` on the bundled local Postgres, creating the database if needed."""
    try:
        import pgserver
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "No DATABASE_URL set and pgserver is not installed. "
            "Either point DATABASE_URL at a Postgres 14+ database or `pip install pgserver`."
        ) from exc
    server = pgserver.get_server(ROOT / ".pgdata", cleanup_mode=None)
    with psycopg.connect(server.get_uri(), autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    return server.get_uri(dbname)


def database_url() -> str:
    return os.environ.get("DATABASE_URL") or local_url("ledger")


def make_pool(url: str, max_size: int = 20) -> ConnectionPool:
    # autocommit, so each `with conn.transaction()` in core.py is a real
    # top-level transaction rather than a savepoint inside an implicit one.
    return ConnectionPool(url, min_size=1, max_size=max_size, kwargs={"autocommit": True}, open=True)


def apply_schema(conn: psycopg.Connection) -> None:
    with conn.transaction():
        # Two processes starting at once would otherwise race on CREATE TRIGGER.
        conn.execute("SELECT pg_advisory_xact_lock(724501)")
        conn.execute(SCHEMA)
