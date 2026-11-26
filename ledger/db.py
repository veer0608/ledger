"""Connections, roles and schema.

Two database roles (see schema.sql):

- the owner, from DATABASE_URL, runs migrations;
- `ledger_app`, which the service connects as, can only read and insert.

Production: run `python -m ledger.migrate` as the owner, then give the
service APP_DATABASE_URL for ledger_app.

Local: with no DATABASE_URL, a bundled Postgres from the `pgserver` package
is started under .pgdata/, so the project runs with no Postgres installed and
no Docker. app_url() then migrates and connects as ledger_app for you.
"""
from __future__ import annotations

import os
import secrets
from pathlib import Path

import psycopg
from psycopg import conninfo, sql
from psycopg_pool import ConnectionPool

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")
APP_ROLE = "ledger_app"
PASSWORD_FILE = ROOT / ".ledger-app-password"


def local_url(dbname: str) -> str:
    """Owner URI for `dbname` on the bundled local Postgres, creating the database if needed."""
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
    """Owner URI."""
    return os.environ.get("DATABASE_URL") or local_url("ledger")


def app_password() -> str:
    """LEDGER_APP_PASSWORD, else a random one generated once and kept in a gitignored file.

    The role is cluster-wide, so every process sharing a server must agree on
    it; a fresh random password per process would lock the others out.
    """
    pw = os.environ.get("LEDGER_APP_PASSWORD")
    if pw:
        return pw
    if PASSWORD_FILE.exists():
        return PASSWORD_FILE.read_text(encoding="utf-8").strip()
    pw = secrets.token_urlsafe(24)
    PASSWORD_FILE.write_text(pw, encoding="utf-8")
    return pw


def apply_schema(conn: psycopg.Connection) -> None:
    with conn.transaction():
        # Two processes starting at once would otherwise race on CREATE TRIGGER.
        conn.execute("SELECT pg_advisory_xact_lock(724501)")
        conn.execute(SCHEMA)


def migrate(owner_url: str) -> None:
    with psycopg.connect(owner_url, autocommit=True) as conn:
        apply_schema(conn)
        conn.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(APP_ROLE), sql.Literal(app_password())))


def as_app_role(owner_url: str) -> str:
    return conninfo.make_conninfo(owner_url, user=APP_ROLE, password=app_password())


def app_url() -> str:
    """URI the service should use: APP_DATABASE_URL, or (local dev) migrate and derive it."""
    url = os.environ.get("APP_DATABASE_URL")
    if url:
        return url
    owner = database_url()
    migrate(owner)
    return as_app_role(owner)


def make_pool(url: str, max_size: int = 20) -> ConnectionPool:
    # autocommit, so each `with conn.transaction()` in core.py is a real
    # top-level transaction rather than a savepoint inside an implicit one.
    return ConnectionPool(url, min_size=1, max_size=max_size, kwargs={"autocommit": True}, open=True)
