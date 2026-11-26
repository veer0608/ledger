"""Apply the schema and set up the app role, as the owner.

    python -m ledger.migrate
    python -m ledger.migrate --system-account bank:hdfc INR

DATABASE_URL is the owner. LEDGER_APP_PASSWORD sets ledger_app's password
(otherwise one is generated into .ledger-app-password).

--system-account creates an account that may go negative: a settlement
account or mint. Only the owner can do this; the app role cannot.
"""
from __future__ import annotations

import argparse

import psycopg

from . import core, db


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Apply the ledger schema as the owner role.")
    ap.add_argument("--system-account", nargs=2, action="append", default=[],
                    metavar=("NAME", "CURRENCY"), help="create an allow_negative account (repeatable)")
    ap.add_argument("--shards", type=int, default=1,
                    help="split each --system-account into this many shards, NAME#0..NAME#N-1, "
                         "for an account so busy its row lock is the bottleneck")
    args = ap.parse_args(argv)

    owner = db.database_url()
    db.migrate(owner)
    print("schema applied; ledger_app can log in")
    with psycopg.connect(owner, autocommit=True) as conn:
        for name, currency in args.system_account:
            if args.shards > 1:
                if conn.execute("SELECT 1 FROM accounts WHERE shard_group = %s", (name,)).fetchone():
                    print(f"shard group {name}: already exists")
                else:
                    ids = core.create_shard_group(conn, name, currency, args.shards)
                    print(f"shard group {name}: created {len(ids)} shards, ids {ids[0]}..{ids[-1]}")
                continue
            row = conn.execute(
                """INSERT INTO accounts (name, currency, allow_negative) VALUES (%s, %s, true)
                   ON CONFLICT (name) DO NOTHING RETURNING id""",
                (name, currency),
            ).fetchone()
            print(f"system account {name}: " + (f"created, id {row[0]}" if row else "already exists"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
