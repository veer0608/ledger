"""Scheduled jobs.

    python -m ledger.jobs expire-holds

Run expire-holds from cron or a scheduler, every minute or so. It connects as
the app role and releases pending holds past their expiry, in batches, until
none are left. Safe to run twice at once: each hold is locked and rechecked.
"""
from __future__ import annotations

import argparse

from . import core, db


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Ledger scheduled jobs.")
    ap.add_argument("job", choices=["expire-holds"])
    ap.add_argument("--batch", type=int, default=1000)
    args = ap.parse_args(argv)

    pool = db.make_pool(db.app_url(), max_size=1)
    try:
        with pool.connection() as conn:
            total = 0
            while (n := core.expire_holds(conn, args.batch)):
                total += n
    finally:
        pool.close()
    print(f"expired {total} hold(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
