"""Scheduled jobs.

    python -m ledger.jobs expire-holds
    python -m ledger.jobs sweep

Run both from cron or a scheduler, every minute or so. They connect as the
app role.

expire-holds releases pending holds past their expiry, in batches, until none
are left. Safe to run twice at once: each hold is locked and rechecked.

sweep moves every receive buffer's balance into its merchant. Until it runs,
those payments show as the merchant's `unswept` and cannot be spent. Safe to
run twice at once: each buffer is read under its row lock.
"""
from __future__ import annotations

import argparse

from . import core, db


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Ledger scheduled jobs.")
    ap.add_argument("job", choices=["expire-holds", "sweep"])
    ap.add_argument("--batch", type=int, default=1000)
    args = ap.parse_args(argv)

    pool = db.make_pool(db.app_url(), max_size=1)
    try:
        with pool.connection() as conn:
            if args.job == "sweep":
                print(f"swept {core.sweep_all(conn)} buffer(s)")
                return 0
            total = 0
            while (n := core.expire_holds(conn, args.batch)):
                total += n
    finally:
        pool.close()
    print(f"expired {total} hold(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
