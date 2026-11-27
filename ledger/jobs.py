"""Scheduled jobs.

    python -m ledger.jobs expire-holds
    python -m ledger.jobs sweep
    python -m ledger.jobs close                       # through the end of yesterday, IST
    python -m ledger.jobs close --through 2026-11-30T23:59:59.999999+05:30

Run both from cron or a scheduler, every minute or so. They connect as the
app role.

expire-holds releases pending holds past their expiry, in batches, until none
are left. Safe to run twice at once: each hold is locked and rechecked.

sweep moves every receive buffer's balance into its merchant. Until it runs,
those payments show as the merchant's `unswept` and cannot be spent. Safe to
run twice at once: each buffer is read under its row lock.

close closes the books through a past moment: it waits for postings in
flight, then snapshots every account that moved since the last close. Run it
once a day, after midnight.
"""
from __future__ import annotations

import argparse
from datetime import datetime, time, timedelta

from . import core, db, recon


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Ledger scheduled jobs.")
    ap.add_argument("job", choices=["expire-holds", "sweep", "close"])
    ap.add_argument("--through", type=datetime.fromisoformat,
                    help="close: an ISO timestamp with an offset; default is the end of yesterday, IST")
    ap.add_argument("--batch", type=int, default=1000)
    args = ap.parse_args(argv)

    pool = db.make_pool(db.app_url(), max_size=1)
    try:
        with pool.connection() as conn:
            if args.job == "close":
                through = args.through or (datetime.combine(recon.local_today(), time(), recon.LOCAL_TZ)
                                           - timedelta(microseconds=1))
                try:
                    n = core.close_period(conn, through)
                except core.Conflict as exc:  # already closed: a rerun of the daily job is not an error
                    print(f"nothing to close: {exc}")
                    return 0
                print(f"closed the books through {through.isoformat()}; {n} account snapshot(s)")
                return 0
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
