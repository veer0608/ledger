"""Throughput of deposits through one busy account, a shard group, and none.

    python -m ledger.bench
    python -m ledger.bench --threads 20 --transfers 3000 --rounds 3 --shards 8

Uses its own database (BENCH_DATABASE_URL, or `ledger_bench` on the bundled
local Postgres) and wipes it between runs. Every transfer runs as the app
role through core.transfer, the same path the API takes. Prints the median of
--rounds runs per scenario.

Numbers depend on the machine and, above all, on the round-trip time to
Postgres: a posting holds its row locks across several round trips, so the
single-account ceiling falls as latency rises.
"""
from __future__ import annotations

import argparse
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg

from . import core, db

USERS = 400


def setup(owner_url: str, shards: int) -> tuple[list[int], list[int]]:
    with psycopg.connect(owner_url, autocommit=True) as owner:
        owner.execute("TRUNCATE entries, transactions, accounts RESTART IDENTITY CASCADE")
        if shards == 1:
            sources = [core.create_account(owner, "bank", "INR", allow_negative=True)["id"]]
        else:
            sources = core.create_shard_group(owner, "bank", "INR", shards)
        users = [core.create_account(owner, f"u{i}", "INR")["id"] for i in range(USERS)]
    return sources, users


def run(pool, plan: list[tuple[str, int, int]], threads: int) -> float:
    chunks = [plan[i::threads] for i in range(threads)]
    barrier = threading.Barrier(threads)

    def worker(chunk):
        with pool.connection() as c:
            barrier.wait()
            for key, src, dst in chunk:
                core.transfer(c, client="bench", key=key, from_account=src, to_account=dst, amount=1)

    start = time.perf_counter()
    with ThreadPoolExecutor(threads) as ex:
        list(ex.map(worker, chunks))
    return len(plan) / (time.perf_counter() - start)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Ledger throughput benchmark.")
    ap.add_argument("--threads", type=int, default=20)
    ap.add_argument("--transfers", type=int, default=3000)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--shards", type=int, default=8)
    args = ap.parse_args(argv)

    owner_url = os.environ.get("BENCH_DATABASE_URL") or db.local_url("ledger_bench")
    db.migrate(owner_url)
    pool = db.make_pool(db.as_app_role(owner_url), max_size=args.threads + 1)
    rng = random.Random(1)
    n = args.transfers

    def single():
        (bank,), users = setup(owner_url, 1)
        return [(f"k{i}", bank, rng.choice(users)) for i in range(n)]

    def sharded():
        _, users = setup(owner_url, args.shards)
        with pool.connection() as c:
            return [(f"k{i}", core.shard_for(c, "bank", client="bench", key=f"k{i}"), rng.choice(users))
                    for i in range(n)]

    def spread():
        # No shared account at all: the ceiling sharding is chasing.
        _, users = setup(owner_url, 1)
        return [(f"k{i}", *rng.sample(users[:USERS // 2], 1), rng.choice(users[USERS // 2:])) for i in range(n)]

    scenarios = [
        ("one settlement account", single),
        (f"settlement group, {args.shards} shards", sharded),
        ("no shared account (user to user)", spread),
    ]
    print(f"{args.threads} threads, {n} transfers per round, median of {args.rounds} rounds")
    try:
        for label, make_plan in scenarios:
            rates = []
            for _ in range(args.rounds):
                plan = make_plan()
                if label.startswith("no shared"):
                    # Fund the senders first, outside the timed run.
                    with pool.connection() as c, psycopg.connect(owner_url, autocommit=True) as owner:
                        mint = core.create_account(owner, "mint", "INR", allow_negative=True)["id"]
                        for i, (src) in enumerate(sorted({p[1] for p in plan})):
                            core.transfer(c, client="bench", key=f"fund{i}", from_account=mint,
                                          to_account=src, amount=10**9)
                rates.append(run(pool, plan, args.threads))
            print(f"  {label:<36} {statistics.median(rates):7.0f} transfers/s"
                  f"   (rounds: {', '.join(f'{r:.0f}' for r in rates)})")
    finally:
        pool.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
