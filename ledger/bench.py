"""Throughput through one busy account, a shard group or receive buffers, and none.

    python -m ledger.bench
    python -m ledger.bench --threads 20 --transfers 3000 --rounds 3 --shards 8
    python -m ledger.bench --rtt-ms 0,0.01,5   # simulate round trips (floor is about 3 ms)
    python -m ledger.bench --only merchant      # just the busy-merchant pair

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
import asyncio
import multiprocessing
import os
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg
from psycopg import conninfo

from . import core, db

USERS = 400


def setup(owner_url: str, shards: int) -> tuple[list[int], list[int]]:
    with psycopg.connect(owner_url, autocommit=True) as owner:
        owner.execute("TRUNCATE ledger_closes, outbox, entries, transactions, accounts RESTART IDENTITY CASCADE")
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


class LatencyProxy:
    """A TCP proxy that delays every chunk by half the given round trip, each way.

    Chunks are timestamped on arrival and released when due, so the delay is
    added per round trip without limiting throughput. It runs in its own
    process so its event loop does not compete with the benchmark's threads
    for the GIL, and it raises the Windows timer resolution to 1 ms (the
    default 15.6 ms turned a requested 1 ms round trip into 31 ms). Even so,
    sleeps round up, which is why main() reports the measured round trip,
    not the requested one.
    """

    def __init__(self, upstream_host: str, upstream_port: int, rtt_ms: float):
        ready = multiprocessing.Queue()
        self.process = multiprocessing.Process(
            target=_serve_proxy, args=(upstream_host, upstream_port, rtt_ms, ready), daemon=True)
        self.process.start()
        self.port = ready.get(timeout=30)

    def url(self, url: str) -> str:
        return conninfo.make_conninfo(url, host="127.0.0.1", port=str(self.port))

    def close(self) -> None:
        self.process.terminate()
        self.process.join()


def _serve_proxy(upstream_host: str, upstream_port: int, rtt_ms: float, ready) -> None:
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.winmm.timeBeginPeriod(1)
    delay = rtt_ms / 2000
    loop = asyncio.new_event_loop()

    async def pipe(reader, writer):
        queue: asyncio.Queue = asyncio.Queue()

        async def receive():
            while data := await reader.read(65536):
                queue.put_nowait((loop.time() + delay, data))
            queue.put_nowait((0, b""))

        async def deliver():
            while True:
                due, data = await queue.get()
                if not data:
                    break
                if (wait := due - loop.time()) > 0:
                    await asyncio.sleep(wait)
                writer.write(data)
                await writer.drain()
            writer.close()

        await asyncio.gather(receive(), deliver(), return_exceptions=True)

    async def handle(client_reader, client_writer):
        up_reader, up_writer = await asyncio.open_connection(upstream_host, upstream_port)
        await asyncio.gather(pipe(client_reader, up_writer), pipe(up_reader, client_writer),
                             return_exceptions=True)

    server = loop.run_until_complete(asyncio.start_server(handle, "127.0.0.1", 0))
    ready.put(server.sockets[0].getsockname()[1])
    loop.run_forever()


def measured_rtt_ms(url: str) -> float:
    with psycopg.connect(url, autocommit=True) as c:
        times = []
        for _ in range(60):
            t = time.perf_counter()
            c.execute("SELECT 1").fetchone()
            times.append((time.perf_counter() - t) * 1000)
    return statistics.median(times[10:])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Ledger throughput benchmark.")
    ap.add_argument("--threads", type=int, default=20)
    ap.add_argument("--transfers", type=int, default=3000)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--shards", type=int, default=8)
    ap.add_argument("--rtt-ms", default="0",
                    help="comma-separated round-trip times to simulate between the app and Postgres, "
                         "e.g. 0,1,2. Setup and funding bypass the delay; only timed transfers pay it.")
    ap.add_argument("--only", choices=["merchant", "settlement"], help="run only one pair of scenarios")
    args = ap.parse_args(argv)
    rtts = [float(x) for x in args.rtt_ms.split(",")]

    owner_url = os.environ.get("BENCH_DATABASE_URL") or db.local_url("ledger_bench")
    db.migrate(owner_url)
    app_url = db.as_app_role(owner_url)
    params = conninfo.conninfo_to_dict(app_url)
    n = args.transfers

    def single(pool, rng):
        (bank,), users = setup(owner_url, 1)
        return [(f"k{i}", bank, rng.choice(users)) for i in range(n)]

    def sharded(pool, rng):
        _, users = setup(owner_url, args.shards)
        with psycopg.connect(owner_url, autocommit=True) as c:
            return [(f"k{i}", core.shard_for(c, "bank", client="bench", key=f"k{i}"), rng.choice(users))
                    for i in range(n)]

    def fund(users):
        with psycopg.connect(owner_url, autocommit=True) as owner:  # outside the timed run
            mint = core.create_account(owner, "mint", "INR", allow_negative=True)["id"]
            for i, u in enumerate(users):
                core.transfer(owner, client="bench", key=f"fund{i}", from_account=mint, to_account=u, amount=10**9)

    def merchant_plain(pool, rng):
        # Customers paying one merchant, which may not go negative, so it cannot be a shard.
        _, users = setup(owner_url, 1)
        fund(users)
        with psycopg.connect(owner_url, autocommit=True) as owner:
            m = core.create_account(owner, "merchant", "INR")["id"]
        return [(f"k{i}", rng.choice(users), m) for i in range(n)]

    def merchant_buffered(pool, rng):
        _, users = setup(owner_url, 1)
        fund(users)
        with psycopg.connect(owner_url, autocommit=True) as owner:
            m = core.create_account(owner, "merchant", "INR")["id"]
            core.create_receive_buffers(owner, m, args.shards)
            return [(f"k{i}", rng.choice(users), core.shard_for(owner, "merchant", client="bench", key=f"k{i}"))
                    for i in range(n)]

    def spread(pool, rng):
        # No shared account at all: the ceiling sharding is chasing.
        _, users = setup(owner_url, 1)
        plan = [(f"k{i}", rng.choice(users[:USERS // 2]), rng.choice(users[USERS // 2:])) for i in range(n)]
        with psycopg.connect(owner_url, autocommit=True) as owner:  # fund senders outside the timed run
            mint = core.create_account(owner, "mint", "INR", allow_negative=True)["id"]
            for i, src in enumerate(sorted({p[1] for p in plan})):
                core.transfer(owner, client="bench", key=f"fund{i}", from_account=mint, to_account=src,
                              amount=10**9)
        return plan

    scenarios = [
        ("one settlement account", single),
        (f"settlement group, {args.shards} shards", sharded),
        ("no shared account (user to user)", spread),
        ("one merchant account", merchant_plain),
        (f"merchant, {args.shards} receive buffers", merchant_buffered),
    ]
    if args.only == "settlement":
        scenarios = scenarios[:2]
    elif args.only == "merchant":
        scenarios = scenarios[3:]
    print(f"{args.threads} threads, {n} transfers per round, median of {args.rounds} rounds")
    for rtt in rtts:
        proxy = LatencyProxy(params.get("host") or "127.0.0.1", int(params.get("port") or 5432), rtt) if rtt else None
        pool = db.make_pool(proxy.url(app_url) if proxy else app_url, max_size=args.threads + 1)
        pool.wait()
        url = proxy.url(app_url) if proxy else app_url
        print(f"round trip: {measured_rtt_ms(url):.2f} ms measured"
              + (f" (proxy asked for {rtt:g} ms)" if rtt else " (direct, no proxy)"))
        try:
            for label, make_plan in scenarios:
                rng = random.Random(1)
                rates = [run(pool, make_plan(pool, rng), args.threads) for _ in range(args.rounds)]
                print(f"  {label:<36} {statistics.median(rates):7.0f} transfers/s"
                      f"   (rounds: {', '.join(f'{r:.0f}' for r in rates)})")
        finally:
            pool.close()
            if proxy:
                proxy.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
