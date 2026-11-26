"""Races. Every test starts N threads behind a barrier so they hit Postgres together.

Each thread uses its own pooled connection, so these are real concurrent
database sessions, not interleavings simulated in Python.
"""
import random
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from ledger import core
from tests.conftest import balance


def run_together(pool, n, fn):
    """Call fn(conn, i) for i in range(n), all released at once. Returns results or exceptions."""
    barrier = threading.Barrier(n)

    def worker(i):
        with pool.connection() as conn:
            barrier.wait()
            try:
                return fn(conn, i)
            except Exception as exc:  # collected and asserted on by the caller
                return exc

    with ThreadPoolExecutor(max_workers=n) as ex:
        return list(ex.map(worker, range(n)))


def outcome(r):
    return type(r).__name__ if isinstance(r, Exception) else "ok"


def test_overdraft_race_cannot_go_negative(pool, conn, make_account):
    # 1,000 paise, 50 simultaneous withdrawals of 30: exactly 33 fit.
    a = make_account("alice", 1_000)
    b = make_account("bob")
    results = run_together(pool, 50, lambda c, i: core.transfer(
        c, key=f"w{i}", from_account=a, to_account=b, amount=30))
    counts = Counter(outcome(r) for r in results)
    assert counts == {"ok": 33, "InsufficientFunds": 17}, counts
    assert balance(conn, a) == 10
    assert balance(conn, b) == 990
    assert core.check_invariants(conn)["ok"]


def test_same_key_storm_moves_money_once(pool, conn, make_account):
    # A client retrying aggressively: 30 copies of one request at the same instant.
    a = make_account("alice", 1_000)
    b = make_account("bob")
    results = run_together(pool, 30, lambda c, i: core.transfer(
        c, key="pay-123", from_account=a, to_account=b, amount=700))
    assert all(isinstance(r, core.Posted) for r in results), Counter(map(outcome, results))
    assert len({r.transaction_id for r in results}) == 1
    assert sum(not r.replayed for r in results) == 1
    assert balance(conn, b) == 700
    assert conn.execute("SELECT count(*) FROM transactions WHERE idempotency_key = 'pay-123'").fetchone()[0] == 1


def test_opposing_transfers_do_not_deadlock(pool, conn, make_account):
    # Half go A->B, half B->A. Without ordered locking this deadlocks.
    a = make_account("alice", 10_000)
    b = make_account("bob", 10_000)

    def move(c, i):
        src, dst = (a, b) if i % 2 else (b, a)
        return core.transfer(c, key=f"x{i}", from_account=src, to_account=dst, amount=7)

    results = run_together(pool, 50, move)
    assert Counter(map(outcome, results)) == {"ok": 50}
    assert balance(conn, a) == 10_000
    assert balance(conn, b) == 10_000


def test_double_reversal_race(pool, conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    t = core.transfer(conn, key="t1", from_account=a, to_account=b, amount=500)
    results = run_together(pool, 20, lambda c, i: core.reverse(c, key=f"r{i}", transaction_id=t.transaction_id))
    assert Counter(map(outcome, results)) == {"ok": 1, "AlreadyReversed": 19}
    assert balance(conn, a) == 1_000
    assert balance(conn, b) == 0


def test_random_stress_conserves_money(pool, conn, make_account):
    names = [f"u{i}" for i in range(6)]
    ids = [make_account(n, 5_000) for n in names]
    total_before = sum(balance(conn, i) for i in ids)
    rng = random.Random(7)
    plan = []
    for k in range(400):
        src, dst = rng.sample(ids, 2)
        plan.append((f"s{k}", src, dst, rng.randint(1, 3_000)))

    def go(c, i):
        out = []
        for key, src, dst, amt in plan[i::40]:
            try:
                out.append(core.transfer(c, key=key, from_account=src, to_account=dst, amount=amt))
            except core.InsufficientFunds as exc:
                out.append(exc)
        return out

    results = run_together(pool, 40, go)
    assert not any(isinstance(r, Exception) for r in results), results
    flat = [x for r in results for x in r]
    assert len(flat) == 400
    assert any(isinstance(x, core.Posted) for x in flat)
    assert sum(balance(conn, i) for i in ids) == total_before
    assert all(balance(conn, i) >= 0 for i in ids)
    assert core.check_invariants(conn)["ok"]
