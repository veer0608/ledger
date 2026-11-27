"""Closing the books: snapshots, balances at a past moment, and the in-flight posting race."""
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from psycopg import errors

from ledger import core
from ledger.api import create_app
from tests.conftest import CLIENT, balance


def db_now(conn):
    return conn.execute("SELECT clock_timestamp()").fetchone()[0]


def pay(conn, key, src, dst, amount):
    return core.transfer(conn, client=CLIENT, key=key, from_account=src, to_account=dst, amount=amount)


@pytest.fixture
def pair(make_account):
    return make_account("alice", 1_000), make_account("bob")


# ----------------------------------------------------------- balances then

def test_a_closed_balance_is_final_and_a_later_one_is_not(conn, pair):
    alice, bob = pair
    pay(conn, "p1", alice, bob, 300)
    t1 = db_now(conn)
    pay(conn, "p2", alice, bob, 200)
    t2 = db_now(conn)
    assert core.close_period(conn, t1) == 3  # mint, alice, bob moved
    assert core.balance_at(conn, alice, t1) | {"at": None} == \
        {"id": alice, "name": "alice", "currency": "INR", "balance": 700, "at": None, "final": True}
    later = core.balance_at(conn, alice, t2)
    assert (later["balance"], later["final"]) == (500, False)
    assert core.balance_at(conn, alice, t1 - timedelta(days=1))["balance"] == 0


def test_closes_are_incremental_and_balances_between_them_are_exact(conn, pair):
    alice, bob = pair
    marks = []
    for i in range(4):
        pay(conn, f"p{i}", alice, bob, 100)
        marks.append(db_now(conn))
    core.close_period(conn, marks[1])
    assert core.close_period(conn, marks[3]) == 2  # only alice and bob moved since; the mint did not
    assert [core.balance_at(conn, bob, m)["balance"] for m in marks] == [100, 200, 300, 400]
    assert all(core.balance_at(conn, bob, m)["final"] for m in marks)
    assert core.check_invariants(conn)["ok"]


def test_trial_balance_sums_to_zero_at_a_past_moment(conn, pair, make_account):
    alice, bob = pair
    pay(conn, "p1", alice, bob, 300)
    t = db_now(conn)
    make_account("carol", 5_000)  # after t: not in the trial balance at t
    core.close_period(conn, t)
    tb = core.trial_balance(conn, t)
    assert tb["balanced"] and tb["final"] and tb["totals"] == {"INR": 0}
    assert {r["name"]: r["balance"] for r in tb["accounts"]} == {"mint:INR": -1_000, "alice": 700, "bob": 300}


# --------------------------------------------------------------- the rules

def test_cannot_close_the_future_or_close_twice(conn, pair):
    pay(conn, "p1", *pair, 10)
    with pytest.raises(core.Conflict, match="not over yet"):
        core.close_period(conn, db_now(conn) + timedelta(hours=1))
    t = db_now(conn)
    core.close_period(conn, t)
    with pytest.raises(core.Conflict, match="already closed"):
        core.close_period(conn, t)
    with pytest.raises(core.Conflict, match="already closed"):
        core.close_period(conn, t - timedelta(seconds=1))


def test_a_close_must_run_read_committed(conn, pair):
    pay(conn, "p1", *pair, 10)
    with pytest.raises(errors.InvalidTransactionState):
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            conn.execute("SELECT ledger_close_period(clock_timestamp() - interval '1 second')")


APP_REFUSED = {
    "write a snapshot": "INSERT INTO balance_snapshots (account_id, as_of, balance) VALUES (1, now(), 999)",
    "record a close": "INSERT INTO ledger_closes (through) VALUES (now())",
    "rewrite a snapshot": "UPDATE balance_snapshots SET balance = 999",
    "reopen the books": "DELETE FROM ledger_closes",
}


@pytest.mark.parametrize("sql", APP_REFUSED.values(), ids=APP_REFUSED.keys())
def test_app_role_cannot_forge_or_undo_a_close(conn, pair, sql):
    pay(conn, "p1", *pair, 10)
    core.close_period(conn, db_now(conn))
    with pytest.raises(errors.InsufficientPrivilege):
        conn.execute(sql)


def test_even_the_owner_cannot_rewrite_a_snapshot_or_reopen(owner_conn, conn, pair):
    pay(conn, "p1", *pair, 10)
    core.close_period(conn, db_now(conn))
    with pytest.raises(errors.RestrictViolation):
        owner_conn.execute("UPDATE balance_snapshots SET balance = 999")
    with pytest.raises(errors.RestrictViolation):
        owner_conn.execute("DELETE FROM ledger_closes")


def test_the_audit_catches_a_snapshot_that_disagrees(owner_conn, conn, pair):
    alice, bob = pair
    pay(conn, "p1", alice, bob, 10)
    core.close_period(conn, db_now(conn))
    with owner_conn.transaction():
        owner_conn.execute("SET LOCAL session_replication_role = replica")
        owner_conn.execute("UPDATE balance_snapshots SET balance = balance + 1 WHERE account_id = %s", (bob,))
    report = core.check_invariants(conn)
    assert not report["ok"] and [r["account_id"] for r in report["snapshot_drift"]] == [bob]


# -------------------------------------------------- the in-flight posting

def test_a_close_waits_for_a_posting_already_in_flight(pool, conn, pair):
    # A posting starts, so its entries are stamped with that start time, and
    # has not committed when the close runs. Without the wait, the snapshot
    # would miss it and the posting would then land inside a closed period.
    alice, bob = pair
    closed = {}
    with pool.connection() as a:
        with a.transaction():
            pay(a, "in-flight", alice, bob, 400)
            time.sleep(0.05)
            through = db_now(conn)  # after the posting started, before it commits

            def close():
                with pool.connection() as b:
                    closed["n"] = core.close_period(b, through)

            closer = threading.Thread(target=close)
            closer.start()
            closer.join(timeout=0.8)
            assert closer.is_alive(), "the close did not wait for the posting in flight"
        closer.join(timeout=10)
    assert closed["n"] == 3
    assert core.balance_at(conn, bob, through) == core.balance_at(conn, bob, through) | {"balance": 400}
    assert core.check_invariants(conn)["ok"]


def test_a_posting_that_started_before_a_close_is_refused_and_a_retry_succeeds(pool, conn, pair):
    alice, bob = pair
    with pool.connection() as a:
        with pytest.raises(core.PeriodClosed, match="retry"):
            with a.transaction():
                a.execute("SELECT 1")  # the database transaction, and so its timestamp, starts here
                time.sleep(0.05)
                core.close_period(conn, db_now(conn))
                pay(a, "late", alice, bob, 400)
        assert pay(a, "late", alice, bob, 400).replayed is False  # same key, fresh transaction
    assert balance(conn, bob) == 400
    assert core.check_invariants(conn)["ok"]


def test_postings_and_a_close_racing(pool, conn, make_account):
    from tests.test_concurrency import outcome, run_together
    src = make_account("src", 100_000)
    users = [make_account(f"u{i}") for i in range(10)]
    through = {}

    def work(c, i):
        if i == 20:
            time.sleep(0.01)
            through["t"] = db_now(c)
            return core.close_period(c, through["t"])
        return pay(c, f"k{i}", src, users[i % 10], 3)

    results = run_together(pool, 21, work)
    refused = [i for i, r in enumerate(results) if isinstance(r, core.PeriodClosed)]
    assert {outcome(r) for i, r in enumerate(results) if i not in refused} == {"ok"}
    for i in refused:  # a client retries with the same key
        pay(conn, f"k{i}", src, users[i % 10], 3)
    assert core.check_invariants(conn)["ok"]  # includes: every snapshot equals its entries


# --------------------------------------------------------------- statement

def test_statement_reads_only_what_it_returns_and_still_adds_up(conn, pair):
    alice, bob = pair
    for i in range(30):
        pay(conn, f"p{i}", alice, bob, 1 + i)
    st = core.statement(conn, bob, limit=5)
    after = [e["balance_after"] for e in st["entries"]]
    assert after[0] == balance(conn, bob) == sum(range(1, 31))
    assert all(after[i] - after[i + 1] == st["entries"][i]["amount"] for i in range(4))


# --------------------------------------------------------------------- API

def test_api_balance_at_and_trial_balance(pool, conn, pair):
    alice, bob = pair
    pay(conn, "p1", alice, bob, 250)
    t = db_now(conn)
    core.close_period(conn, t)
    with TestClient(create_app(pool)) as client:
        r = client.get(f"/accounts/{bob}/balance", params={"at": t.isoformat()})
        assert (r.status_code, r.json()["balance"], r.json()["final"]) == (200, 250, True)
        tb = client.get("/trial-balance", params={"at": t.isoformat()}).json()
        assert tb["balanced"] and tb["totals"] == {"INR": 0}
        naive = client.get(f"/accounts/{bob}/balance", params={"at": "2026-11-27T10:00:00"})
        assert naive.status_code == 422
        assert client.get("/periods").json()["closed_through"] is not None
