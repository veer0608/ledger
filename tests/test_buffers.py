"""Receive buffers: a busy merchant's payments land in buffer rows and are swept in."""
from collections import Counter

import pytest
from fastapi.testclient import TestClient
from psycopg import errors

from ledger import core
from ledger.api import create_app
from tests.conftest import CLIENT, balance
from tests.test_concurrency import outcome, run_together


@pytest.fixture
def hotel(conn, owner_conn):
    merchant = core.create_account(conn, "merchant:hotel", "INR")["id"]
    return merchant, core.create_receive_buffers(owner_conn, merchant, 4)


def pay(conn, key, payer, amount, group="merchant:hotel"):
    to = core.shard_for(conn, group, client=CLIENT, key=key)
    return core.transfer(conn, client=CLIENT, key=key, from_account=payer, to_account=to, amount=amount)


# ------------------------------------------------------------------ shape

def test_buffers_are_owner_made_and_shaped_like_the_merchant(conn, hotel):
    merchant, buffers = hotel
    b = core.get_account(conn, buffers[2])
    assert (b["name"], b["shard_group"], b["receives_for"], b["allow_negative"]) == \
        ("merchant:hotel#2", "merchant:hotel", merchant, False)


def test_app_role_cannot_make_buffers(conn):
    m = core.create_account(conn, "merchant:shop", "INR")["id"]
    with pytest.raises(core.Forbidden):
        core.create_receive_buffers(conn, m, 2)
    with pytest.raises(errors.InsufficientPrivilege):
        conn.execute("INSERT INTO accounts (name, currency, shard_group, receives_for) VALUES ('x', 'INR', 'g', %s)",
                     (m,))


def test_a_buffer_must_share_its_merchants_currency(owner_conn, conn):
    m = core.create_account(conn, "merchant:shop", "INR")["id"]
    with pytest.raises(errors.ForeignKeyViolation):
        owner_conn.execute("INSERT INTO accounts (name, currency, shard_group, receives_for) "
                           "VALUES ('merchant:shop#0', 'USD', 'merchant:shop', %s)", (m,))


def test_a_buffer_cannot_be_negative_capable(owner_conn, conn):
    m = core.create_account(conn, "merchant:shop", "INR")["id"]
    with pytest.raises(errors.CheckViolation):
        owner_conn.execute("INSERT INTO accounts (name, currency, shard_group, receives_for, allow_negative) "
                           "VALUES ('merchant:shop#0', 'INR', 'merchant:shop', %s, true)", (m,))


# ------------------------------------------------------------- money in

def test_payments_spread_over_buffers_and_retries_replay(conn, hotel, make_account):
    merchant, buffers = hotel
    alice = make_account("alice", 10_000)
    landed = Counter(core.get_transaction(conn, pay(conn, f"p{i}", alice, 100).transaction_id)["entries"][1]
                     ["account_id"] for i in range(40))
    assert set(landed) == set(buffers)
    again = pay(conn, "p7", alice, 100)
    assert again.replayed
    m = core.get_account(conn, merchant)
    assert (m["balance"], m["unswept"]) == (0, 4_000)
    assert core.group_balance(conn, "merchant:hotel")["balance"] == 4_000


def test_unswept_money_cannot_be_spent_until_swept(conn, hotel, make_account):
    merchant, buffers = hotel
    alice, supplier = make_account("alice", 1_000), make_account("supplier")
    pay(conn, "p1", alice, 600)
    with pytest.raises(core.InsufficientFunds):
        core.transfer(conn, client=CLIENT, key="payout", from_account=merchant, to_account=supplier, amount=500)
    assert core.sweep_all(conn, merchant) == 1
    core.transfer(conn, client=CLIENT, key="payout", from_account=merchant, to_account=supplier, amount=500)
    m = core.get_account(conn, merchant)
    assert (m["balance"], m["unswept"]) == (100, 0)
    assert core.check_invariants(conn)["ok"]


# ------------------------------------------------------------ money out

def test_a_buffer_cannot_pay_anyone(conn, hotel, make_account):
    merchant, buffers = hotel
    alice, mallory = make_account("alice", 1_000), make_account("mallory")
    pay(conn, "p1", alice, 600)
    full = next(b for b in buffers if balance(conn, b))
    with pytest.raises(core.ReceiveOnly):
        core.transfer(conn, client=CLIENT, key="steal", from_account=full, to_account=mallory, amount=100)
    # Not even to its own merchant by a normal transfer of part of it: only sweep() moves it.
    assert balance(conn, mallory) == 0


def test_the_database_refuses_a_buffer_debit_the_app_did_not_check(conn, hotel, make_account):
    # Raw SQL as the app role, skipping core.transfer entirely.
    merchant, buffers = hotel
    alice, mallory = make_account("alice", 1_000), make_account("mallory")
    pay(conn, "p1", alice, 600)
    full = next(b for b in buffers if balance(conn, b))
    with pytest.raises(errors.CheckViolation, match="receive buffer"):
        with conn.transaction():
            t = conn.execute("INSERT INTO transactions (client_id, idempotency_key, request_hash, currency) "
                             "VALUES ('x', 'raw', 'h', 'INR') RETURNING id").fetchone()[0]
            conn.execute("INSERT INTO entries (transaction_id, account_id, currency, amount) VALUES "
                         "(%s, %s, 'INR', -100), (%s, %s, 'INR', 100)", (t, full, t, mallory))
    assert balance(conn, full) == 600


def test_a_sweep_that_also_pays_someone_else_is_refused(conn, hotel, make_account):
    merchant, buffers = hotel
    alice, mallory = make_account("alice", 1_000), make_account("mallory")
    pay(conn, "p1", alice, 600)
    full = next(b for b in buffers if balance(conn, b))
    with pytest.raises(errors.CheckViolation, match="receive buffer"):
        with conn.transaction():
            t = conn.execute("INSERT INTO transactions (client_id, idempotency_key, request_hash, currency) "
                             "VALUES ('x', 'raw', 'h', 'INR') RETURNING id").fetchone()[0]
            conn.execute("INSERT INTO entries (transaction_id, account_id, currency, amount) VALUES "
                         "(%s, %s, 'INR', -600), (%s, %s, 'INR', 500), (%s, %s, 'INR', 100)",
                         (t, full, t, merchant, t, mallory))


def test_a_payment_into_a_buffer_is_refunded_from_the_merchant_not_reversed(conn, hotel, make_account):
    merchant, buffers = hotel
    alice = make_account("alice", 1_000)
    p = pay(conn, "p1", alice, 600)
    with pytest.raises(core.ReceiveOnly):
        core.reverse(conn, client=CLIENT, key="r1", transaction_id=p.transaction_id)
    core.sweep_all(conn)
    core.transfer(conn, client=CLIENT, key="refund-p1", from_account=merchant, to_account=alice, amount=600)
    assert balance(conn, alice) == 1_000


def test_a_buffer_cannot_hold_money(conn, hotel, make_account):
    merchant, buffers = hotel
    alice, shop = make_account("alice", 1_000), make_account("shop")
    pay(conn, "p1", alice, 600)
    full = next(b for b in buffers if balance(conn, b))
    with pytest.raises(core.ReceiveOnly):
        core.authorize(conn, client=CLIENT, key="h1", account=full, payee=shop, amount=100)


# ---------------------------------------------------------------- sweep

def test_sweep_moves_everything_and_an_empty_buffer_is_a_no_op(conn, hotel, make_account):
    merchant, buffers = hotel
    alice = make_account("alice", 10_000)
    for i in range(20):
        pay(conn, f"p{i}", alice, 50)
    nonempty = sum(1 for b in buffers if balance(conn, b))
    assert core.sweep_all(conn) == nonempty
    assert all(balance(conn, b) == 0 for b in buffers)
    assert balance(conn, merchant) == 1_000
    assert core.sweep(conn, buffers[0]) is None
    assert core.sweep_all(conn) == 0
    assert core.check_invariants(conn)["ok"]


def test_sweeping_a_non_buffer_is_refused(conn, make_account):
    with pytest.raises(core.LedgerError, match="not a receive buffer"):
        core.sweep(conn, make_account("alice"))


def test_payments_and_sweeps_racing(pool, conn, hotel, make_account):
    # 40 payers and 15 sweepers at once (the pool holds 60): nothing lost, nothing doubled, no buffer below zero.
    merchant, buffers = hotel
    payers = [make_account(f"u{i}", 1_000) for i in range(40)]

    def work(c, i):
        if i < 40:
            return pay(c, f"p{i}", payers[i], 7)
        return core.sweep_all(c)

    results = run_together(pool, 55, work)
    assert Counter(map(outcome, results)) == {"ok": 55}
    core.sweep_all(conn)
    assert balance(conn, merchant) == 40 * 7
    assert all(balance(conn, b) == 0 for b in buffers)
    assert core.check_invariants(conn)["ok"]


def test_concurrent_sweeps_of_one_buffer_move_it_once(pool, conn, hotel, make_account):
    merchant, buffers = hotel
    alice = make_account("alice", 10_000)
    for i in range(30):
        pay(conn, f"p{i}", alice, 10)
    target = max(buffers, key=lambda b: balance(conn, b))
    expected = balance(conn, target)
    results = run_together(pool, 20, lambda c, i: core.sweep(c, target))
    assert Counter(map(outcome, results)) == {"ok": 20}
    assert sum(1 for r in results if r is not None) == 1
    assert balance(conn, merchant) == expected
    assert core.check_invariants(conn)["ok"]


# ------------------------------------------------------------------ API

def test_api_pays_a_merchant_group_and_shows_unswept(pool, hotel, make_account):
    merchant, buffers = hotel
    alice = make_account("alice", 1_000)
    with TestClient(create_app(pool)) as client:
        hd = {"Idempotency-Key": "checkout-1", "Client-Id": "shop"}
        r = client.post("/transfers", headers=hd, json={"from_account": alice, "to_group": "merchant:hotel",
                                                         "amount": 250})
        assert r.status_code == 201
        assert r.json()["entries"][1]["account_id"] in buffers
        m = client.get(f"/accounts/{merchant}").json()
        assert (m["balance"], m["unswept"]) == (0, 250)
        bad = client.post("/transfers", headers={"Idempotency-Key": "x", "Client-Id": "shop"},
                          json={"from_group": "merchant:hotel", "to_account": alice, "amount": 1})
        assert (bad.status_code, bad.json()["error"]) == (422, "receive_buffer")
