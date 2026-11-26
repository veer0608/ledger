"""Holds: authorise now, capture or void later."""
import time
from collections import Counter
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from psycopg import errors

from ledger import core
from ledger.api import create_app
from tests.conftest import CLIENT, balance
from tests.test_concurrency import outcome, run_together


def acct(conn, account_id):
    a = core.get_account(conn, account_id)
    return a["balance"], a["held"], a["available"]


def hold(conn, key, account, payee, amount, **kw):
    return core.authorize(conn, client=CLIENT, key=key, account=account, payee=payee, amount=amount, **kw)


@pytest.fixture
def shop(make_account):
    """Alice with 1,000 paise and a merchant."""
    return make_account("alice", 1_000), make_account("merchant")


# --------------------------------------------------------------- behaviour

def test_hold_reserves_without_moving(conn, shop):
    alice, merchant = shop
    hold(conn, "h1", alice, merchant, 300)
    assert acct(conn, alice) == (1_000, 300, 700)
    assert balance(conn, merchant) == 0
    assert core.check_invariants(conn)["ok"]


def test_held_money_cannot_be_spent(conn, shop, make_account):
    alice, merchant = shop
    bob = make_account("bob")
    hold(conn, "h1", alice, merchant, 800)
    with pytest.raises(core.InsufficientFunds, match="200 available"):
        core.transfer(conn, client=CLIENT, key="t1", from_account=alice, to_account=bob, amount=201)
    core.transfer(conn, client=CLIENT, key="t2", from_account=alice, to_account=bob, amount=200)
    assert acct(conn, alice) == (800, 800, 0)


def test_hold_needs_available_funds(conn, shop):
    alice, merchant = shop
    hold(conn, "h1", alice, merchant, 600)
    with pytest.raises(core.InsufficientFunds):
        hold(conn, "h2", alice, merchant, 401)


def test_full_capture(conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 300)
    p = core.capture(conn, client=CLIENT, key="c1", hold_id=h.hold_id)
    assert acct(conn, alice) == (700, 0, 700)
    assert balance(conn, merchant) == 300
    got = core.get_hold(conn, h.hold_id)
    assert (got["status"], got["captured_amount"], got["capture_transaction_id"]) == ("captured", 300, p.transaction_id)
    assert core.check_invariants(conn)["ok"]


def test_capture_can_use_the_whole_balance(conn, shop):
    # Release-then-debit ordering: a hold on every paisa must still capture.
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 1_000)
    core.capture(conn, client=CLIENT, key="c1", hold_id=h.hold_id)
    assert acct(conn, alice) == (0, 0, 0)


def test_partial_capture_releases_the_rest(conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 500)
    core.capture(conn, client=CLIENT, key="c1", hold_id=h.hold_id, amount=450)
    assert acct(conn, alice) == (550, 0, 550)
    assert balance(conn, merchant) == 450


def test_cannot_capture_more_than_held(conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 500)
    with pytest.raises(core.LedgerError, match="cannot capture 501"):
        core.capture(conn, client=CLIENT, key="c1", hold_id=h.hold_id, amount=501)
    assert core.get_hold(conn, h.hold_id)["status"] == "pending"


def test_capture_happens_once(conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 300)
    first = core.capture(conn, client=CLIENT, key="c1", hold_id=h.hold_id)
    assert core.capture(conn, client=CLIENT, key="c1", hold_id=h.hold_id) == core.Posted(first.transaction_id, True)
    with pytest.raises(core.HoldClosed, match="already captured"):
        core.capture(conn, client=CLIENT, key="c2", hold_id=h.hold_id)
    assert balance(conn, merchant) == 300


def test_void_releases_and_is_idempotent(conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 300)
    assert core.void(conn, hold_id=h.hold_id)["status"] == "voided"
    assert core.void(conn, hold_id=h.hold_id)["status"] == "voided"
    assert acct(conn, alice) == (1_000, 0, 1_000)
    with pytest.raises(core.HoldClosed, match="already voided"):
        core.capture(conn, client=CLIENT, key="c1", hold_id=h.hold_id)


def test_cannot_void_a_captured_hold(conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 300)
    core.capture(conn, client=CLIENT, key="c1", hold_id=h.hold_id)
    with pytest.raises(core.HoldClosed, match="already captured"):
        core.void(conn, hold_id=h.hold_id)


def test_refund_after_capture_is_a_reversal(conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 300)
    p = core.capture(conn, client=CLIENT, key="c1", hold_id=h.hold_id)
    core.reverse(conn, client=CLIENT, key="refund-1", transaction_id=p.transaction_id)
    assert acct(conn, alice) == (1_000, 0, 1_000)
    assert core.get_hold(conn, h.hold_id)["status"] == "captured"  # the hold's history stands


def test_authorize_replay_and_conflict(conn, shop):
    alice, merchant = shop
    first = hold(conn, "h1", alice, merchant, 300)
    assert hold(conn, "h1", alice, merchant, 300) == core.Authorized(first.hold_id, True)
    with pytest.raises(core.IdempotencyConflict):
        hold(conn, "h1", alice, merchant, 301)
    assert acct(conn, alice)[1] == 300


def test_expired_hold_cannot_be_captured_and_is_released_by_the_job(conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 300, expires_in=timedelta(milliseconds=50))
    time.sleep(0.2)
    assert core.get_hold(conn, h.hold_id)["lapsed"] is True
    assert acct(conn, alice)[1] == 300  # still reserved until the job runs: the safe direction
    with pytest.raises(core.HoldClosed, match="expired"):
        core.capture(conn, client=CLIENT, key="c1", hold_id=h.hold_id)
    assert core.expire_holds(conn) == 1
    assert core.expire_holds(conn) == 0
    assert core.get_hold(conn, h.hold_id)["status"] == "expired"
    assert acct(conn, alice) == (1_000, 0, 1_000)


def test_live_hold_is_not_expired(conn, shop):
    alice, merchant = shop
    hold(conn, "h1", alice, merchant, 300)
    assert core.expire_holds(conn) == 0


@pytest.mark.parametrize("kw,match", [
    ({"amount": 0}, "amount"), ({"payee": "self"}, "itself"),
    ({"expires_in": timedelta(0)}, "expires_in"), ({"expires_in": timedelta(days=31)}, "expires_in"),
])
def test_bad_authorizations(conn, shop, kw, match):
    alice, merchant = shop
    args = {"account": alice, "payee": merchant, "amount": 100}
    args.update(kw)
    if args["payee"] == "self":
        args["payee"] = alice
    with pytest.raises(core.LedgerError, match=match):
        hold(conn, "h1", **args)


def test_hold_currency_mismatch(conn, make_account):
    alice = make_account("alice", 1_000)
    usd = make_account("merchant-usd", currency="USD")
    with pytest.raises(core.CurrencyMismatch):
        hold(conn, "h1", alice, usd, 100)


# ------------------------------------------------- raw SQL as the app role

def _raw_hold(conn, account, payee, amount=100, key="raw"):
    return conn.execute(
        """INSERT INTO holds (client_id, idempotency_key, request_hash, account_id, payee_id, currency,
                              amount, expires_at)
           VALUES ('raw', %s, 'x', %s, %s, 'INR', %s, now() + interval '1 day') RETURNING id""",
        (key, account, payee, amount),
    ).fetchone()[0]


def test_raw_hold_beyond_available_is_refused(conn, shop):
    alice, merchant = shop
    with pytest.raises(errors.CheckViolation):
        _raw_hold(conn, alice, merchant, amount=1_001)


def test_capture_without_paying_the_payee_is_refused(conn, shop, make_account):
    # Mark a hold captured against an unrelated transaction: the reservation
    # would be released and the merchant never paid.
    alice, merchant = shop
    bob = make_account("bob")
    h = _raw_hold(conn, alice, merchant, amount=300)
    other = core.transfer(conn, client=CLIENT, key="t1", from_account=alice, to_account=bob, amount=300)
    with pytest.raises(errors.CheckViolation, match="does not move"):
        with conn.transaction():
            conn.execute("""UPDATE holds SET status = 'captured', captured_amount = 300,
                                             capture_transaction_id = %s, resolved_at = now()
                            WHERE id = %s""", (other.transaction_id, h))
    assert core.get_hold(conn, h)["status"] == "pending"


def test_resolved_hold_cannot_be_reopened(conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 300)
    core.void(conn, hold_id=h.hold_id)
    with pytest.raises(errors.RestrictViolation, match="already voided"):
        conn.execute("UPDATE holds SET status = 'pending', resolved_at = NULL WHERE id = %s", (h.hold_id,))
    assert acct(conn, alice)[1] == 0


def test_live_hold_cannot_be_marked_expired(conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 300)
    with pytest.raises(errors.CheckViolation, match="does not expire until"):
        conn.execute("UPDATE holds SET status = 'expired', resolved_at = now() WHERE id = %s", (h.hold_id,))


APP_ROLE_REFUSED = {
    "shrink a hold": "UPDATE holds SET amount = 1",
    "extend a hold": "UPDATE holds SET expires_at = now() + interval '1 year'",
    "redirect a hold": "UPDATE holds SET payee_id = account_id",
    "delete holds": "DELETE FROM holds",
    "write held": "UPDATE accounts SET held = 0",
    "open a hold already captured": "INSERT INTO holds (client_id, idempotency_key, request_hash, account_id, "
                                    "payee_id, currency, amount, expires_at, status) "
                                    "VALUES ('r', 'k', 'x', 1, 2, 'INR', 1, now() + interval '1 day', 'captured')",
}


@pytest.mark.parametrize("statement", APP_ROLE_REFUSED.values(), ids=APP_ROLE_REFUSED.keys())
def test_app_role_is_refused(conn, shop, statement):
    alice, merchant = shop
    hold(conn, "h1", alice, merchant, 300)
    with pytest.raises(errors.InsufficientPrivilege):
        conn.execute(statement)
    assert acct(conn, alice) == (1_000, 300, 700)


def test_owner_cannot_write_held(owner_conn, shop):
    alice, _ = shop
    with pytest.raises(errors.CheckViolation, match="derived from entries"):
        owner_conn.execute("UPDATE accounts SET held = 5 WHERE id = %s", (alice,))


# ------------------------------------------------------------------- races

def test_authorization_race(pool, conn, shop):
    alice, merchant = shop
    results = run_together(pool, 50, lambda c, i: core.authorize(
        c, client=CLIENT, key=f"h{i}", account=alice, payee=merchant, amount=30))
    assert Counter(map(outcome, results)) == {"ok": 33, "InsufficientFunds": 17}
    assert acct(conn, alice) == (1_000, 990, 10)


def test_holds_and_transfers_race_for_the_same_money(pool, conn, shop, make_account):
    alice, merchant = shop
    bob = make_account("bob")

    def go(c, i):
        if i % 2:
            return core.authorize(c, client=CLIENT, key=f"h{i}", account=alice, payee=merchant, amount=30)
        return core.transfer(c, client=CLIENT, key=f"t{i}", from_account=alice, to_account=bob, amount=30)

    results = run_together(pool, 50, go)
    assert Counter(map(outcome, results))["ok"] == 33
    assert acct(conn, alice)[2] == 10
    assert core.check_invariants(conn)["ok"]


def test_capture_versus_void_race(pool, conn, shop):
    # The merchant captures while the customer cancels. Exactly one wins.
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 300)

    def go(c, i):
        if i % 2:
            return core.capture(c, client=CLIENT, key=f"c{i}", hold_id=h.hold_id)
        return core.void(c, hold_id=h.hold_id)

    results = run_together(pool, 20, go)
    counts = Counter(f"{'capture' if i % 2 else 'void'}:{outcome(r)}" for i, r in enumerate(results))
    assert not any("Deadlock" in k for k in counts), counts
    final = core.get_hold(conn, h.hold_id)["status"]
    if final == "captured":
        assert counts == {"capture:ok": 1, "capture:HoldClosed": 9, "void:HoldClosed": 10}, counts
        assert balance(conn, merchant) == 300
    else:
        assert final == "voided"
        assert counts == {"void:ok": 10, "capture:HoldClosed": 10}, counts
        assert balance(conn, merchant) == 0
    assert acct(conn, alice)[1] == 0
    assert core.check_invariants(conn)["ok"]


def test_double_capture_race(pool, conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 300)
    results = run_together(pool, 20, lambda c, i: core.capture(c, client=CLIENT, key=f"c{i}", hold_id=h.hold_id))
    assert Counter(map(outcome, results)) == {"ok": 1, "HoldClosed": 19}
    assert balance(conn, merchant) == 300


def test_capture_retry_storm_replays(pool, conn, shop):
    alice, merchant = shop
    h = hold(conn, "h1", alice, merchant, 300)
    results = run_together(pool, 20, lambda c, i: core.capture(c, client=CLIENT, key="c1", hold_id=h.hold_id))
    assert all(isinstance(r, core.Posted) for r in results), Counter(map(outcome, results))
    assert len({r.transaction_id for r in results}) == 1
    assert balance(conn, merchant) == 300


def test_authorize_retry_storm_holds_once(pool, conn, shop):
    alice, merchant = shop
    results = run_together(pool, 20, lambda c, i: core.authorize(
        c, client=CLIENT, key="h1", account=alice, payee=merchant, amount=700))
    assert all(isinstance(r, core.Authorized) for r in results), Counter(map(outcome, results))
    assert len({r.hold_id for r in results}) == 1
    assert acct(conn, alice)[1] == 700


# --------------------------------------------------------------------- API

@pytest.fixture
def client(pool):
    with TestClient(create_app(pool)) as c:
        yield c


def test_api_hold_lifecycle(client, conn, shop):
    alice, merchant = shop
    h = {"Idempotency-Key": "h1", "Client-Id": "shop-1"}
    r = client.post("/holds", headers=h, json={"account": alice, "payee": merchant, "amount": 500})
    assert r.status_code == 201
    hold_id = r.json()["id"]
    again = client.post("/holds", headers=h, json={"account": alice, "payee": merchant, "amount": 500})
    assert (again.status_code, again.headers.get("Idempotent-Replayed")) == (200, "true")
    assert client.get(f"/accounts/{alice}").json()["available"] == 500

    cap = client.post(f"/holds/{hold_id}/capture", headers={"Idempotency-Key": "c1", "Client-Id": "shop-1"},
                      json={"amount": 450})
    assert cap.status_code == 201
    assert cap.json()["entries"] == [{"account_id": alice, "amount": -450}, {"account_id": merchant, "amount": 450}]
    assert client.get(f"/holds/{hold_id}").json()["status"] == "captured"
    assert client.post(f"/holds/{hold_id}/void").status_code == 409


def test_api_capture_without_body_takes_all(client, shop):
    alice, merchant = shop
    hd = {"Idempotency-Key": "h1", "Client-Id": "shop-1"}
    hold_id = client.post("/holds", headers=hd, json={"account": alice, "payee": merchant, "amount": 200}).json()["id"]
    cap = client.post(f"/holds/{hold_id}/capture", headers={"Idempotency-Key": "c1", "Client-Id": "shop-1"})
    assert cap.status_code == 201
    assert client.get(f"/holds/{hold_id}").json()["captured_amount"] == 200


@pytest.mark.parametrize("body", [
    {"amount": 1.5}, {"amount": 100, "expires_in_seconds": 5}, {"amount": 100, "expires_in_seconds": 31 * 86400},
    {"amount": 100, "status": "captured"},
])
def test_api_rejects_bad_holds(client, shop, body):
    alice, merchant = shop
    r = client.post("/holds", headers={"Idempotency-Key": "h1", "Client-Id": "s"},
                    json={"account": alice, "payee": merchant, **body})
    assert r.status_code == 422
