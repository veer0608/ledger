"""Outbox: every committed fact has exactly one event, and the relay delivers it."""
import json
import threading
import time
from collections import Counter
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg
import pytest
from psycopg import errors

from ledger import core, relay
from tests.conftest import CLIENT
from tests.test_concurrency import run_together

SECRET = "test-secret-0123456789"
FAST = relay.Policy(base_delay=0.0, max_attempts=3, timeout=2.0, batch=10)


def events(conn, **where):
    sql = "SELECT * FROM outbox"
    if where:
        sql += " WHERE " + " AND ".join(f"{k} = %({k})s" for k in where)
    return core._cur(conn).execute(sql + " ORDER BY id", where).fetchall()


def pay(conn, key, src, dst, amount, **kw):
    return core.transfer(conn, client=CLIENT, key=key, from_account=src, to_account=dst, amount=amount, **kw)


@pytest.fixture
def pair(make_account):
    return make_account("alice", 1_000), make_account("bob")


# ------------------------------------------------------------ what is written

def test_a_transfer_writes_one_event_with_its_entries(conn, pair):
    alice, bob = pair
    before = len(events(conn))
    t = pay(conn, "p1", alice, bob, 250, external_ref="UTR1", memo="rent")
    [e] = events(conn, subject_id=t.transaction_id, event_type="transaction.posted")
    assert len(events(conn)) == before + 1
    assert e["payload"]["entries"] == [{"account_id": alice, "amount": -250}, {"account_id": bob, "amount": 250}]
    assert (e["payload"]["external_ref"], e["payload"]["memo"], e["payload"]["client_id"]) == ("UTR1", "rent", CLIENT)
    assert (e["attempts"], e["delivered_at"], e["dead_at"]) == (0, None, None)


def test_a_replay_writes_no_second_event(conn, pair):
    alice, bob = pair
    t = pay(conn, "p1", alice, bob, 250)
    n = len(events(conn))
    assert pay(conn, "p1", alice, bob, 250).replayed
    assert len(events(conn)) == n
    assert len(events(conn, subject_id=t.transaction_id, event_type="transaction.posted")) == 1


def test_a_refused_transfer_writes_no_event(conn, pair):
    alice, bob = pair
    n = len(events(conn))
    with pytest.raises(core.InsufficientFunds):
        pay(conn, "p1", alice, bob, 5_000)
    assert len(events(conn)) == n


def test_a_rolled_back_transfer_writes_no_event(conn, pair):
    # The transfer itself succeeds; the caller's outer transaction is what rolls back.
    alice, bob = pair
    n = len(events(conn))
    with conn.transaction():
        pay(conn, "p1", alice, bob, 250)
        raise psycopg.Rollback()
    assert len(events(conn)) == n
    assert core.check_invariants(conn)["ok"]


def test_a_reversal_is_its_own_event(conn, pair):
    alice, bob = pair
    t = pay(conn, "p1", alice, bob, 250)
    r = core.reverse(conn, client=CLIENT, key="r1", transaction_id=t.transaction_id)
    [e] = events(conn, subject_id=r.transaction_id)
    assert e["event_type"] == "transaction.reversed"
    assert e["payload"]["reverses_id"] == t.transaction_id


def test_a_hold_emits_each_status_once(conn, make_account):
    alice, shop = make_account("alice", 1_000), make_account("shop")
    h1 = core.authorize(conn, client=CLIENT, key="h1", account=alice, payee=shop, amount=400)
    cap = core.capture(conn, client=CLIENT, key="c1", hold_id=h1.hold_id, amount=300)
    h2 = core.authorize(conn, client=CLIENT, key="h2", account=alice, payee=shop, amount=100)
    core.void(conn, hold_id=h2.hold_id)
    core.void(conn, hold_id=h2.hold_id)  # a retried void is a no-op, and so is its event
    h3 = core.authorize(conn, client=CLIENT, key="h3", account=alice, payee=shop, amount=50,
                        expires_in=timedelta(milliseconds=50))
    time.sleep(0.2)
    core.expire_holds(conn)

    by_hold = lambda h: [e["event_type"] for e in events(conn, subject_id=h.hold_id) if e["event_type"][:5] == "hold."]
    assert by_hold(h1) == ["hold.authorized", "hold.captured"]
    assert by_hold(h2) == ["hold.authorized", "hold.voided"]
    assert by_hold(h3) == ["hold.authorized", "hold.expired"]
    [captured] = events(conn, subject_id=h1.hold_id, event_type="hold.captured")
    assert (captured["payload"]["captured_amount"], captured["payload"]["capture_transaction_id"]) == \
        (300, cap.transaction_id)
    assert events(conn, subject_id=cap.transaction_id, event_type="transaction.posted")
    assert core.check_invariants(conn)["ok"]


def test_concurrent_transfers_each_get_exactly_one_event(pool, conn, make_account):
    src = make_account("src", 10_000)
    users = [make_account(f"u{i}") for i in range(10)]
    results = run_together(pool, 40, lambda c, i: pay(c, f"k{i % 20}", src, users[i % 10], 7))
    ids = {r.transaction_id for r in results}
    assert len(ids) == 20  # 40 requests, 20 distinct keys
    posted = Counter(e["subject_id"] for e in events(conn, event_type="transaction.posted"))
    assert all(posted[i] == 1 for i in ids)
    assert core.check_invariants(conn)["ok"]


# ------------------------------------------------------------- who may write

APP_REFUSED = {
    "forge an event": "INSERT INTO outbox (event_type, subject_id, payload) VALUES ('transaction.posted', 1, '{}')",
    "rewrite a payload": "UPDATE outbox SET payload = '{}'",
    "change an event's type": "UPDATE outbox SET event_type = 'x'",
    "delete an event": "DELETE FROM outbox",
    "truncate the outbox": "TRUNCATE outbox",
}


@pytest.mark.parametrize("sql", APP_REFUSED.values(), ids=APP_REFUSED.keys())
def test_app_role_cannot_tamper_with_events(conn, pair, sql):
    pay(conn, "p1", *pair, 10)
    with pytest.raises(errors.InsufficientPrivilege):
        conn.execute(sql)


def test_even_the_owner_cannot_rewrite_or_delete_an_event(owner_conn, conn, pair):
    pay(conn, "p1", *pair, 10)
    with pytest.raises(errors.RestrictViolation, match="immutable"):
        owner_conn.execute("UPDATE outbox SET payload = '{}'")
    with pytest.raises(errors.RestrictViolation, match="never deleted"):
        owner_conn.execute("DELETE FROM outbox")


def test_delivery_state_only_moves_forward(conn, pair):
    pay(conn, "p1", *pair, 10)
    conn.execute("UPDATE outbox SET delivered_at = now()")
    with pytest.raises(errors.RestrictViolation, match="already delivered"):
        conn.execute("UPDATE outbox SET delivered_at = NULL")
    pay(conn, "p2", *pair, 10)
    conn.execute("UPDATE outbox SET attempts = 3 WHERE delivered_at IS NULL")
    with pytest.raises(errors.RestrictViolation, match="cannot go down"):
        conn.execute("UPDATE outbox SET attempts = 1 WHERE delivered_at IS NULL")


# ------------------------------------------------------------------- relay

class Receiver:
    """A webhook endpoint on localhost that records what it gets."""

    def __init__(self):
        self.calls: list[dict] = []
        self.fail_next = 0
        self.status_on_fail = 500
        self.delay = 0.0
        self.lock = threading.Lock()
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                time.sleep(receiver.delay)
                with receiver.lock:
                    receiver.calls.append({"headers": dict(self.headers), "body": body})
                    failing = receiver.fail_next > 0
                    receiver.fail_next -= failing
                self.send_response(receiver.status_on_fail if failing else 204)
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/hooks"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def ids(self):
        return Counter(int(c["headers"]["Ledger-Event-Id"]) for c in self.calls)

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def receiver():
    r = Receiver()
    yield r
    r.close()


def test_relay_delivers_signed_events(conn, pair, receiver):
    alice, bob = pair
    t = pay(conn, "p1", alice, bob, 250)
    assert relay.run_once(conn, receiver.url, SECRET, FAST)["delivered"] == len(events(conn))
    assert all(e["delivered_at"] for e in events(conn))
    call = next(c for c in receiver.calls if c["headers"]["Ledger-Event-Type"] == "transaction.posted"
                and json.loads(c["body"])["data"]["transaction_id"] == t.transaction_id)
    assert relay.verify(SECRET, call["body"], call["headers"]["Ledger-Signature"])
    assert not relay.verify("wrong-secret-0123456789", call["body"], call["headers"]["Ledger-Signature"])
    assert not relay.verify(SECRET, call["body"].replace(b"250", b"999"), call["headers"]["Ledger-Signature"])
    assert relay.run_once(conn, receiver.url, SECRET, FAST)["delivered"] == 0  # nothing left


def test_signature_refuses_an_old_timestamp():
    body = b'{"id":1}'
    header = relay.sign(SECRET, body, 1_000_000)
    assert relay.verify(SECRET, body, header, now=1_000_000 + 299)
    assert not relay.verify(SECRET, body, header, now=1_000_000 + 301)
    assert not relay.verify(SECRET, body, "garbage")


def test_a_failed_delivery_is_retried_later_with_the_same_body(conn, owner_conn, pair, receiver):
    pay(conn, "p1", *pair, 10)
    receiver.fail_next = 1
    slow = relay.Policy(base_delay=60, max_attempts=3, batch=1, timeout=2.0)
    n = len(events(conn))
    first = relay.run_once(conn, receiver.url, SECRET, slow)
    assert first["failed"] == 1
    failed = [e for e in events(conn) if e["delivered_at"] is None]
    assert len(failed) == 1 and failed[0]["last_error"] == "HTTP 500" and failed[0]["attempts"] == 1
    # Backoff: not due yet, so a second pass leaves it alone.
    assert relay.run_once(conn, receiver.url, SECRET, slow) == {"delivered": 0, "failed": 0, "dead": 0}
    owner_conn.execute("UPDATE outbox SET next_attempt_at = now() WHERE delivered_at IS NULL")
    assert relay.run_once(conn, receiver.url, SECRET, slow)["delivered"] == 1
    bodies = [c["body"] for c in receiver.calls if int(c["headers"]["Ledger-Event-Id"]) == failed[0]["id"]]
    assert len(bodies) == 2 and bodies[0] == bodies[1]
    assert [c["headers"]["Ledger-Delivery-Attempt"] for c in receiver.calls
            if int(c["headers"]["Ledger-Event-Id"]) == failed[0]["id"]] == ["1", "2"]
    assert all(e["delivered_at"] for e in events(conn)) and len(events(conn)) == n


def test_an_event_goes_dead_after_max_attempts(conn, pair, receiver):
    pay(conn, "p1", *pair, 10)
    receiver.fail_next = 10_000
    counts = relay.run_once(conn, receiver.url, SECRET, FAST)
    total = len(events(conn))
    assert counts == {"delivered": 0, "failed": total * 2, "dead": total}
    assert all(e["dead_at"] and e["attempts"] == 3 for e in events(conn))
    assert relay.run_once(conn, receiver.url, SECRET, FAST)["dead"] == 0  # dead stays dead


def test_an_unreachable_receiver_is_a_failure_not_a_crash(conn, pair):
    pay(conn, "p1", *pair, 10)
    counts = relay.run_once(conn, "http://127.0.0.1:9/nothing-here", SECRET, FAST)
    assert counts["dead"] == len(events(conn))
    assert all(e["last_error"] for e in events(conn))


def test_a_slow_receiver_times_out(conn, pair, receiver):
    pay(conn, "p1", *pair, 10)
    receiver.delay = 1.0
    counts = relay.run_once(conn, receiver.url, SECRET, relay.Policy(timeout=0.2, max_attempts=1, batch=50))
    assert counts["dead"] == len(events(conn))
    assert "timed out" in events(conn)[0]["last_error"]


def test_a_crashed_relays_events_come_back_after_the_lease(conn, pair, receiver):
    pay(conn, "p1", *pair, 10)
    short = relay.Policy(lease=0.3, batch=100, timeout=2.0)
    taken = relay.claim(conn, short)  # a relay claims, then dies before sending
    assert taken
    assert relay.run_once(conn, receiver.url, SECRET, short)["delivered"] == 0  # still leased
    time.sleep(0.5)
    assert relay.run_once(conn, receiver.url, SECRET, short)["delivered"] == len(taken)
    assert all(e["attempts"] == 2 for e in events(conn))


def test_a_stale_relays_failure_does_not_move_a_newer_claim(conn, pair):
    pay(conn, "p1", *pair, 10)
    short = relay.Policy(lease=0.2, batch=100)
    [old, *_] = relay.claim(conn, short)
    time.sleep(0.4)
    [new, *_] = relay.claim(conn, relay.Policy(lease=600, batch=100))  # another relay takes over
    assert new["id"] == old["id"] and new["attempts"] == 2
    relay.mark_failed(conn, old, "late failure from the first relay", relay.Policy(base_delay=0))
    row = events(conn, id=old["id"])[0]
    assert row["last_error"] is None  # fenced out: attempts no longer matched
    assert row["next_attempt_at"] > row["created_at"] + timedelta(seconds=300)


def test_relays_running_together_deliver_each_event_once(pool, conn, make_account, receiver):
    src = make_account("src", 100_000)
    dst = make_account("dst")
    for i in range(150):
        pay(conn, f"k{i}", src, dst, 1)
    total = len(events(conn))
    policy = relay.Policy(batch=5, timeout=5.0)
    results = run_together(pool, 8, lambda c, i: relay.run_once(c, receiver.url, SECRET, policy))
    assert sum(r["delivered"] for r in results) == total
    assert sum(1 for r in results if r["delivered"]) > 1  # the work really was shared
    assert set(receiver.ids().values()) == {1}
    assert len(receiver.ids()) == total


def test_invariants_report_a_fact_without_an_event(owner_conn, conn, pair):
    pay(conn, "p1", *pair, 10)
    assert core.check_invariants(conn)["missing_events"] == []
    with owner_conn.transaction():
        owner_conn.execute("SET LOCAL session_replication_role = replica")  # what a raw restore could do
        owner_conn.execute("DELETE FROM outbox WHERE event_type = 'transaction.posted'")
    report = core.check_invariants(conn)
    assert not report["ok"]
    assert {r["subject"] for r in report["missing_events"]} == {"transaction"}


def test_upgrading_a_database_with_history_records_old_facts_as_never_sent(owner_url, owner_conn, conn,
                                                                          make_account):
    from ledger import db
    alice, shop = make_account("alice", 1_000), make_account("shop")
    h = core.authorize(conn, client=CLIENT, key="h1", account=alice, payee=shop, amount=100)
    core.void(conn, hold_id=h.hold_id)
    with owner_conn.transaction():  # as if this history predates the outbox
        owner_conn.execute("SET LOCAL session_replication_role = replica")
        owner_conn.execute("DELETE FROM outbox")
    assert not core.check_invariants(conn)["ok"]
    db.migrate(owner_url)
    assert core.check_invariants(conn)["ok"]
    old = events(conn)
    assert {e["event_type"] for e in old} == {"transaction.posted", "hold.authorized", "hold.voided"}
    assert all(e["dead_at"] and "never sent" in e["last_error"] for e in old)
    db.migrate(owner_url)  # running it again adds nothing
    assert len(events(conn)) == len(old)
