"""Shard groups: one busy system account split across several rows."""
from collections import Counter

import pytest
from fastapi.testclient import TestClient
from psycopg import errors

from ledger import core, recon
from ledger.api import create_app
from tests.conftest import CLIENT, balance
from tests.test_concurrency import outcome, run_together


@pytest.fixture
def bank(owner_conn):
    return core.create_shard_group(owner_conn, "bank:hdfc", "INR", 8)


def deposit(conn, key, to, amount, ref=None):
    src = core.shard_for(conn, "bank:hdfc", client=CLIENT, key=key)
    return core.transfer(conn, client=CLIENT, key=key, from_account=src, to_account=to, amount=amount,
                         external_ref=ref)


def test_group_is_created_by_the_owner_as_negative_capable_shards(conn, bank):
    assert len(bank) == 8
    shard = core.get_account(conn, bank[3])
    assert (shard["name"], shard["shard_group"], shard["allow_negative"]) == ("bank:hdfc#3", "bank:hdfc", True)


def test_routing_is_deterministic_and_spreads(conn, bank):
    picks = [core.shard_for(conn, "bank:hdfc", client=CLIENT, key=f"k{i}") for i in range(400)]
    assert picks == [core.shard_for(conn, "bank:hdfc", client=CLIENT, key=f"k{i}") for i in range(400)]
    counts = Counter(picks)
    assert set(counts) == set(bank)
    assert max(counts.values()) < 2 * min(counts.values())  # roughly even: 50 each expected


def test_retry_lands_on_the_same_shard_and_replays(conn, bank, make_account):
    alice = make_account("alice")
    first = deposit(conn, "dep-1", alice, 500)
    again = deposit(conn, "dep-1", alice, 500)
    assert again == core.Posted(first.transaction_id, True)
    assert balance(conn, alice) == 500


def test_group_balance_is_the_sum_of_its_shards(conn, bank, make_account):
    alice = make_account("alice")
    for i in range(20):
        deposit(conn, f"d{i}", alice, 100)
    g = core.group_balance(conn, "bank:hdfc")
    assert (g["shards"], g["balance"]) == (8, -2_000)
    assert sum(balance(conn, s) for s in bank) == -2_000
    assert core.check_invariants(conn)["ok"]


def test_reference_rule_spans_shards(conn, bank, make_account):
    # Two deliveries of one UTR under different keys route to different shards.
    alice = make_account("alice")
    keys = [f"hook-{i}" for i in range(50)]
    k1 = keys[0]
    k2 = next(k for k in keys if core.shard_for(conn, "bank:hdfc", client=CLIENT, key=k)
              != core.shard_for(conn, "bank:hdfc", client=CLIENT, key=k1))
    deposit(conn, k1, alice, 500, ref="UTR9")
    with pytest.raises(core.DuplicateReference):
        deposit(conn, k2, alice, 500, ref="UTR9")


def test_recon_reads_the_whole_group(conn, bank, make_account, tmp_path):
    alice = make_account("alice")
    for i in range(6):
        deposit(conn, f"d{i}", alice, 100_00, ref=f"UTR{i}")
    today = recon.local_today()
    lines = "".join(f"{today},100.00,UTR{i}\n" for i in range(5))
    p = tmp_path / "s.csv"
    p.write_text("date,amount,reference\n" + lines, encoding="utf-8")
    got = Counter(r.status for r in recon.run(conn, p, "bank:hdfc", today, today))
    assert got == {"matched": 5, "ledger_only": 1}


def test_app_role_cannot_make_a_shard(conn):
    with pytest.raises(core.Forbidden):
        core.create_account(conn, "x#0", "INR", allow_negative=True, shard_group="x")
    with pytest.raises(errors.InsufficientPrivilege):
        conn.execute("INSERT INTO accounts (name, currency, shard_group) VALUES ('y', 'INR', 'g')")


def test_only_negative_capable_accounts_can_be_shards(owner_conn):
    # A shard with a balance floor would need the group total under a lock,
    # which is the contention sharding exists to remove.
    with pytest.raises(errors.CheckViolation):
        owner_conn.execute("INSERT INTO accounts (name, currency, shard_group) VALUES ('m#0', 'INR', 'm')")


def test_unknown_group(conn):
    with pytest.raises(core.NotFound):
        core.shard_for(conn, "nope", client=CLIENT, key="k")


def test_concurrent_deposits_through_the_group(pool, conn, bank, make_account):
    users = [make_account(f"u{i}") for i in range(10)]
    results = run_together(pool, 40, lambda c, i: core.transfer(
        c, client=CLIENT, key=f"d{i}", from_account=core.shard_for(c, "bank:hdfc", client=CLIENT, key=f"d{i}"),
        to_account=users[i % 10], amount=7))
    assert Counter(map(outcome, results)) == {"ok": 40}
    assert core.group_balance(conn, "bank:hdfc")["balance"] == -280
    assert core.check_invariants(conn)["ok"]


# --------------------------------------------------------------------- API

@pytest.fixture
def client(pool):
    with TestClient(create_app(pool)) as c:
        yield c


def test_api_transfer_from_a_group(client, bank, make_account):
    alice = make_account("alice")
    hd = {"Idempotency-Key": "dep-1", "Client-Id": "webhooks"}
    body = {"from_group": "bank:hdfc", "to_account": alice, "amount": 250, "external_ref": "UTR1"}
    r = client.post("/transfers", headers=hd, json=body)
    assert r.status_code == 201
    assert r.json()["entries"][0]["account_id"] in bank
    again = client.post("/transfers", headers=hd, json=body)
    assert (again.status_code, again.json()["id"]) == (200, r.json()["id"])
    assert client.get("/groups/bank:hdfc").json()["balance"] == -250


@pytest.mark.parametrize("body", [
    {"to_account": 1, "amount": 1},
    {"from_account": 1, "from_group": "bank:hdfc", "to_account": 2, "amount": 1},
])
def test_api_needs_exactly_one_source(client, body):
    r = client.post("/transfers", headers={"Idempotency-Key": "k", "Client-Id": "c"}, json=body)
    assert r.status_code == 422


def test_api_unknown_group_is_404(client, make_account):
    alice = make_account("alice")
    r = client.post("/transfers", headers={"Idempotency-Key": "k", "Client-Id": "c"},
                    json={"from_group": "nope", "to_account": alice, "amount": 1})
    assert r.status_code == 404
