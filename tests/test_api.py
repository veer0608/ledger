import pytest
from fastapi.testclient import TestClient

from ledger.api import create_app


@pytest.fixture
def client(pool):
    with TestClient(create_app(pool)) as c:
        yield c


def account(client, name, allow_negative=False, currency="INR"):
    r = client.post("/accounts", json={"name": name, "currency": currency, "allow_negative": allow_negative})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def pay(client, key, src, dst, amount, **extra):
    return client.post("/transfers", headers={"Idempotency-Key": key},
                       json={"from_account": src, "to_account": dst, "amount": amount, **extra})


def test_transfer_flow(client):
    mint = account(client, "mint", allow_negative=True)
    alice = account(client, "alice")
    r = pay(client, "k1", mint, alice, 5_000, memo="opening")
    assert r.status_code == 201
    assert r.json()["entries"] == [{"account_id": mint, "amount": -5_000}, {"account_id": alice, "amount": 5_000}]
    assert client.get(f"/accounts/{alice}").json()["balance"] == 5_000


def test_replay_returns_200_and_header(client):
    mint = account(client, "mint", allow_negative=True)
    alice = account(client, "alice")
    first = pay(client, "k1", mint, alice, 100)
    again = pay(client, "k1", mint, alice, 100)
    assert again.status_code == 200
    assert again.headers["Idempotent-Replayed"] == "true"
    assert again.json()["id"] == first.json()["id"]
    assert client.get(f"/accounts/{alice}").json()["balance"] == 100


def test_key_reuse_with_different_body_is_409(client):
    mint = account(client, "mint", allow_negative=True)
    alice = account(client, "alice")
    pay(client, "k1", mint, alice, 100)
    r = pay(client, "k1", mint, alice, 999)
    assert r.status_code == 409
    assert r.json()["error"] == "idempotency_conflict"


def test_missing_idempotency_key_is_rejected(client):
    r = client.post("/transfers", json={"from_account": 1, "to_account": 2, "amount": 1})
    assert r.status_code == 422


@pytest.mark.parametrize("amount", [1.5, 1.0, "100", True, 0, -5])
def test_non_integer_amounts_rejected(client, amount):
    mint = account(client, "mint", allow_negative=True)
    alice = account(client, "alice")
    assert pay(client, "k1", mint, alice, amount).status_code == 422


def test_insufficient_funds_is_422(client):
    alice = account(client, "alice")
    bob = account(client, "bob")
    r = pay(client, "k1", alice, bob, 1)
    assert r.status_code == 422
    assert r.json()["error"] == "insufficient_funds"


def test_unknown_account_is_404(client):
    assert client.get("/accounts/424242").status_code == 404


def test_reverse_and_statement(client):
    mint = account(client, "mint", allow_negative=True)
    alice = account(client, "alice")
    t = pay(client, "k1", mint, alice, 300).json()
    r = client.post(f"/transactions/{t['id']}/reverse", headers={"Idempotency-Key": "rev1"})
    assert r.status_code == 201
    assert r.json()["reverses_id"] == t["id"]
    again = client.post(f"/transactions/{t['id']}/reverse", headers={"Idempotency-Key": "rev2"})
    assert again.status_code == 409
    st = client.get(f"/accounts/{alice}/statement").json()
    assert [e["balance_after"] for e in st["entries"]] == [0, 300]


def test_health_reports_invariants(client):
    mint = account(client, "mint", allow_negative=True)
    alice = account(client, "alice")
    pay(client, "k1", mint, alice, 300)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["ok"] is True
