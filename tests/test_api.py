import pytest
from fastapi.testclient import TestClient

from ledger import core
from ledger.api import create_app

HEADERS = {"Client-Id": "shop-1"}


@pytest.fixture
def client(pool):
    with TestClient(create_app(pool)) as c:
        yield c


@pytest.fixture
def mint(owner_conn):
    # Accounts that may go negative are the owner's to create; the API cannot.
    return core.create_account(owner_conn, "mint", "INR", allow_negative=True)["id"]


def account(client, name, currency="INR"):
    r = client.post("/accounts", json={"name": name, "currency": currency})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def pay(client, key, src, dst, amount, headers=HEADERS, **extra):
    return client.post("/transfers", headers={"Idempotency-Key": key, **headers},
                       json={"from_account": src, "to_account": dst, "amount": amount, **extra})


def test_transfer_flow(client, mint):
    alice = account(client, "alice")
    r = pay(client, "k1", mint, alice, 5_000, memo="opening")
    assert r.status_code == 201
    assert r.json()["entries"] == [{"account_id": mint, "amount": -5_000}, {"account_id": alice, "amount": 5_000}]
    assert r.json()["client_id"] == "shop-1"
    assert client.get(f"/accounts/{alice}").json()["balance"] == 5_000


def test_api_cannot_create_a_mint(client):
    r = client.post("/accounts", json={"name": "m", "currency": "INR", "allow_negative": True})
    assert r.status_code == 422


def test_replay_returns_200_and_header(client, mint):
    alice = account(client, "alice")
    first = pay(client, "k1", mint, alice, 100)
    again = pay(client, "k1", mint, alice, 100)
    assert again.status_code == 200
    assert again.headers["Idempotent-Replayed"] == "true"
    assert again.json()["id"] == first.json()["id"]
    assert client.get(f"/accounts/{alice}").json()["balance"] == 100


def test_key_is_scoped_to_client(client, mint):
    alice = account(client, "alice")
    assert pay(client, "k1", mint, alice, 100).status_code == 201
    assert pay(client, "k1", mint, alice, 100, headers={"Client-Id": "shop-2"}).status_code == 201
    assert client.get(f"/accounts/{alice}").json()["balance"] == 200


def test_key_reuse_with_different_body_is_409(client, mint):
    alice = account(client, "alice")
    pay(client, "k1", mint, alice, 100)
    r = pay(client, "k1", mint, alice, 999)
    assert r.status_code == 409
    assert r.json()["error"] == "idempotency_conflict"


def test_duplicate_reference_is_409(client, mint):
    alice = account(client, "alice")
    pay(client, "k1", mint, alice, 100, external_ref="UTR1")
    r = pay(client, "k2", mint, alice, 100, external_ref="UTR1")
    assert r.status_code == 409
    assert r.json()["error"] == "duplicate_external_ref"


@pytest.mark.parametrize("headers", [{"Idempotency-Key": "k"}, {"Client-Id": "c"}])
def test_missing_key_or_client_is_rejected(client, headers):
    r = client.post("/transfers", headers=headers, json={"from_account": 1, "to_account": 2, "amount": 1})
    assert r.status_code == 422


@pytest.mark.parametrize("amount", [1.5, 1.0, "100", True, 0, -5])
def test_non_integer_amounts_rejected(client, mint, amount):
    alice = account(client, "alice")
    assert pay(client, "k1", mint, alice, amount).status_code == 422


def test_nul_in_memo_is_400_not_500(client, mint):
    alice = account(client, "alice")
    r = pay(client, "k1", mint, alice, 1, memo="a\u0000b")
    assert r.status_code == 400


def test_insufficient_funds_is_422(client):
    alice = account(client, "alice")
    bob = account(client, "bob")
    r = pay(client, "k1", alice, bob, 1)
    assert r.status_code == 422
    assert r.json()["error"] == "insufficient_funds"


def test_unknown_account_is_404(client):
    assert client.get("/accounts/424242").status_code == 404


def test_reverse_and_statement(client, mint):
    alice = account(client, "alice")
    t = pay(client, "k1", mint, alice, 300).json()
    r = client.post(f"/transactions/{t['id']}/reverse", headers={"Idempotency-Key": "rev1", **HEADERS})
    assert r.status_code == 201
    assert r.json()["reverses_id"] == t["id"]
    again = client.post(f"/transactions/{t['id']}/reverse", headers={"Idempotency-Key": "rev2", **HEADERS})
    assert again.status_code == 409
    st = client.get(f"/accounts/{alice}/statement").json()
    assert [e["balance_after"] for e in st["entries"]] == [0, 300]


def test_health_reports_invariants(client, mint):
    alice = account(client, "alice")
    pay(client, "k1", mint, alice, 300)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["ok"] is True
