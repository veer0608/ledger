import pytest

from ledger import core
from tests.conftest import balance


def test_transfer_moves_money_and_balances(conn, make_account):
    a = make_account("alice", 10_000)
    b = make_account("bob")
    posted = core.transfer(conn, key="t1", from_account=a, to_account=b, amount=2_500, memo="rent")
    assert posted.replayed is False
    assert balance(conn, a) == 7_500
    assert balance(conn, b) == 2_500
    txn = core.get_transaction(conn, posted.transaction_id)
    assert [(e["account_id"], e["amount"]) for e in txn["entries"]] == [(a, -2_500), (b, 2_500)]
    assert core.check_invariants(conn)["ok"]


def test_insufficient_funds_writes_nothing(conn, make_account):
    a = make_account("alice", 100)
    b = make_account("bob")
    before = conn.execute("SELECT count(*) FROM transactions").fetchone()[0]
    with pytest.raises(core.InsufficientFunds):
        core.transfer(conn, key="t1", from_account=a, to_account=b, amount=101)
    assert balance(conn, a) == 100
    assert balance(conn, b) == 0
    assert conn.execute("SELECT count(*) FROM transactions").fetchone()[0] == before


def test_failed_transfer_does_not_burn_the_key(conn, make_account):
    a = make_account("alice", 100)
    b = make_account("bob")
    with pytest.raises(core.InsufficientFunds):
        core.transfer(conn, key="t1", from_account=a, to_account=b, amount=500)
    core.transfer(conn, key="top-up", from_account=core.account_id(conn, "mint:INR"),
                  to_account=a, amount=1_000)
    posted = core.transfer(conn, key="t1", from_account=a, to_account=b, amount=500)
    assert posted.replayed is False
    assert balance(conn, b) == 500


def test_retry_with_same_key_replays_instead_of_paying_twice(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    first = core.transfer(conn, key="t1", from_account=a, to_account=b, amount=1_000)
    # The retry would fail for insufficient funds if it ran again. It must replay.
    second = core.transfer(conn, key="t1", from_account=a, to_account=b, amount=1_000)
    assert second == core.Posted(first.transaction_id, replayed=True)
    assert balance(conn, a) == 0
    assert balance(conn, b) == 1_000


def test_same_key_different_request_is_rejected(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    core.transfer(conn, key="t1", from_account=a, to_account=b, amount=100)
    with pytest.raises(core.IdempotencyConflict):
        core.transfer(conn, key="t1", from_account=a, to_account=b, amount=200)
    assert balance(conn, b) == 100


def test_currency_mismatch(conn, make_account):
    inr = make_account("alice-inr", 1_000)
    usd = make_account("bob-usd", currency="USD")
    with pytest.raises(core.CurrencyMismatch):
        core.transfer(conn, key="t1", from_account=inr, to_account=usd, amount=100)


def test_unknown_account(conn, make_account):
    a = make_account("alice", 1_000)
    with pytest.raises(core.NotFound):
        core.transfer(conn, key="t1", from_account=a, to_account=999_999, amount=100)


@pytest.mark.parametrize("amount", [0, -1, 1.5, True, "100", None, core.MAX_AMOUNT + 1])
def test_bad_amounts_rejected(conn, make_account, amount):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    with pytest.raises(core.LedgerError):
        core.transfer(conn, key="t1", from_account=a, to_account=b, amount=amount)


def test_self_transfer_rejected(conn, make_account):
    a = make_account("alice", 1_000)
    with pytest.raises(core.LedgerError):
        core.transfer(conn, key="t1", from_account=a, to_account=a, amount=1)


def test_duplicate_account_name(conn, make_account):
    make_account("alice")
    with pytest.raises(core.Conflict):
        core.create_account(conn, "alice", "INR")


def test_reversal_restores_balances(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    t = core.transfer(conn, key="t1", from_account=a, to_account=b, amount=400, external_ref="UTR1")
    r = core.reverse(conn, key="r1", transaction_id=t.transaction_id)
    assert balance(conn, a) == 1_000
    assert balance(conn, b) == 0
    rev = core.get_transaction(conn, r.transaction_id)
    assert rev["reverses_id"] == t.transaction_id
    assert rev["external_ref"] == "UTR1"
    assert core.check_invariants(conn)["ok"]


def test_reversal_is_idempotent_and_happens_once(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    t = core.transfer(conn, key="t1", from_account=a, to_account=b, amount=400)
    r1 = core.reverse(conn, key="r1", transaction_id=t.transaction_id)
    assert core.reverse(conn, key="r1", transaction_id=t.transaction_id) == core.Posted(r1.transaction_id, True)
    with pytest.raises(core.AlreadyReversed):
        core.reverse(conn, key="r2", transaction_id=t.transaction_id)
    assert balance(conn, a) == 1_000


def test_cannot_reverse_a_reversal(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    t = core.transfer(conn, key="t1", from_account=a, to_account=b, amount=400)
    r = core.reverse(conn, key="r1", transaction_id=t.transaction_id)
    with pytest.raises(core.LedgerError):
        core.reverse(conn, key="r2", transaction_id=r.transaction_id)


def test_reversal_needs_the_money_to_still_be_there(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    c = make_account("carol")
    t = core.transfer(conn, key="t1", from_account=a, to_account=b, amount=400)
    core.transfer(conn, key="t2", from_account=b, to_account=c, amount=300)
    with pytest.raises(core.InsufficientFunds):
        core.reverse(conn, key="r1", transaction_id=t.transaction_id)
    assert balance(conn, b) == 100


def test_statement_running_balance(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    core.transfer(conn, key="t1", from_account=a, to_account=b, amount=100)
    core.transfer(conn, key="t2", from_account=a, to_account=b, amount=250)
    st = core.statement(conn, a)
    assert [(e["amount"], e["balance_after"]) for e in st["entries"]] == [(-250, 650), (-100, 900), (1_000, 1_000)]
    assert st["account"]["balance"] == st["entries"][0]["balance_after"]
