import pytest

from ledger import core
from tests.conftest import CLIENT, balance


def test_transfer_moves_money_and_balances(conn, make_account):
    a = make_account("alice", 10_000)
    b = make_account("bob")
    posted = core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=2_500, memo="rent")
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
        core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=101)
    assert balance(conn, a) == 100
    assert balance(conn, b) == 0
    assert conn.execute("SELECT count(*) FROM transactions").fetchone()[0] == before


def test_failed_transfer_does_not_burn_the_key(conn, make_account):
    a = make_account("alice", 100)
    b = make_account("bob")
    with pytest.raises(core.InsufficientFunds):
        core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=500)
    core.transfer(conn, client=CLIENT, key="top-up", from_account=core.account_id(conn, "mint:INR"),
                  to_account=a, amount=1_000)
    posted = core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=500)
    assert posted.replayed is False
    assert balance(conn, b) == 500


def test_retry_with_same_key_replays_instead_of_paying_twice(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    first = core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=1_000)
    # The retry would fail for insufficient funds if it ran again. It must replay.
    second = core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=1_000)
    assert second == core.Posted(first.transaction_id, replayed=True)
    assert balance(conn, a) == 0
    assert balance(conn, b) == 1_000


def test_same_key_different_request_is_rejected(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=100)
    with pytest.raises(core.IdempotencyConflict):
        core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=200)
    assert balance(conn, b) == 100


def test_currency_mismatch(conn, make_account):
    inr = make_account("alice-inr", 1_000)
    usd = make_account("bob-usd", currency="USD")
    with pytest.raises(core.CurrencyMismatch):
        core.transfer(conn, client=CLIENT, key="t1", from_account=inr, to_account=usd, amount=100)


def test_unknown_account(conn, make_account):
    a = make_account("alice", 1_000)
    with pytest.raises(core.NotFound):
        core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=999_999, amount=100)


@pytest.mark.parametrize("amount", [0, -1, 1.5, True, "100", None, core.MAX_AMOUNT + 1])
def test_bad_amounts_rejected(conn, make_account, amount):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    with pytest.raises(core.LedgerError):
        core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=amount)


def test_self_transfer_rejected(conn, make_account):
    a = make_account("alice", 1_000)
    with pytest.raises(core.LedgerError):
        core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=a, amount=1)


def test_duplicate_account_name(conn, make_account):
    make_account("alice")
    with pytest.raises(core.Conflict):
        core.create_account(conn, "alice", "INR")


def test_reversal_restores_balances(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    t = core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=400, external_ref="UTR1")
    r = core.reverse(conn, client=CLIENT, key="r1", transaction_id=t.transaction_id)
    assert balance(conn, a) == 1_000
    assert balance(conn, b) == 0
    rev = core.get_transaction(conn, r.transaction_id)
    assert rev["reverses_id"] == t.transaction_id
    assert rev["external_ref"] == "UTR1"
    assert core.check_invariants(conn)["ok"]


def test_reversal_is_idempotent_and_happens_once(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    t = core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=400)
    r1 = core.reverse(conn, client=CLIENT, key="r1", transaction_id=t.transaction_id)
    assert core.reverse(conn, client=CLIENT, key="r1", transaction_id=t.transaction_id) == core.Posted(r1.transaction_id, True)
    with pytest.raises(core.AlreadyReversed):
        core.reverse(conn, client=CLIENT, key="r2", transaction_id=t.transaction_id)
    assert balance(conn, a) == 1_000


def test_cannot_reverse_a_reversal(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    t = core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=400)
    r = core.reverse(conn, client=CLIENT, key="r1", transaction_id=t.transaction_id)
    with pytest.raises(core.LedgerError):
        core.reverse(conn, client=CLIENT, key="r2", transaction_id=r.transaction_id)


def test_reversal_needs_the_money_to_still_be_there(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    c = make_account("carol")
    t = core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=400)
    core.transfer(conn, client=CLIENT, key="t2", from_account=b, to_account=c, amount=300)
    with pytest.raises(core.InsufficientFunds):
        core.reverse(conn, client=CLIENT, key="r1", transaction_id=t.transaction_id)
    assert balance(conn, b) == 100


def test_statement_running_balance(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    core.transfer(conn, client=CLIENT, key="t1", from_account=a, to_account=b, amount=100)
    core.transfer(conn, client=CLIENT, key="t2", from_account=a, to_account=b, amount=250)
    st = core.statement(conn, a)
    assert [(e["amount"], e["balance_after"]) for e in st["entries"]] == [(-250, 650), (-100, 900), (1_000, 1_000)]
    assert st["account"]["balance"] == st["entries"][0]["balance_after"]


# ------------------------------------------------ idempotency is per client

def test_two_clients_can_use_the_same_key(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    c = make_account("carol")
    first = core.transfer(conn, client="shop-1", key="checkout-1", from_account=a, to_account=b, amount=10)
    second = core.transfer(conn, client="shop-2", key="checkout-1", from_account=b, to_account=c, amount=5)
    assert first.transaction_id != second.transaction_id
    assert not second.replayed


def test_identical_requests_from_two_clients_both_run(conn, make_account):
    # Before scoping, the second customer got a "success" replay and paid nothing.
    a = make_account("alice", 1_000)
    b = make_account("bob")
    core.transfer(conn, client="shop-1", key="1", from_account=a, to_account=b, amount=10)
    again = core.transfer(conn, client="shop-2", key="1", from_account=a, to_account=b, amount=10)
    assert not again.replayed
    assert balance(conn, b) == 20


@pytest.mark.parametrize("field,value", [("memo", "a\x00b"), ("external_ref", "UTR\n1"), ("key", "k\x01")])
def test_control_characters_rejected(conn, make_account, field, value):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    kwargs = {"client": CLIENT, "key": "t1", "from_account": a, "to_account": b, "amount": 1, field: value}
    with pytest.raises(core.LedgerError):
        core.transfer(conn, **kwargs)


def test_app_role_cannot_create_a_mint(conn):
    with pytest.raises(core.Forbidden):
        core.create_account(conn, "mint:sneaky", "INR", allow_negative=True)


# ------------------------------------------ one live posting per reference

def test_same_utr_under_a_new_key_is_refused(conn, make_account):
    bank = make_account("bank", allow_negative=True)
    a = make_account("alice")
    core.transfer(conn, client=CLIENT, key="hook-1", from_account=bank, to_account=a, amount=500, external_ref="UTR7")
    with pytest.raises(core.DuplicateReference, match="UTR7"):
        core.transfer(conn, client=CLIENT, key="hook-1-redelivered", from_account=bank, to_account=a,
                      amount=500, external_ref="UTR7")
    assert balance(conn, a) == 500


def test_same_utr_from_another_client_is_refused(conn, make_account):
    bank = make_account("bank", allow_negative=True)
    a = make_account("alice")
    core.transfer(conn, client="webhooks", key="1", from_account=bank, to_account=a, amount=500, external_ref="UTR7")
    with pytest.raises(core.DuplicateReference):
        core.transfer(conn, client="backfill-job", key="1", from_account=bank, to_account=a,
                      amount=500, external_ref="UTR7")


def test_reference_can_be_reposted_after_reversal(conn, make_account):
    # Credited the wrong wallet: reverse it, then post the same UTR to the right one.
    bank = make_account("bank", allow_negative=True)
    wrong = make_account("wrong")
    right = make_account("right")
    t = core.transfer(conn, client=CLIENT, key="d1", from_account=bank, to_account=wrong, amount=500, external_ref="UTR7")
    core.reverse(conn, client=CLIENT, key="r1", transaction_id=t.transaction_id)
    core.transfer(conn, client=CLIENT, key="d2", from_account=bank, to_account=right, amount=500, external_ref="UTR7")
    assert (balance(conn, wrong), balance(conn, right)) == (0, 500)
    with pytest.raises(core.DuplicateReference):
        core.transfer(conn, client=CLIENT, key="d3", from_account=bank, to_account=right, amount=500, external_ref="UTR7")
