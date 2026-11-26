"""The database must refuse bad money even when the application is bypassed.

Each test writes raw SQL, as a buggy service or a person with psql would.
"""
import psycopg
import pytest
from psycopg import errors

from tests.conftest import balance


def _txn(conn, key="raw", currency="INR"):
    return conn.execute(
        "INSERT INTO transactions (idempotency_key, request_hash, currency) VALUES (%s, 'x', %s) RETURNING id",
        (key, currency),
    ).fetchone()[0]


def _entry(conn, txn, account, amount, currency="INR"):
    conn.execute(
        "INSERT INTO entries (transaction_id, account_id, currency, amount) VALUES (%s, %s, %s, %s)",
        (txn, account, currency, amount),
    )


def test_unbalanced_transaction_fails_at_commit(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    with pytest.raises(errors.CheckViolation, match="does not balance"):
        with conn.transaction():
            t = _txn(conn)
            _entry(conn, t, a, -500)
            _entry(conn, t, b, 400)  # 100 paise created from nothing
    assert balance(conn, a) == 1_000


def test_single_leg_transaction_fails(conn, make_account):
    a = make_account("alice", 1_000)
    with pytest.raises(errors.CheckViolation, match="needs at least 2"):
        with conn.transaction():
            _txn(conn)


def test_entries_cannot_be_edited(conn, make_account):
    make_account("alice", 1_000)
    with pytest.raises(errors.RestrictViolation, match="append-only"):
        conn.execute("UPDATE entries SET amount = amount * 2")


def test_entries_cannot_be_deleted(conn, make_account):
    make_account("alice", 1_000)
    with pytest.raises(errors.RestrictViolation, match="append-only"):
        conn.execute("DELETE FROM entries")


def test_transactions_cannot_be_deleted(conn, make_account):
    make_account("alice", 1_000)
    with pytest.raises(errors.RestrictViolation, match="append-only"):
        conn.execute("DELETE FROM transactions")


def test_balance_cannot_be_written_directly(conn, make_account):
    a = make_account("alice", 1_000)
    with pytest.raises(errors.CheckViolation, match="derived from entries"):
        conn.execute("UPDATE accounts SET balance = 1000000 WHERE id = %s", (a,))
    assert balance(conn, a) == 1_000


def test_account_cannot_open_with_a_balance(conn):
    with pytest.raises(errors.CheckViolation, match="open at balance 0"):
        conn.execute("INSERT INTO accounts (name, currency, balance) VALUES ('rich', 'INR', 500)")


def test_balance_floor_holds_without_the_app_check(conn, make_account):
    a = make_account("alice", 100)
    b = make_account("bob")
    with pytest.raises(errors.CheckViolation):
        with conn.transaction():
            t = _txn(conn)
            _entry(conn, t, a, -101)
            _entry(conn, t, b, 101)
    assert balance(conn, a) == 100


def test_entry_currency_must_match_account(conn, make_account):
    a = make_account("alice", 1_000)
    b = make_account("bob")
    with pytest.raises(errors.ForeignKeyViolation):
        with conn.transaction():
            t = _txn(conn, currency="USD")
            _entry(conn, t, a, -100, currency="USD")
            _entry(conn, t, b, 100, currency="USD")


def test_schema_is_rerunnable(conn):
    from ledger import db
    db.apply_schema(conn)
    db.apply_schema(conn)
