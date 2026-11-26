"""The database must refuse bad money even when the application is bypassed.

Each test writes raw SQL, as a buggy service or someone holding the service's
credentials would. The first group runs as ledger_app, the role the service
uses: that is who the guarantees are claimed against. The last group runs as
the owner, to show the row-level triggers hold there too, short of the owner
disabling them (which an owner can always do).
"""
import pytest
from psycopg import errors

from ledger import core, db
from tests.conftest import balance


def _txn(conn, key="raw", currency="INR"):
    return conn.execute(
        """INSERT INTO transactions (client_id, idempotency_key, request_hash, currency)
           VALUES ('raw', %s, 'x', %s) RETURNING id""",
        (key, currency),
    ).fetchone()[0]


def _entry(conn, txn, account, amount, currency="INR"):
    conn.execute(
        "INSERT INTO entries (transaction_id, account_id, currency, amount) VALUES (%s, %s, %s, %s)",
        (txn, account, currency, amount),
    )


# ------------------------------------------- what the app role CAN write

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
    make_account("alice", 1_000)
    with pytest.raises(errors.CheckViolation, match="needs at least 2"):
        with conn.transaction():
            _txn(conn)


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


def test_raw_insert_of_a_live_reference_twice_is_refused(conn, make_account):
    make_account("alice", 1_000)
    mint = core.account_id(conn, "mint:INR")
    a = core.account_id(conn, "alice")
    with conn.transaction():
        t = conn.execute("""INSERT INTO transactions (client_id, idempotency_key, request_hash, currency, external_ref)
                            VALUES ('raw', 'k1', 'x', 'INR', 'UTR9') RETURNING id""").fetchone()[0]
        _entry(conn, t, mint, -5)
        _entry(conn, t, a, 5)
    with pytest.raises(errors.UniqueViolation, match="already posted"):
        conn.execute("""INSERT INTO transactions (client_id, idempotency_key, request_hash, currency, external_ref)
                        VALUES ('raw', 'k2', 'x', 'INR', 'UTR9')""")


# ----------------------------------------- what the app role CANNOT do
# Every one of these worked before the role split, as the superuser.

APP_ROLE_REFUSED = {
    "edit an entry": "UPDATE entries SET amount = amount * 2",
    "delete entries": "DELETE FROM entries",
    "delete transactions": "DELETE FROM transactions",
    "truncate history": "TRUNCATE entries",
    "write a balance": "UPDATE accounts SET balance = 1000000",
    "turn an account into a mint": "UPDATE accounts SET allow_negative = true",
    "open an account with a balance": "INSERT INTO accounts (name, currency, balance) VALUES ('rich', 'INR', 500)",
    "open a mint": "INSERT INTO accounts (name, currency, allow_negative) VALUES ('m', 'INR', true)",
    "backdate an entry": "INSERT INTO entries (transaction_id, account_id, currency, amount, created_at) "
                         "VALUES (1, 1, 'INR', 1, '2020-01-01')",
    "disable triggers": "ALTER TABLE accounts DISABLE TRIGGER USER",
    "drop a guard": "DROP TRIGGER entries_balanced ON entries",
    "switch off all triggers": "SET session_replication_role = replica",
    "create a function in public": "CREATE FUNCTION public.f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
}


@pytest.mark.parametrize("statement", APP_ROLE_REFUSED.values(), ids=APP_ROLE_REFUSED.keys())
def test_app_role_is_refused(conn, make_account, statement):
    a = make_account("alice", 1_000)
    with pytest.raises(errors.InsufficientPrivilege):
        conn.execute(statement)
    assert balance(conn, a) == 1_000
    assert core.check_invariants(conn)["ok"]


def test_app_role_is_not_superuser(conn):
    user, superuser = conn.execute(
        "SELECT current_user, rolsuper FROM pg_roles WHERE rolname = current_user").fetchone()
    assert (user, superuser) == (db.APP_ROLE, False)


# --------------------------------- row-level guards, even for the owner

def test_owner_cannot_edit_entries(owner_conn, make_account):
    make_account("alice", 1_000)
    with pytest.raises(errors.RestrictViolation, match="append-only"):
        owner_conn.execute("UPDATE entries SET amount = amount * 2")


def test_owner_cannot_delete_transactions(owner_conn, make_account):
    make_account("alice", 1_000)
    with pytest.raises(errors.RestrictViolation, match="append-only"):
        owner_conn.execute("DELETE FROM transactions")


def test_owner_cannot_write_a_balance(owner_conn, make_account):
    a = make_account("alice", 1_000)
    with pytest.raises(errors.CheckViolation, match="derived from entries"):
        owner_conn.execute("UPDATE accounts SET balance = 1000000 WHERE id = %s", (a,))


def test_owner_cannot_open_an_account_with_a_balance(owner_conn):
    with pytest.raises(errors.CheckViolation, match="open at balance 0"):
        owner_conn.execute("INSERT INTO accounts (name, currency, balance) VALUES ('rich', 'INR', 500)")


def test_schema_is_rerunnable(owner_url):
    db.migrate(owner_url)
    db.migrate(owner_url)
