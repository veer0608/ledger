"""Test database setup.

Tests TRUNCATE every table, so they never read DATABASE_URL. They use
TEST_DATABASE_URL (set in CI) or a `ledger_test` database on the bundled
local Postgres.
"""
import os

import pytest

from ledger import core, db


@pytest.fixture(scope="session")
def db_url():
    return os.environ.get("TEST_DATABASE_URL") or db.local_url("ledger_test")


@pytest.fixture(scope="session")
def pool(db_url):
    p = db.make_pool(db_url, max_size=60)
    with p.connection() as conn:
        db.apply_schema(conn)
    yield p
    p.close()


@pytest.fixture(autouse=True)
def clean(pool):
    with pool.connection() as conn:
        conn.execute("TRUNCATE entries, transactions, accounts RESTART IDENTITY CASCADE")
    yield


@pytest.fixture
def conn(pool):
    with pool.connection() as c:
        yield c


@pytest.fixture
def make_account(conn):
    """Create an account, optionally funded from an INR mint that may go negative."""
    state = {"n": 0}

    def make(name: str, balance: int = 0, currency: str = "INR", allow_negative: bool = False) -> int:
        acct = core.create_account(conn, name, currency, allow_negative)["id"]
        if balance:
            mint_name = f"mint:{currency}"
            try:
                mint = core.account_id(conn, mint_name)
            except core.NotFound:
                mint = core.create_account(conn, mint_name, currency, allow_negative=True)["id"]
            state["n"] += 1
            core.transfer(conn, key=f"fund-{name}-{state['n']}", from_account=mint,
                          to_account=acct, amount=balance)
        return acct

    return make


def balance(conn, account_id: int) -> int:
    return core.get_account(conn, account_id)["balance"]
