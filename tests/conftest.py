"""Test database setup.

Tests TRUNCATE every table, so they never read DATABASE_URL. They use
TEST_DATABASE_URL (set in CI) or a `ledger_test` database on the bundled
local Postgres, as the owner for setup, and as ledger_app for everything the
service would do. Running as the app role is the point: the guarantees are
claimed against that role.
"""
import os

import pytest

from ledger import core, db

CLIENT = "test-client"


@pytest.fixture(scope="session")
def owner_url():
    url = os.environ.get("TEST_DATABASE_URL") or db.local_url("ledger_test")
    db.migrate(url)
    return url


@pytest.fixture(scope="session")
def owner_pool(owner_url):
    p = db.make_pool(owner_url, max_size=4)
    yield p
    p.close()


@pytest.fixture(scope="session")
def pool(owner_url):
    p = db.make_pool(db.as_app_role(owner_url), max_size=60)
    yield p
    p.close()


@pytest.fixture(autouse=True)
def clean(owner_pool):
    with owner_pool.connection() as conn:
        conn.execute("TRUNCATE ledger_closes, outbox, entries, transactions, accounts RESTART IDENTITY CASCADE")
    yield


@pytest.fixture
def conn(pool):
    """A connection as ledger_app."""
    with pool.connection() as c:
        yield c


@pytest.fixture
def owner_conn(owner_pool):
    with owner_pool.connection() as c:
        yield c


@pytest.fixture
def make_account(conn, owner_conn):
    """Create an account, optionally funded from a per-currency mint.

    Accounts that may go negative (mints, settlement accounts) are created by
    the owner, as they would be in a deployment; the rest by the app role.
    """
    state = {"n": 0}

    def make(name: str, balance: int = 0, currency: str = "INR", allow_negative: bool = False) -> int:
        creator = owner_conn if allow_negative else conn
        acct = core.create_account(creator, name, currency, allow_negative)["id"]
        if balance:
            mint_name = f"mint:{currency}"
            try:
                mint = core.account_id(conn, mint_name)
            except core.NotFound:
                mint = core.create_account(owner_conn, mint_name, currency, allow_negative=True)["id"]
            state["n"] += 1
            core.transfer(conn, client=CLIENT, key=f"fund-{name}-{state['n']}", from_account=mint,
                          to_account=acct, amount=balance)
        return acct

    return make


def balance(conn, account_id: int) -> int:
    return core.get_account(conn, account_id)["balance"]
