"""Ledger operations.

Every money movement is a transaction of two or more entries that sum to zero.
Amounts are integers in minor units (paise for INR). A positive balance is
what an account holds; a transfer is a negative entry on the sender and a
positive entry on the receiver.

Concurrency, in one paragraph: a posting locks every account it touches with
SELECT ... FOR UPDATE in ascending id order (via ledger_lock_accounts), so two
transfers between the same pair of accounts in opposite directions queue
instead of deadlocking. The balance check happens after the lock, so two
withdrawals cannot both see the same old balance. The idempotency key is
claimed by INSERT ... ON CONFLICT inside the same transaction, so a retried
request either waits for the first attempt and replays its result, or (if
the first attempt rolled back) runs.

Idempotency keys are scoped per client: (client_id, key) is unique, not key
alone. In a deployment client_id comes from the auth layer in front of this.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass

import psycopg
from psycopg import errors
from psycopg.rows import dict_row

MAX_AMOUNT = 10**15  # ten trillion rupees in paise; far under bigint, far over anything real
CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


class LedgerError(Exception):
    status = 400
    code = "bad_request"


class Forbidden(LedgerError):
    status = 403
    code = "forbidden"


class NotFound(LedgerError):
    status = 404
    code = "not_found"


class Conflict(LedgerError):
    status = 409
    code = "conflict"


class IdempotencyConflict(Conflict):
    code = "idempotency_conflict"


class AlreadyReversed(Conflict):
    code = "already_reversed"


class DuplicateReference(Conflict):
    code = "duplicate_external_ref"


class InsufficientFunds(LedgerError):
    status = 422
    code = "insufficient_funds"


class CurrencyMismatch(LedgerError):
    status = 422
    code = "currency_mismatch"


@dataclass(frozen=True)
class Posted:
    transaction_id: int
    replayed: bool  # True when this client had already used this key for the same request


def _cur(conn: psycopg.Connection) -> psycopg.Cursor:
    return conn.cursor(row_factory=dict_row)


def _request_hash(payload: dict) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()


def _check_text(value: object, what: str, max_len: int, required: bool) -> None:
    if value is None and not required:
        return
    if not isinstance(value, str) or not value.strip() or len(value) > max_len:
        raise LedgerError(f"{what} must be 1 to {max_len} characters")
    # Postgres text cannot hold NUL; other control characters have no business in a key or reference.
    if any(ord(ch) < 32 and ch not in "\t" for ch in value):
        raise LedgerError(f"{what} contains a control character")


def _check_amount(amount: object) -> None:
    # bool is an int subclass; True must not mean one paisa.
    if isinstance(amount, bool) or not isinstance(amount, int):
        raise LedgerError("amount must be an integer number of minor units")
    if not 0 < amount <= MAX_AMOUNT:
        raise LedgerError(f"amount must be between 1 and {MAX_AMOUNT}")


# ---------------------------------------------------------------- accounts

def create_account(conn, name: str, currency: str, allow_negative: bool = False) -> dict:
    """allow_negative accounts (mints, settlement accounts) need the owner role."""
    _check_text(name, "account name", 200, required=True)
    if not CURRENCY_RE.match(currency or ""):
        raise LedgerError("currency must be a 3-letter ISO code such as INR")
    # The app role has no INSERT grant on allow_negative, so the column is only
    # named when it is actually being set.
    cols, vals = ["name", "currency"], [name, currency]
    if allow_negative:
        cols.append("allow_negative")
        vals.append(True)
    query = f"""INSERT INTO accounts ({", ".join(cols)}) VALUES ({", ".join(["%s"] * len(cols))})
                RETURNING id, name, currency, allow_negative, balance, created_at"""
    try:
        return _cur(conn).execute(query, vals).fetchone()
    except errors.UniqueViolation:
        raise Conflict(f"account {name!r} already exists") from None
    except errors.InsufficientPrivilege:
        raise Forbidden("only the database owner can create an account that may go negative") from None


def get_account(conn, account_id: int) -> dict:
    row = _cur(conn).execute(
        "SELECT id, name, currency, allow_negative, balance, created_at FROM accounts WHERE id = %s",
        (account_id,),
    ).fetchone()
    if row is None:
        raise NotFound(f"account {account_id} not found")
    return row


def account_id(conn, name: str) -> int:
    row = conn.execute("SELECT id FROM accounts WHERE name = %s", (name,)).fetchone()
    if row is None:
        raise NotFound(f"account {name!r} not found")
    return row[0]


def statement(conn, account_id: int, limit: int = 100) -> dict:
    """Latest entries on an account, newest first, each with the balance after it."""
    account = get_account(conn, account_id)
    rows = _cur(conn).execute(
        """SELECT * FROM (
               SELECT e.id AS entry_id, e.transaction_id, e.amount, t.memo, t.external_ref,
                      e.created_at,
                      sum(e.amount) OVER (ORDER BY e.id) AS balance_after
               FROM entries e
               JOIN transactions t ON t.id = e.transaction_id
               WHERE e.account_id = %s
           ) s
           ORDER BY entry_id DESC
           LIMIT %s""",
        (account_id, limit),
    ).fetchall()
    return {"account": account, "entries": rows}


# ------------------------------------------------------------ transactions

def get_transaction(conn, transaction_id: int) -> dict:
    cur = _cur(conn)
    txn = cur.execute(
        """SELECT id, client_id, idempotency_key, currency, memo, external_ref, reverses_id, created_at
           FROM transactions WHERE id = %s""",
        (transaction_id,),
    ).fetchone()
    if txn is None:
        raise NotFound(f"transaction {transaction_id} not found")
    txn["entries"] = cur.execute(
        "SELECT account_id, amount FROM entries WHERE transaction_id = %s ORDER BY id",
        (transaction_id,),
    ).fetchall()
    return txn


def transfer(conn, *, client: str, key: str, from_account: int, to_account: int, amount: int,
             memo: str | None = None, external_ref: str | None = None) -> Posted:
    _check_text(client, "client id", 100, required=True)
    _check_text(key, "idempotency key", 255, required=True)
    _check_text(memo, "memo", 500, required=False)
    _check_text(external_ref, "external_ref", 200, required=False)
    _check_amount(amount)
    if from_account == to_account:
        raise LedgerError("cannot transfer an account to itself")
    payload = {"op": "transfer", "from": from_account, "to": to_account, "amount": amount,
               "memo": memo, "external_ref": external_ref}
    return _post(conn, client=client, key=key, request_hash=_request_hash(payload),
                 legs=[(from_account, -amount), (to_account, amount)],
                 memo=memo, external_ref=external_ref)


def reverse(conn, *, client: str, key: str, transaction_id: int) -> Posted:
    """Post the mirror image of a transaction. History is never edited."""
    _check_text(client, "client id", 100, required=True)
    _check_text(key, "idempotency key", 255, required=True)
    request_hash = _request_hash({"op": "reverse", "transaction_id": transaction_id})
    replay = _replay(conn, client, key, request_hash)
    if replay:
        return replay
    original = get_transaction(conn, transaction_id)
    if original["reverses_id"] is not None:
        raise LedgerError("cannot reverse a reversal; post a new transfer instead")
    legs = [(e["account_id"], -e["amount"]) for e in original["entries"]]
    return _post(conn, client=client, key=key, request_hash=request_hash, legs=legs,
                 memo=f"reversal of transaction {transaction_id}",
                 external_ref=original["external_ref"], reverses_id=transaction_id)


def _replay(conn, client: str, key: str, request_hash: str) -> Posted | None:
    row = conn.execute(
        "SELECT id, request_hash FROM transactions WHERE client_id = %s AND idempotency_key = %s",
        (client, key),
    ).fetchone()
    if row is None:
        return None
    if row[1] != request_hash:
        raise IdempotencyConflict(f"idempotency key {key!r} was already used for a different request")
    return Posted(row[0], replayed=True)


def _post(conn, *, client: str, key: str, request_hash: str, legs: list[tuple[int, int]],
          memo: str | None, external_ref: str | None, reverses_id: int | None = None) -> Posted:
    # Fast path for the common retry: no locks needed to replay.
    replay = _replay(conn, client, key, request_hash)
    if replay:
        return replay

    ids = sorted({acct for acct, _ in legs})
    try:
        with conn.transaction():
            cur = _cur(conn)
            accounts = {r["id"]: r for r in cur.execute(
                "SELECT id, currency, balance, allow_negative FROM ledger_lock_accounts(%s)", (ids,))}
            missing = sorted(set(ids) - accounts.keys())
            if missing:
                raise NotFound(f"account {missing[0]} not found")
            currencies = {a["currency"] for a in accounts.values()}
            if len(currencies) != 1:
                raise CurrencyMismatch(f"accounts are in different currencies: {sorted(currencies)}")
            currency = currencies.pop()

            # Claim the key before checking funds: a retry of a transfer that
            # already succeeded must replay, not fail for lack of the money it moved.
            claimed = cur.execute(
                """INSERT INTO transactions
                       (client_id, idempotency_key, request_hash, currency, memo, external_ref, reverses_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (client_id, idempotency_key) DO NOTHING
                   RETURNING id""",
                (client, key, request_hash, currency, memo, external_ref, reverses_id),
            ).fetchone()
            if claimed is None:
                # A concurrent request with this key committed while we waited.
                replay = _replay(conn, client, key, request_hash)
                if replay is None:  # pragma: no cover - would mean the key vanished
                    raise RuntimeError(f"idempotency key {key!r} conflicted but no row is visible")
                return replay
            txn_id = claimed["id"]

            net: dict[int, int] = defaultdict(int)
            for acct, amount in legs:
                net[acct] += amount
            for acct, delta in sorted(net.items()):
                a = accounts[acct]
                if not a["allow_negative"] and a["balance"] + delta < 0:
                    raise InsufficientFunds(
                        f"account {acct} holds {a['balance']} and cannot pay {-delta}")

            cur.executemany(
                "INSERT INTO entries (transaction_id, account_id, currency, amount) VALUES (%s, %s, %s, %s)",
                [(txn_id, acct, currency, amount) for acct, amount in legs],
            )
    except errors.UniqueViolation as exc:
        name = exc.diag.constraint_name
        if name == "transactions_reverses_id_key":
            raise AlreadyReversed(f"transaction {reverses_id} has already been reversed") from None
        if name == "transactions_external_ref_live":
            # The same request retried at the same instant also lands here: it
            # waited on the reference lock, not the key. Replay it if so.
            replay = _replay(conn, client, key, request_hash)
            if replay:
                return replay
            raise DuplicateReference(exc.diag.message_primary) from None
        raise
    except errors.CheckViolation as exc:
        # Backstop: the explicit check above should always fire first.
        if exc.diag.constraint_name == "balance_floor":
            raise InsufficientFunds("balance would go below zero") from None
        raise
    except errors.NumericValueOutOfRange:
        raise LedgerError("balance would exceed the largest representable amount") from None
    return Posted(txn_id, replayed=False)


# -------------------------------------------------------------- invariants

def check_invariants(conn) -> dict:
    """Audit the whole ledger. Every list should be empty and `ok` true."""
    cur = _cur(conn)
    drift = cur.execute("SELECT * FROM ledger_drift ORDER BY id").fetchall()
    unbalanced = cur.execute(
        """SELECT transaction_id, sum(amount) AS total FROM entries
           GROUP BY transaction_id HAVING sum(amount) <> 0 ORDER BY transaction_id"""
    ).fetchall()
    currency_totals = cur.execute(
        "SELECT currency, sum(amount) AS total FROM entries GROUP BY currency ORDER BY currency"
    ).fetchall()
    nonzero = [c for c in currency_totals if c["total"] != 0]
    return {
        "ok": not drift and not unbalanced and not nonzero,
        "balance_drift": drift,
        "unbalanced_transactions": unbalanced,
        "nonzero_currency_totals": nonzero,
    }
