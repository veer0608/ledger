# ledger

A double-entry ledger on Postgres: the part of a payments company that decides
whether money moved. Transfers are idempotent, balances stay correct under
concurrent load, history is append-only, and a reconciliation job checks the
ledger against a bank statement.

The rules that keep money correct live in the database, not only in Python.
A buggy service, or a person with `psql`, still cannot create money.

## What it guarantees, and the test that proves each one

| Guarantee | How | Test |
|---|---|---|
| Every transaction sums to zero | deferred constraint trigger, checked at COMMIT | `test_db_guards::test_unbalanced_transaction_fails_at_commit` |
| A balance never drifts from its entries | balance is written only by a trigger on `entries`; direct `UPDATE` is refused | `test_balance_cannot_be_written_directly` |
| No overdraft under a race | row locks (`SELECT ... FOR UPDATE`) plus a `CHECK (balance >= 0)` backstop | `test_concurrency::test_overdraft_race_cannot_go_negative` |
| A retried request never pays twice | idempotency key claimed with `INSERT ... ON CONFLICT` in the same transaction | `test_same_key_storm_moves_money_once` |
| Opposite transfers do not deadlock | accounts locked in ascending id order | `test_opposing_transfers_do_not_deadlock` |
| A transaction is reversed at most once | `UNIQUE (reverses_id)` | `test_double_reversal_race` |
| History cannot be edited | `UPDATE`/`DELETE` on entries and transactions raise | `test_entries_cannot_be_edited` |
| No currency mixing | composite foreign keys tie each entry's currency to its account and transaction | `test_entry_currency_must_match_account` |

Amounts are `bigint` paise everywhere. There is no float in the schema, the
code, or the API (the API rejects `1.5`, `1.0`, `"100"` and `true`).

## The race tests can fail

A concurrency test that passes proves little unless it fails when the
protection is removed. Each one was checked by breaking `core.py` on purpose:

| Change to `core.py` | Result |
|---|---|
| Lock accounts in sender-then-receiver order instead of id order | 49 of 50 opposing transfers die with `DeadlockDetected` |
| Remove `FOR UPDATE` | the opposing-transfer and stress tests fail with `DeadlockDetected` (the balance trigger's row updates now take locks in leg order) |
| Remove `ON CONFLICT` from the key claim | 20 of 30 identical retries crash with `UniqueViolation` |
| Remove the application's funds check | **all tests still pass**: the database `CHECK` stops the overdraft by itself |

The last row is the point of putting the rules in the database. The
application check exists for a clear error message. It is not what keeps the
balance non-negative.

The tests run real concurrent sessions: each thread takes its own connection
and all of them are released at once from a barrier.

## Reconciliation

`python -m ledger.recon statement.csv --account bank:hdfc` matches a bank
statement against the ledger's settlement account in two passes:

1. **By reference** (UTR, RRN): same reference and amount is `matched`; same
   reference, different amount is `amount_mismatch`.
2. **By amount within a date window** (default 2 days), closest date first,
   one to one. Only when at least one side has no reference, so two payments
   with different UTRs are never paired because their amounts agree.

What is left is `bank_only` (the money moved and the ledger never heard, such
as a dropped webhook) or `ledger_only` (recorded, not yet at the bank). The
command writes `recon_report.csv` and exits non-zero unless everything matched,
so it can gate a nightly job.

## Run it

Needs Python 3.11+. With no `DATABASE_URL` set, it starts a bundled Postgres 16
from the `pgserver` package under `.pgdata/`, so nothing has to be installed.

```powershell
python -m venv .venv
.venv\Scripts\pip install -r requirements-dev.txt
.venv\Scripts\python -m pytest
.venv\Scripts\python -m ledger.demo
```

The demo runs a day of a small wallet business (deposits, a payment between
users, a fee and its reversal, withdrawals, a retried deposit that replays)
and reconciles it against a statement with problems planted in it:

```
balances
  bank:hdfc        -6150.00
  wallet:asha       4850.00
  wallet:ravi       1300.00
  revenue:fees         0.00
invariants ok: True

reconciliation of bank:hdfc
  matched            3
  matched_by_amount  1
  amount_mismatch    1
  bank_only          1
  ledger_only        1
  ! amount_mismatch: statement line 5, 745.00, UTR004 (bank 745.00 vs ledger 750.00, difference -5.00)
  ! bank_only: statement line 7, 1000.00, UTR006
  ! ledger_only: ledger txn 8, -400.00, UTR005
```

`bank:hdfc` is negative because it is the outside world's side of the bank
account: money that came in through the bank is a transfer out of it. Its
balance is minus what the bank should be holding for the wallets: 6,150.00,
the sum of the three wallet balances.

The API: `.venv\Scripts\uvicorn ledger.api:app`, then open `/docs`.

| Endpoint | |
|---|---|
| `POST /accounts` | create an account |
| `GET /accounts/{id}` | balance |
| `GET /accounts/{id}/statement` | entries with running balance |
| `POST /transfers` | requires an `Idempotency-Key` header; 201 when new, 200 plus `Idempotent-Replayed: true` on a retry |
| `POST /transactions/{id}/reverse` | requires `Idempotency-Key` |
| `GET /health` | full invariant audit; 500 if anything disagrees |

## Decisions

- **A failed transfer does not use up its key.** If a transfer is refused for
  insufficient funds, the same key can be retried once the account is funded.
  Stripe stores the failure against the key instead. Either is defensible;
  this one is simpler and a test pins it.
- **The key is claimed before the funds check.** A retry of a transfer that
  succeeded must replay. It must not fail for lack of the money it already moved.
- **Corrections are reversals.** A reversal is a new transaction that mirrors
  the original. It needs the money to still be there, as a real refund does.
- **No auth.** This is the ledger core, meant to sit behind a service that
  owns identity. Anyone who can call `POST /accounts` can make an
  `allow_negative` account, which prints money. Do not expose it directly.
- **IST is hard-coded for statement dates.** It is a fixed +05:30 with no DST,
  so no timezone database is needed.

## Layout

```
ledger/schema.sql   tables, triggers, the drift view
ledger/core.py      transfer, reverse, statement, invariant audit
ledger/api.py       FastAPI
ledger/recon.py     statement parser and two-pass matcher
ledger/demo.py      end-to-end run
tests/              72 tests: core, raw-SQL attacks on the schema, races, API, reconciliation
```
