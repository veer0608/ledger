# ledger

A double-entry ledger on Postgres: the part of a payments company that decides
whether money moved. Transfers are idempotent, balances stay correct under
concurrent load, history is append-only, a bank reference is credited at most
once, and a reconciliation job checks the ledger against a bank statement.

The rules that keep money correct live in the database, not only in Python.
They are claimed against **the role the service connects as** (`ledger_app`):
a bug in the service, or someone holding its credentials, cannot create money
or rewrite history. The schema owner is trusted, as a DBA always is; an owner
can disable any trigger.

## What it guarantees, and the test that proves each one

| Guarantee | How | Test |
|---|---|---|
| Every transaction sums to zero | deferred constraint trigger, checked at COMMIT | `test_db_guards::test_unbalanced_transaction_fails_at_commit` |
| A balance never drifts from its entries | balance is written only by a `SECURITY DEFINER` trigger on `entries`; the app role has no `UPDATE` on accounts | `test_app_role_is_refused[write a balance]` |
| No overdraft under a race | row locks (`SELECT ... FOR UPDATE`) plus a `CHECK (balance >= 0)` backstop | `test_concurrency::test_overdraft_race_cannot_go_negative` |
| A retried request never pays twice | idempotency key claimed with `INSERT ... ON CONFLICT` in the same transaction | `test_same_key_storm_moves_money_once` |
| Keys never collide between clients | `UNIQUE (client_id, idempotency_key)` | `test_identical_requests_from_two_clients_both_run` |
| A bank reference (UTR) is credited at most once | trigger + advisory lock per reference; allowed again only after the first posting is reversed | `test_same_utr_race_across_unrelated_accounts` |
| Opposite transfers do not deadlock | accounts locked in ascending id order | `test_opposing_transfers_do_not_deadlock` |
| A transaction is reversed at most once | `UNIQUE (reverses_id)` | `test_double_reversal_race` |
| History cannot be edited or wiped | no `UPDATE`/`DELETE`/`TRUNCATE` grant; row triggers also stop the owner's edits | `test_app_role_is_refused[truncate history]` |
| The app cannot make a mint | column-level `INSERT` grant excludes `allow_negative` | `test_app_role_is_refused[open a mint]` |
| No currency mixing | composite foreign keys tie each entry's currency to its account and transaction | `test_entry_currency_must_match_account` |

Amounts are `bigint` paise everywhere. There is no float in the schema, the
code, or the API (the API rejects `1.5`, `1.0`, `"100"` and `true`).

## Flaws found in the first version, and what fixed them

The first version passed all 72 of its tests and still had these. Each was
found by attacking it, then fixed, then the attack was rerun:

| Flaw | Attack that worked | Fix |
|---|---|---|
| The "database guarantees" were bypassable: the app connected as a superuser that owned the tables | `UPDATE accounts SET allow_negative = true` then overdraw; `ALTER TABLE ... DISABLE TRIGGER`; `TRUNCATE entries`; `SET session_replication_role = replica` | separate owner and `ledger_app` roles; the app gets `SELECT`, column-level `INSERT` and two `SECURITY DEFINER` functions, nothing else |
| One UTR could be credited twice | webhook redelivered under a fresh idempotency key: alice credited 500 twice | one live posting per `external_ref`, enforced in the database |
| Reconciliation could report "all clean" while hiding a missing deposit | the period was inferred from the first and last bank lines, so a ledger entry dated before the first one was silently dropped, exit code 0 | `--from` / `--to` are required; a statement line outside the period is an error |
| Idempotency keys were global | two shops both sending `checkout-1`: the second shop's identical payment got a "success" replay and never ran | keys are scoped per `Client-Id` |
| A NUL byte in `memo` was a 500 | `"memo": "a\u0000b"` | control characters rejected with a 400 |
| Balance overflow was a 500 | about 9,300 maximum-size deposits into one account | mapped to a 400 |

## The race tests can fail

A concurrency test that passes proves little unless it fails when the
protection is removed. Each one was checked by breaking the code on purpose:

| Change | Result |
|---|---|
| Lock accounts in sender-then-receiver order instead of id order | 49 of 50 opposing transfers die with `DeadlockDetected` |
| Remove `FOR UPDATE` | the opposing-transfer and stress tests fail with `DeadlockDetected` (the balance trigger's row updates now take locks in leg order) |
| Remove `ON CONFLICT` from the key claim | 20 of 30 identical retries crash with `UniqueViolation` |
| Remove the advisory lock from the reference rule | 13 to 16 of 20 racers credit the same UTR (three runs) |
| Remove the replay check on a duplicate reference | 19 of 20 identical retries are refused instead of replayed |
| Run the guard tests as the owner instead of `ledger_app` | 16 of 24 fail: the role is doing the work |
| Remove the application's funds check | **all tests still pass**: the database `CHECK` stops the overdraft by itself |

One of these needed a second test. The first duplicate-UTR race sent all 20
deposits between the same two accounts. The account row locks serialised them
before the reference rule was ever reached, so removing the advisory lock
changed nothing. `test_same_utr_race_across_unrelated_accounts` gives each
racer its own accounts, so only the reference lock stands between them.

The last row is the point of putting the rules in the database. The
application check exists for a clear error message. It is not what keeps the
balance non-negative.

The tests run real concurrent sessions: each thread takes its own connection
and all of them are released at once from a barrier.

## Reconciliation

```powershell
.venv\Scripts\python -m ledger.recon statement.csv --account bank:hdfc --from 2026-11-01 --to 2026-11-30
```

It matches a bank statement against the ledger's settlement account in two passes:

1. **By reference** (UTR, RRN): same reference and amount is `matched`; same
   reference, different amount is `amount_mismatch`.
2. **By amount within a date window** (default 2 days), closest date first,
   one to one. Only when at least one side has no reference, so two payments
   with different UTRs are never paired because their amounts agree.

What is left is `bank_only` (the money moved and the ledger never heard, such
as a dropped webhook) or `ledger_only` (recorded, not yet at the bank). Every
ledger entry dated inside the period is reported. The command writes
`recon_report.csv` and exits non-zero unless everything matched, so it can
gate a nightly job.

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
users, a fee and its reversal, withdrawals, a retried deposit that replays, a
redelivered webhook that is refused) and reconciles it against a statement
with problems planted in it:

```
app role refused a mint: only the database owner can create an account that may go negative
redelivered webhook refused: external_ref UTR001 is already posted as transaction 1
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

### Deploying

Two roles, two URLs:

```powershell
$env:DATABASE_URL = "postgresql://owner@db-host/ledger"
.venv\Scripts\python -m ledger.migrate --system-account bank:hdfc INR
```

That applies the schema as the owner, gives `ledger_app` a login (password
from `LEDGER_APP_PASSWORD`), and creates the settlement account, which only
the owner can do. The service then runs with `APP_DATABASE_URL` pointing at
`ledger_app` and never sees the owner's credentials. Locally, with neither
set, all of this happens automatically against the bundled Postgres.

### API

`.venv\Scripts\uvicorn ledger.api:app`, then open `/docs`.

| Endpoint | |
|---|---|
| `POST /accounts` | create an account (never one that may go negative) |
| `GET /accounts/{id}` | balance |
| `GET /accounts/{id}/statement` | entries with running balance |
| `POST /transfers` | requires `Idempotency-Key` and `Client-Id`; 201 when new, 200 plus `Idempotent-Replayed: true` on a retry, 409 for a reused key or an already-posted `external_ref` |
| `POST /transactions/{id}/reverse` | requires `Idempotency-Key` and `Client-Id` |
| `GET /health` | full invariant audit; 500 if anything disagrees |

## Decisions

- **A failed transfer does not use up its key.** If a transfer is refused for
  insufficient funds, the same key can be retried once the account is funded.
  Stripe stores the failure against the key instead. Either is defensible;
  this one is simpler and a test pins it.
- **The key is claimed before the funds check.** A retry of a transfer that
  succeeded must replay. It must not fail for lack of the money it already moved.
- **A duplicate UTR is refused, not replayed.** A redelivered webhook with a
  new key gets a 409 naming the transaction that already holds the reference.
  Treating it as a replay would hide a client that is minting fresh keys for
  retries.
- **External references are global**, not per settlement account. A UTR is
  meant to identify one payment; if two banks ever produced the same string,
  this would refuse the second for a human to look at, which is the safe
  direction.
- **Corrections are reversals.** A reversal is a new transaction that mirrors
  the original. It needs the money to still be there, as a real refund does.
- **IST is hard-coded for statement dates.** It is a fixed +05:30 with no DST,
  so no timezone database is needed (the bundled Postgres has none).

## Known limits

Measured or seen, not fixed:

- **A busy settlement account is a bottleneck.** Every deposit locks the one
  `bank:hdfc` row: 703 transfers/s through it against 1,106 spread across
  accounts (local, 20 threads). The limit is how long that lock is held, so a
  network round trip per statement lowers it further. The usual fixes are
  splitting the settlement account or not locking accounts that may go negative.
- **`Client-Id` is trusted as sent.** It keeps honest clients' keys apart. It
  does not stop one client claiming to be another; that is the auth layer's job.
- **The schema is re-applied on local startup**, which briefly locks tables.
  In a deployment, `ledger.migrate` runs once and the service only connects.
- **The statement endpoint rescans the account's history** to compute running
  balances on every call.

## Layout

```
ledger/schema.sql   tables, triggers, roles and grants, the drift view
ledger/core.py      transfer, reverse, statement, invariant audit
ledger/api.py       FastAPI
ledger/recon.py     statement parser and two-pass matcher
ledger/migrate.py   owner-side setup and system accounts
ledger/demo.py      end-to-end run
tests/              106 tests: core, raw-SQL attacks as the app role, races, API, reconciliation
```
