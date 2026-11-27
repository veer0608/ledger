# ledger

A double-entry ledger on Postgres: the part of a payments company that decides
whether money moved. Transfers are idempotent, balances stay correct under
concurrent load (including on the one account every payment touches),
history is append-only, a bank reference is credited at most once, money can be held and captured later the way a card authorisation works,
every change is published as a signed webhook event that cannot be lost or
invented, the books close daily into snapshots that answer "what was the
balance then", and a reconciliation job checks the ledger against a bank
statement.

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
| No overdraft under a race | row locks (`SELECT ... FOR NO KEY UPDATE`) plus a `CHECK (balance >= 0)` backstop | `test_concurrency::test_overdraft_race_cannot_go_negative` |
| A retried request never pays twice | idempotency key claimed with `INSERT ... ON CONFLICT` in the same transaction | `test_same_key_storm_moves_money_once` |
| Keys never collide between clients | `UNIQUE (client_id, idempotency_key)` | `test_identical_requests_from_two_clients_both_run` |
| A bank reference (UTR) is credited at most once | trigger + advisory lock per reference; allowed again only after the first posting is reversed | `test_same_utr_race_across_unrelated_accounts` |
| Opposite transfers do not deadlock | accounts locked in ascending id order | `test_opposing_transfers_do_not_deadlock` |
| A transaction is reversed at most once | `UNIQUE (reverses_id)` | `test_double_reversal_race` |
| History cannot be edited or wiped | no `UPDATE`/`DELETE`/`TRUNCATE` grant; row triggers also stop the owner's edits | `test_app_role_is_refused[truncate history]` |
| The app cannot make a mint | column-level `INSERT` grant excludes `allow_negative` | `test_app_role_is_refused[open a mint]` |
| Entries are written with their transaction, never added to it later | composite foreign key on `(transaction_id, currency, created_at)` | `test_cannot_append_entries_to_an_old_transaction` |
| No currency mixing | composite foreign keys tie each entry's currency to its account and transaction | `test_entry_currency_must_match_account` |
| Money on hold cannot be spent | the balance floor is `balance - held >= 0`; `held` is derived from pending holds by trigger | `test_holds::test_holds_and_transfers_race_for_the_same_money` |
| A hold resolves exactly once | guard trigger allows only pending to captured, voided or expired | `test_capture_versus_void_race` |
| A capture pays exactly what it says to the hold's payee | deferred trigger checks the capture transaction's entries at COMMIT | `test_capture_without_paying_the_payee_is_refused` |
| An expired hold cannot be captured | the guard trigger compares `expires_at` to the database clock | `test_expired_hold_cannot_be_captured_and_is_released_by_the_job` |
| A busy merchant's receive buffer pays only that merchant | deferred trigger at COMMIT: a debit from a buffer must be exactly a sweep into its merchant; only the owner creates buffers | `test_buffers::test_the_database_refuses_a_buffer_debit_the_app_did_not_check` |
| A closed period's snapshot includes every posting stamped inside it | postings hold a shared lock to COMMIT; a close takes it exclusively and waits; a posting that started before the close and arrives after is refused | `test_periods::test_a_close_waits_for_a_posting_already_in_flight` |
| Snapshots and closes cannot be forged, edited or undone | written only by a `SECURITY DEFINER` function; append-only triggers, even for the owner | `test_app_role_cannot_forge_or_undo_a_close` |
| Every committed posting and hold change has exactly one event, and a rolled-back one has none | a trigger writes the event in the same database transaction; `UNIQUE (event_type, subject_id)` | `test_outbox::test_a_rolled_back_transfer_writes_no_event` |
| The app cannot forge, rewrite or delete an event | no `INSERT` grant on the outbox; a guard trigger freezes content, even for the owner | `test_app_role_cannot_tamper_with_events` |
| Relays running together do not send an event twice at once | claim with `FOR UPDATE SKIP LOCKED`, then a lease | `test_relays_running_together_deliver_each_event_once` |

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
| Money could move inside an old transaction (found later, while designing period close) | as the app role, append a -700 and a +700 entry to a transaction committed earlier: the sum stays zero, so the balance check passed, and 700 moved with no new posting, no idempotency key and no event, while the audit still said ok | an entry must carry its transaction's `created_at` through a composite foreign key; `now()` is fixed for a database transaction and the app cannot set `created_at`, so only the transaction that wrote the row can add entries to it. On a database where it already happened, the upgrade fails and names the entry |

## The race tests can fail

A concurrency test that passes proves little unless it fails when the
protection is removed. Each one was checked by breaking the code on purpose:

| Change | Result |
|---|---|
| Lock accounts in sender-then-receiver order instead of id order | 49 of 50 opposing transfers die with `DeadlockDetected` |
| Remove the row lock | the opposing-transfer and stress tests fail with `DeadlockDetected` (the balance trigger's row updates now take locks in leg order) |
| Remove `ON CONFLICT` from the key claim | 20 of 30 identical retries crash with `UniqueViolation` |
| Remove the advisory lock from the reference rule | 13 to 16 of 20 racers credit the same UTR (three runs) |
| Remove the replay check on a duplicate reference | 19 of 20 identical retries are refused instead of replayed |
| Run the guard tests as the owner instead of `ledger_app` | 16 of 24 fail: the role is doing the work |
| Remove the application's funds check | **all tests still pass**: the database `CHECK` stops the overdraft by itself |
| `void` locks the hold before the account | 8 to 19 of 20 racing captures and voids die with `DeadlockDetected` (three runs) |
| `capture` debits before releasing the hold | a hold on the whole balance can no longer be captured |
| Remove the deferred capture-matches check | a hold can be marked captured against an unrelated transaction |
| `authorize` skips the replay check after taking the lock | 18 of 20 identical retries are refused for lack of the money the first one reserved |
| `capture` skips its own expiry check | still refused: the database guard raises instead, with a less friendly error |
| `authorize` skips its funds check | **all tests still pass**: the balance floor refuses the hold |
| `sweep` reads the buffer's balance before taking its lock | 19 of 20 concurrent sweeps of one buffer crash; the buffer's floor stops the money moving twice (three runs) |
| Postings skip the shared period lock | a close does not wait for a posting in flight; the race test's audit finds a snapshot that missed money (3 runs of 3) |
| Remove the refusal of a posting that started before the close | it lands inside the closed period and the snapshot no longer matches its entries |
| Lock accounts `FOR UPDATE` instead of `FOR NO KEY UPDATE` | postings racing a close deadlock, 5 runs of 5: see below |
| Drop the `created_at` foreign key on entries | a balanced pair appended to an old transaction moves 700 |
| Remove the database's sweep-only rule for buffers | two raw-SQL tests pay a stranger out of a buffer |
| Remove the application's check that a buffer cannot pay | **all money tests still pass**: the database refuses; only the API's error code changes, to `insufficient_funds` |
| The relay claims events without `FOR UPDATE SKIP LOCKED` | 8 relays send 151 events 211 times; fails 5 runs of 5 |
| The relay's failure write is not fenced on `attempts` | a relay whose lease ran out pushes back the retry clock of the relay that took over |
| The event trigger fires at once instead of at COMMIT | events carry no entries, because they are inserted after the transaction row |

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

## Holds

A hold reserves money now and moves it later: a card authorisation, a UPI
mandate, a hotel pre-auth. While it is pending, the account's `held` goes up
and `available` (`balance - held`) goes down, but nothing moves. It ends
exactly once:

| Ending | What happens |
|---|---|
| `captured` | money moves to the payee named when the hold was opened. Capturing less than the hold is a partial capture: the rest is released |
| `voided` | released, nothing moves. Voiding twice is a no-op, so retries are safe |
| `expired` | released by `python -m ledger.jobs expire-holds`, run on a schedule |

A refund after capture is an ordinary reversal of the capture transaction.

Every path takes locks in the same order, accounts then the hold. A `void`
that took them the other way round (the hold first, then the account through
the held-balance trigger) deadlocks against a concurrent capture; the
mutation table above shows the race test catching it. Between a hold
lapsing and the job running, the money stays reserved. That errs the safe way:
reserved a little too long, never released early. A lapsed hold cannot be
captured either way, because the database checks the clock itself.

## Busy accounts: shard groups

Every deposit debits the bank settlement account, so every deposit wants the
same row lock. Postings to it queue one behind another however many workers
there are. A shard group splits that account into several rows
(`bank:hdfc#0` to `bank:hdfc#7`), and each posting picks one:

```powershell
.venv\Scripts\python -m ledger.migrate --system-account bank:hdfc INR --shards 8
```

A posting holds its row locks across several round trips to Postgres, so
the one-account ceiling is set by network latency more than by Postgres.
`python -m ledger.bench --rtt-ms 0,0.01,5` puts a delaying TCP proxy between
the app and the database and reports the round trip it actually measured.
20 threads, 1,000 deposits a round, median of 3 rounds, transfers/s:

| Round trip (measured) | one settlement account | 8 shards | no shared account | shards vs one |
|---|---|---|---|---|
| 0.08 ms (direct) | 553 | 898 | 873 | 1.6x |
| 3.0 ms | 151 | 624 | 646 | 4.1x |
| 7.0 ms | 88 | 632 | 619 | 7.2x |

How to read it:

- **The one-account column is the problem.** Every added millisecond of
  round trip is a millisecond the bank row stays locked, so it falls from
  553 to 88. The shards barely move.
- **At 3 ms and above, the shards and the no-shared-account column both sit
  near 620 to 650.** That is this harness's own limit (one Python process
  and the proxy), not Postgres's, so read those rows as "the shards removed
  the contention", not as a throughput figure.
- **The proxy cannot go below about 3 ms.** Asking for 0.5, 1 or 2 ms all
  measured 2.98 ms, because sleeps round up even with the Windows timer at
  1 ms (at the default 15.6 ms, a requested 1 ms came out as 31). Same-region
  cloud round trips of 0.2 to 1 ms fall between the first two rows and are
  not measured here.
- **Absolute numbers move with machine load.** The day before, a direct run
  of 3,000 deposits a round measured 965, 1,209 and 1,325. Compare columns
  within one row, not across runs.
- **The one-account cells at 3 ms and 7 ms did not reproduce.** A rerun on
  2026-11-27 at 3 ms gave 30 to 35 a second, three rounds, with the outbox's
  triggers switched off too, against 151 the day before; one round of another
  run gave 179. About 31 ms a posting is twice the default Windows timer
  tick of 15.6 ms, so under lock contention the proxy's sleeps probably fall
  back to the coarse timer some of the time. That is not established. Treat
  those two cells as unreliable in either direction. The shard and
  no-shared-account columns held steady across every run (540 to 780).

The details that make it safe:

- **The shard is picked by a hash of (client, key), not at random.** A retry
  must land on the same shard to replay. Picked at random, a retry would go
  to a different shard, its request hash would differ, and it would fail as
  an idempotency conflict.
- **Only accounts that may go negative can be shards** (a `CHECK` enforces
  it). They have no balance floor, so no posting ever needs the group's total.
- **The UTR rule still spans the group**, since it is keyed on the reference,
  not the account. A test sends two deliveries of one UTR to different shards.
- **Reconciliation takes the group name** and reads every shard;
  `GET /groups/{name}` returns the total.

## Busy merchants: receive buffers

A merchant taking thousands of payments is a hot row too, but it cannot be a
shard group: a merchant may not go negative, and checking a floor across
shards needs the group's total under a lock, which is the contention the
shards were meant to remove.

The way out is that **money arriving can never break a floor**. Only money
leaving can. So the merchant keeps its one real account, and payments land in
buffer rows beside it (`merchant:hotel#0` to `#7`), picked by the same hash of
(client, key) as shards, so retries replay. A sweep moves each buffer into the
merchant:

```powershell
.venv\Scripts\python -m ledger.migrate --receive-buffers merchant:hotel 8
.venv\Scripts\python -m ledger.jobs sweep    # from a scheduler, every minute or so
```

Payers send to the group (`"to_group": "merchant:hotel"`). The merchant's
`GET /accounts/{id}` shows `balance`, which it can spend, and `unswept`, which
has arrived but not yet been swept.

What keeps it safe:

- **A buffer pays only its own merchant.** A deferred trigger checks at
  COMMIT that any transaction debiting a buffer is exactly a sweep: that debit
  and an equal credit to its merchant, nothing else. A payout, a hold or a
  reversal out of a buffer is refused, by the app with a clear error and by
  the database if the app is bypassed.
- **Buffers have a floor like any customer account.** A sweep reads the
  buffer's balance under its row lock, so two sweeps of one buffer queue and
  the second finds it empty. A mutation test moves that read before the lock:
  19 of 20 concurrent sweeps then try to move money that is already gone, and
  the floor refuses every one.
- **Only the owner creates buffers**, as with shards and mints.

**Throughput.** `python -m ledger.bench --only merchant`, direct (no latency
proxy), 20 threads, 3,000 payments a round from 400 customers, median of 5
rounds, transfers/s:

| | one merchant account | 8 receive buffers |
|---|---|---|
| first run | 713 | 1,037 |
| second run | 638 | 898 |

About 1.4 times, close to what shards gave the settlement account with no
added latency. Behind the 3 ms latency proxy the buffers held 700 to 1,000 a
second; the one-account figure there is not reported, for the reason in the
last bullet of the shard section.

## Events: the outbox

Other services need to know when money moves: a wallet app to notify the
user, a risk system to score the payment, a warehouse to load it. Publishing
from the application after the database commit is the dual-write problem: a
crash between the two loses the event, and publishing before the commit can
announce a transfer that then rolls back.

Here the event is written **by a trigger, in the same database transaction as
the money**. If the transfer commits, its event exists; if it rolls back, so
does the event. The application cannot skip it, and since the app role has no
`INSERT` on the outbox, it cannot invent one either.

| Event | When |
|---|---|
| `transaction.posted` | a transfer or capture commits; carries its entries |
| `transaction.reversed` | a reversal commits; carries `reverses_id` |
| `hold.authorized`, `hold.captured`, `hold.voided`, `hold.expired` | a hold opens or resolves |

The transaction event fires at COMMIT, from a deferred trigger, because the
transaction row is inserted before its entries; fired at once, it would carry
none. A mutation test pins that.

A relay delivers them:

```powershell
$env:LEDGER_WEBHOOK_SECRET = "a long random string shared with the receiver"
.venv\Scripts\python -m ledger.relay --url https://receiver.example/ledger-events
```

- **At least once.** A relay that crashes after the receiver answered but
  before recording it sends the event again. Every attempt has the same body
  and the same `Ledger-Event-Id`, so the receiver dedupes on that id.
- **Signed.** `Ledger-Signature: t=<unix time>,v1=<hex>` is HMAC-SHA256 over
  `"<t>.<body>"`. The timestamp is inside the signature, so a captured request
  cannot be replayed later. `relay.verify(secret, body, header)` is the
  receiver's side, and it refuses anything older than five minutes.
- **Retried with backoff.** A non-2xx, a refused connection or a timeout is
  retried after 2 s, 4 s, 8 s and so on, with jitter, capped at six hours.
  After 12 attempts (about a day and a half) the event is marked dead, with
  its last error, for a human.
- **Several relays can run.** One `UPDATE` claims a batch with
  `FOR UPDATE SKIP LOCKED` and leases it, so no transaction stays open while
  sending. If a relay dies, its events come due again when the lease ends.
  A relay that finishes after its lease ran out cannot overwrite the retry
  schedule of the relay that took over: its write is fenced on the attempt
  number.
- **Not ordered.** A failing event does not hold back the ones behind it.
  Each payload carries its ids and `created_at`; the statement endpoint is the
  ordered record of an account.

There is no "events after id N" feed for consumers to poll. Ids are assigned
when the row is inserted, but rows become visible when their transaction
commits, and those orders differ: a consumer that saw event 11 and then asked
for "after 11" would skip an event 10 that committed a moment later. The relay
has no such gap, because it asks for "everything not yet delivered".

`GET /health` reports the backlog (pending, dead, age of the oldest pending
event) next to the invariants, and the invariant audit checks that every
transaction and hold state has its event.

**What it costs.** The event is one more insert, made at COMMIT while the
posting's row locks are still held. `ledger.bench` direct, 20 threads, 3,000
transfers a round, median of 5 rounds, transfers/s:

| | one settlement account | 8 shards | no shared account |
|---|---|---|---|
| without events (two runs) | 1,007 and 994 | 1,408 and 1,305 | 1,422 and 1,330 |
| with events | 895 | 1,313 | 1,342 |

On the one busy account it costs about 10%, because every posting now holds
the bank row a little longer. Where no row is shared, the difference is
inside the run-to-run noise. The first run with events was disturbed (its
one-account rounds ranged from 285 to 906) and was run again; the table shows
the rerun.

## Closing the books

```powershell
.venv\Scripts\python -m ledger.jobs close     # daily, after midnight: through the end of yesterday, IST
```

A close records, for every account that moved since the last one, its
balance at the cutoff. After that:

- `GET /accounts/{id}/balance?at=...` is the latest snapshot at or before that
  moment plus the entries after it, not a scan of the account's history.
  `final` says whether the books are closed through that moment.
- `GET /trial-balance?at=...` lists every non-zero balance at a moment and the
  total per currency, which is zero or the response says `balanced: false`.
- The invariant audit recomputes every snapshot from the entries.

**The part that is easy to get wrong.** A posting's entries are stamped with
the time its database transaction *started*, but other sessions only see them
once it *commits*. A posting that started at 23:59:59.9 and commits at
00:00:00.3 is stamped inside yesterday. A close that ran at 00:00:00.1 could
not see it, and the snapshot would be short by exactly that amount, forever,
with nothing failing. This is the same gap that rules out an "events after id
N" feed, from the other side.

So every posting takes a shared advisory lock when it inserts its
transaction row and holds it to COMMIT, and a close takes that lock
exclusively. The close waits for every posting in flight, and new postings
wait for the close. A posting that started before the cutoff but reached its
insert after the close is refused with `period_closed` (409), and its client
retries with the same key, which now runs in a new transaction stamped after
the cutoff. The lock is shared, so postings never wait on each other for it,
only on a close.

**A deadlock the race test found.** With row locks taken `FOR UPDATE`, a
posting holding an account's row waited on the close's lock, while the close,
writing that account's snapshot, waited on the row: the snapshot's foreign key
takes a `KEY SHARE` lock on the account, and `FOR UPDATE` blocks it. Postings
never change an account's key, so they now lock `FOR NO KEY UPDATE`, which
still makes postings to one account queue and does not block foreign-key
checks. Put back, `FOR UPDATE` deadlocks the race test 5 runs of 5.

**The statement endpoint no longer rescans history.** It reads the newest
`limit` entries and works back from the current balance, in one `REPEATABLE
READ` snapshot so a posting committing in between cannot skew it.

**What the lock costs: nothing measurable.** `ledger.bench` direct, 20
threads, 3,000 transfers a round, median of 5 rounds, run back to back with
the lock off, on, off, on, transfers/s:

| period lock | one settlement account | 8 shards | no shared account | one merchant | 8 buffers |
|---|---|---|---|---|---|
| off | 670, 703 | 1,043, 1,040 | 1,017, 1,018 | 697, 692 | 1,014, 977 |
| on | 670, 679 | 1,008, 1,022 | 1,026, 1,008 | 684, 692 | 1,009, 1,008 |

Every column is inside run-to-run noise. Every figure here is lower than the
morning's runs in the outbox table, lock on or off: the machine was slower
in the afternoon, which is why each comparison in this README is made back
to back, never across sessions.

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
redelivered webhook that is refused, a hotel pre-auth captured for less, a
cancelled ride, a ride still on hold), delivers the day's events to a local
receiver that is down for its first request, and reconciles the ledger
against a statement with problems planted in it:

```
app role refused a mint: only the database owner can create an account that may go negative
redelivered webhook refused: external_ref UTR001 is already posted as transaction 1
hold refused (amounts in paise): account 3 has 120500 available and cannot hold 130000
balances (held is reserved by open holds; available = balance - held)
                    balance     held  available
  bank:hdfc        -6150.00     0.00   -6150.00
  wallet:asha       3050.00     0.00    3050.00
  wallet:ravi       1300.00    95.00    1205.00
  revenue:fees         0.00     0.00       0.00
  merchant:hotel    1800.00     0.00    1800.00
  merchant:rides       0.00     0.00       0.00
invariants ok: True

events: 15 delivered, signatures checked; the receiver's one outage (HTTP 503) was retried
  hold.authorized        3
  hold.captured          1
  hold.voided            1
  transaction.posted     9
  transaction.reversed   1

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
balance is minus what the bank should be holding for everyone else: 6,150.00,
the sum of every other account's balance.

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
| `GET /accounts/{id}` | balance, held, available, and for a merchant with buffers, `unswept` |
| `GET /accounts/{id}/statement` | entries with running balance |
| `POST /transfers` | requires `Idempotency-Key` and `Client-Id`; each side is `from_account`/`to_account` (an id) or `from_group`/`to_group` (a shard group, or a merchant with receive buffers, which can only be paid into); 201 when new, 200 plus `Idempotent-Replayed: true` on a retry, 409 for a reused key or an already-posted `external_ref` |
| `GET /groups/{name}` | a shard group's total balance |
| `GET /accounts/{id}/balance?at=` | balance at a moment (with a timezone); `final` once the books are closed through it |
| `GET /trial-balance?at=` | every non-zero balance at a moment, totals per currency |
| `GET /periods` | how far the books are closed |
| `POST /transactions/{id}/reverse` | requires `Idempotency-Key` and `Client-Id` |
| `POST /holds` | `{account, payee, amount, expires_in_seconds}`; requires `Idempotency-Key` and `Client-Id`; default expiry 7 days, at most 30 |
| `GET /holds/{id}` | status, amounts, expiry, `lapsed` |
| `POST /holds/{id}/capture` | optional `{amount}` for a partial capture; requires `Idempotency-Key` and `Client-Id`; returns the transaction |
| `POST /holds/{id}/void` | release; 409 if already captured or expired |
| `GET /health` | full invariant audit, 500 if anything disagrees; also the event backlog |

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

- **A merchant can spend only what has been swept.** Payments sit in
  buffers, shown as `unswept`, until the sweep job runs. A payout or refund
  that needs them waits for the next sweep, or the job is run first.
- **A payment into a buffer cannot be reversed**, since a reversal would debit
  the buffer. Refund it from the merchant's account, as a new transfer.
- **Captures still lock the merchant's row.** A hold names its payee when it is
  opened, and the capture credits that account directly, not a buffer.
- **Latency is simulated, not real.** The benchmark's proxy adds delay on one
  machine; it is not a network between two hosts, and it cannot simulate
  round trips under about 3 ms.
- **`Client-Id` is trusted as sent.** It keeps honest clients' keys apart. It
  does not stop one client claiming to be another; that is the auth layer's job.
- **The schema is re-applied on local startup**, which briefly locks tables.
  In a deployment, `ledger.migrate` runs once and the service only connects.
- **A close holds every posting back while it runs.** It waits for postings in
  flight and new ones wait for it. Once a day that is short; how short at
  scale is not measured here.
- **One ledger-wide period.** There is no per-currency or per-entity close,
  and no reopening a closed period: a correction after the close is a new
  posting in the open period.
- **The relay connects as the app role**, so that role can also mark an event
  delivered without sending it. It still cannot forge, edit or delete one. A
  separate relay role would close this.
- **One receiver.** Every event goes to one URL; fan-out to several
  subscribers, each with its own progress, is not built.
- **The outbox only grows.** Delivered events are never deleted, by design,
  and there is no archiving yet.

## Layout

```
ledger/schema.sql   tables, triggers, roles and grants, the outbox, the audit views
ledger/core.py      transfer, reverse, holds, statement, invariant audit
ledger/api.py       FastAPI
ledger/recon.py     statement parser and two-pass matcher
ledger/migrate.py   owner-side setup and system accounts
ledger/jobs.py      scheduled jobs (hold expiry, buffer sweep, daily close)
ledger/relay.py     delivers outbox events as signed webhooks
ledger/bench.py     throughput: one hot account vs shards or buffers vs none
ledger/demo.py      end-to-end run
tests/              220 tests: core, holds, shard groups, receive buffers, closing the books, events and the relay, raw-SQL attacks as the app role, races, API, reconciliation
```
