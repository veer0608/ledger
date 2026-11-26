"""Reconcile a bank statement CSV against the ledger's settlement account.

    python -m ledger.recon statement.csv --account bank:hdfc --from 2026-11-01 --to 2026-11-30

The settlement account stands for the outside world as seen through one bank
account. Money that arrives through the bank enters the ledger as a transfer
*out of* the settlement account, so a statement credit of +1,500.00 matches a
ledger entry of -150000 paise on it. All amounts below are from the bank's
point of view (credit positive).

Statement CSV columns: date, amount, reference, description (optional).
Dates are YYYY-MM-DD or DD/MM/YYYY. Amounts are rupees with at most two
decimals; a leading minus or a "Dr" suffix means a debit.

Matching runs in two passes:
  1. reference: statement reference == ledger external_ref (UTR, RRN, ...).
     Same reference, different amount -> amount_mismatch.
  2. amount + date: same amount within --window days, closest date first,
     one-to-one. Only pairs where at least one side has no reference, so two
     payments with different UTRs are never glued together on amount alone.
Whatever is left is bank_only (money moved, ledger never heard) or
ledger_only (ledger recorded it, bank has not).

The statement period is given, never inferred from the statement's own
lines. Inferring it from the first and last bank line silently dropped any
ledger entry dated before the bank's first line, which is exactly the entry
that never reached the bank. Every ledger entry dated inside the period is
reported. A statement line outside the period is an error: wrong file.

Exit code is 0 only when everything matched cleanly.
"""
from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

# Banks print local dates; ledger timestamps are converted to match. IST is a
# fixed +05:30 with no DST, so no tz database is needed (the bundled Postgres has none).
LOCAL_TZ = timezone(timedelta(hours=5, minutes=30), "IST")


@dataclass(frozen=True)
class BankLine:
    line: int
    date: date
    amount: int  # minor units, credit positive
    reference: str
    description: str = ""


@dataclass(frozen=True)
class LedgerLine:
    entry_id: int
    transaction_id: int
    date: date
    amount: int  # minor units, bank's point of view
    reference: str


@dataclass(frozen=True)
class Result:
    status: str  # matched | matched_by_amount | amount_mismatch | bank_only | ledger_only
    bank: BankLine | None
    ledger: LedgerLine | None
    note: str = ""


CLEAN = {"matched", "matched_by_amount"}


def to_minor(text: str) -> int:
    """'1,500.50' -> 150050, '-20' -> -2000, '20.00 Dr' -> -2000. Refuses sub-paisa amounts."""
    s = text.strip().replace(",", "").replace("₹", "")
    sign = 1
    low = s.lower()
    if low.endswith("dr"):
        sign, s = -1, s[:-2].strip()
    elif low.endswith("cr"):
        s = s[:-2].strip()
    try:
        d = Decimal(s)
    except InvalidOperation:
        raise ValueError(f"not an amount: {text!r}") from None
    if not d.is_finite():
        raise ValueError(f"not an amount: {text!r}")
    if d != d.quantize(Decimal("0.01")):
        raise ValueError(f"more than 2 decimal places: {text!r}")
    return sign * int(d * 100)


def fmt(minor: int) -> str:
    sign = "-" if minor < 0 else ""
    rupees, paise = divmod(abs(minor), 100)
    return f"{sign}{rupees}.{paise:02d}"


def parse_date(text: str) -> date:
    s = text.strip()
    for pattern in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, pattern).date()
        except ValueError:
            pass
    raise ValueError(f"unrecognised date {text!r}; use YYYY-MM-DD or DD/MM/YYYY")


def read_statement(path: Path) -> list[BankLine]:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        headers = {h.strip().lower() for h in reader.fieldnames or []}
        missing = {"date", "amount", "reference"} - headers
        if missing:
            raise ValueError(f"{path}: missing column(s) {sorted(missing)}")
        lines = []
        for row in reader:
            n = reader.line_num  # the physical line, so blank lines do not shift it
            row = {k.strip().lower(): (v or "").strip() for k, v in row.items() if k}
            if not any(row.values()):
                continue
            try:
                lines.append(BankLine(n, parse_date(row["date"]), to_minor(row["amount"]),
                                      row["reference"], row.get("description", "")))
            except ValueError as exc:
                raise ValueError(f"{path}:{n}: {exc}") from None
        return lines


def reconcile(bank: list[BankLine], ledger: list[LedgerLine], window_days: int = 2) -> list[Result]:
    results: list[Result] = []
    used: set[int] = set()  # ledger entry ids

    by_ref: dict[str, list[LedgerLine]] = {}
    for led in ledger:
        if led.reference:
            by_ref.setdefault(led.reference, []).append(led)

    # Pass 1: reference.
    leftover: list[BankLine] = []
    for b in bank:
        candidates = [x for x in by_ref.get(b.reference, []) if x.entry_id not in used] if b.reference else []
        if not candidates:
            leftover.append(b)
            continue
        # Prefer the candidate with the same amount, then the closest date.
        led = min(candidates, key=lambda x: (x.amount != b.amount, abs((x.date - b.date).days), x.entry_id))
        used.add(led.entry_id)
        if led.amount == b.amount:
            results.append(Result("matched", b, led))
        else:
            results.append(Result("amount_mismatch", b, led,
                                  f"bank {fmt(b.amount)} vs ledger {fmt(led.amount)}, "
                                  f"difference {fmt(b.amount - led.amount)}"))

    # Pass 2: amount within the date window, closest first.
    window = timedelta(days=window_days)
    pairs = sorted(
        (abs((b.date - led.date).days), b.line, led.entry_id, b, led)
        for b in leftover
        for led in ledger
        if led.entry_id not in used
        and led.amount == b.amount
        and abs(b.date - led.date) <= window
        and not (b.reference and led.reference)
    )
    paired_bank: set[int] = set()
    for gap, _, _, b, led in pairs:
        if b.line in paired_bank or led.entry_id in used:
            continue
        paired_bank.add(b.line)
        used.add(led.entry_id)
        results.append(Result("matched_by_amount", b, led, f"date gap {gap} day(s)"))

    for b in leftover:
        if b.line not in paired_bank:
            results.append(Result("bank_only", b, None))
    for led in ledger:
        if led.entry_id not in used:
            results.append(Result("ledger_only", None, led))
    return results


def load_ledger(conn, account_name: str, start: date, end: date) -> list[LedgerLine]:
    lo = datetime.combine(start, time.min, LOCAL_TZ)
    hi = datetime.combine(end + timedelta(days=1), time.min, LOCAL_TZ)
    rows = conn.execute(
        """SELECT e.id, e.transaction_id, e.created_at, -e.amount, coalesce(t.external_ref, '')
           FROM entries e
           JOIN transactions t ON t.id = e.transaction_id
           JOIN accounts a ON a.id = e.account_id
           WHERE (a.name = %s OR a.shard_group = %s) AND e.created_at >= %s AND e.created_at < %s
           ORDER BY e.id""",
        (account_name, account_name, lo, hi),
    ).fetchall()
    return [LedgerLine(eid, tid, ts.astimezone(LOCAL_TZ).date(), amt, ref)
            for eid, tid, ts, amt, ref in rows]


def local_today() -> date:
    return datetime.now(LOCAL_TZ).date()


def run(conn, statement: Path, account_name: str, start: date, end: date,
        window_days: int = 2) -> list[Result]:
    if end < start:
        raise ValueError(f"period ends ({end}) before it starts ({start})")
    if not conn.execute("SELECT 1 FROM accounts WHERE name = %s OR shard_group = %s",
                        (account_name, account_name)).fetchone():
        raise ValueError(f"no ledger account or shard group named {account_name!r}")
    bank = read_statement(statement)
    outside = [b for b in bank if not start <= b.date <= end]
    if outside:
        b = outside[0]
        raise ValueError(f"{statement}:{b.line}: dated {b.date}, outside the period {start} to {end}")
    window = timedelta(days=window_days)
    ledger = load_ledger(conn, account_name, start - window, end + window)
    results = reconcile(bank, ledger, window_days)
    # Ledger lines in the margin outside the period were loaded only as match
    # candidates; unmatched, they belong to the neighbouring statement.
    return [r for r in results
            if not (r.status == "ledger_only" and not start <= r.ledger.date <= end)]


def write_report(results: list[Result], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["status", "bank_line", "bank_date", "bank_amount", "bank_reference", "bank_description",
                    "ledger_transaction", "ledger_date", "ledger_amount", "ledger_reference", "note"])
        for r in results:
            b, led = r.bank, r.ledger
            w.writerow([
                r.status,
                b.line if b else "", b.date if b else "", fmt(b.amount) if b else "",
                b.reference if b else "", b.description if b else "",
                led.transaction_id if led else "", led.date if led else "",
                fmt(led.amount) if led else "", led.reference if led else "",
                r.note,
            ])


def summarise(results: list[Result]) -> str:
    order = ["matched", "matched_by_amount", "amount_mismatch", "bank_only", "ledger_only"]
    counts = {s: sum(r.status == s for r in results) for s in order}
    width = max(map(len, order))
    out = [f"  {s:<{width}}  {counts[s]}" for s in order]
    for r in results:
        if r.status not in CLEAN:
            where = f"statement line {r.bank.line}" if r.bank else f"ledger txn {r.ledger.transaction_id}"
            amount = fmt((r.bank or r.ledger).amount)
            ref = (r.bank or r.ledger).reference or "no ref"
            out.append(f"  ! {r.status}: {where}, {amount}, {ref}" + (f" ({r.note})" if r.note else ""))
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    from . import db

    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("statement", type=Path)
    ap.add_argument("--account", required=True,
                    help="ledger settlement account or shard group name, e.g. bank:hdfc")
    ap.add_argument("--from", dest="start", required=True, type=date.fromisoformat,
                    help="first day of the statement period, YYYY-MM-DD")
    ap.add_argument("--to", dest="end", required=True, type=date.fromisoformat,
                    help="last day of the statement period, YYYY-MM-DD")
    ap.add_argument("--window", type=int, default=2, help="days either side for amount matching (default 2)")
    ap.add_argument("--out", type=Path, default=Path("recon_report.csv"))
    args = ap.parse_args(argv)

    pool = db.make_pool(db.app_url(), max_size=2)
    try:
        with pool.connection() as conn:
            results = run(conn, args.statement, args.account, args.start, args.end, args.window)
    finally:
        pool.close()
    write_report(results, args.out)
    print(summarise(results))
    print(f"report: {args.out}")
    return 0 if all(r.status in CLEAN for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
