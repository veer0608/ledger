from datetime import date, timedelta

import pytest

from ledger import core, recon
from ledger.recon import BankLine, LedgerLine, reconcile
from tests.conftest import CLIENT

D = date(2026, 11, 20)


def statuses(results):
    return sorted((r.status, r.bank.line if r.bank else None, r.ledger.entry_id if r.ledger else None)
                  for r in results)


@pytest.mark.parametrize("text,minor", [
    ("1500", 150000), ("1,500.50", 150050), ("-20", -2000), ("20.00 Dr", -2000),
    ("20.00 Cr", 2000), ("0.01", 1), ("₹99.90", 9990),
])
def test_to_minor(text, minor):
    assert recon.to_minor(text) == minor


@pytest.mark.parametrize("text", ["1.005", "abc", "", "NaN", "Infinity"])
def test_to_minor_rejects(text):
    with pytest.raises(ValueError):
        recon.to_minor(text)


def test_fmt_round_trips():
    for minor in (0, 1, -1, 150050, -2000):
        assert recon.to_minor(recon.fmt(minor)) == minor


def test_reference_match_and_amount_mismatch():
    bank = [BankLine(2, D, 5000, "UTR1"), BankLine(3, D, 745, "UTR2")]
    ledger = [LedgerLine(10, 1, D, 5000, "UTR1"), LedgerLine(11, 2, D, 750, "UTR2")]
    r = reconcile(bank, ledger)
    assert statuses(r) == [("amount_mismatch", 3, 11), ("matched", 2, 10)]
    assert "difference -0.05" in next(x for x in r if x.status == "amount_mismatch").note


def test_amount_match_needs_a_missing_reference():
    # Same amount, same day, but different UTRs: two different payments.
    bank = [BankLine(2, D, 1000, "UTR-A")]
    ledger = [LedgerLine(10, 1, D, 1000, "UTR-B")]
    assert statuses(reconcile(bank, ledger)) == [("bank_only", 2, None), ("ledger_only", None, 10)]


def test_amount_match_within_window_picks_closest_date():
    bank = [BankLine(2, D, 300, "")]
    ledger = [LedgerLine(10, 1, date(2026, 11, 22), 300, ""), LedgerLine(11, 2, date(2026, 11, 21), 300, "")]
    r = reconcile(bank, ledger, window_days=2)
    assert statuses(r) == [("ledger_only", None, 10), ("matched_by_amount", 2, 11)]


def test_amount_match_outside_window_does_not_pair():
    bank = [BankLine(2, D, 300, "")]
    ledger = [LedgerLine(10, 1, date(2026, 11, 25), 300, "")]
    assert statuses(reconcile(bank, ledger, window_days=2)) == [("bank_only", 2, None), ("ledger_only", None, 10)]


def test_duplicate_bank_line_is_not_matched_twice():
    bank = [BankLine(2, D, 500, "UTR1"), BankLine(3, D, 500, "UTR1")]
    ledger = [LedgerLine(10, 1, D, 500, "UTR1")]
    assert statuses(reconcile(bank, ledger)) == [("bank_only", 3, None), ("matched", 2, 10)]


def test_one_to_one_in_amount_pass():
    bank = [BankLine(2, D, 100, ""), BankLine(3, D, 100, "")]
    ledger = [LedgerLine(10, 1, D, 100, "")]
    assert [r.status for r in reconcile(bank, ledger)].count("matched_by_amount") == 1


def test_read_statement_formats(tmp_path):
    p = tmp_path / "s.csv"
    p.write_text("Date,Amount,Reference,Description\n20/11/2026,\"1,200.00\",UTR9,UPI\n\n2026-11-21,50 Dr,,ATM\n",
                 encoding="utf-8")
    lines = recon.read_statement(p)
    assert [(b.line, b.date, b.amount, b.reference) for b in lines] == [
        (2, D, 120000, "UTR9"), (4, date(2026, 11, 21), -5000, "")]


def test_read_statement_reports_bad_line(tmp_path):
    p = tmp_path / "s.csv"
    p.write_text("date,amount,reference\n2026-11-20,12.345,UTR1\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"s\.csv:2: more than 2 decimal places"):
        recon.read_statement(p)


def test_against_the_database(conn, make_account, tmp_path):
    bank = make_account("bank:hdfc", allow_negative=True)
    user = make_account("wallet:u")
    core.transfer(conn, client=CLIENT, key="d1", from_account=bank, to_account=user, amount=500_00, external_ref="UTR1")
    core.transfer(conn, client=CLIENT, key="w1", from_account=user, to_account=bank, amount=200_00, external_ref="UTR2")
    core.transfer(conn, client=CLIENT, key="w2", from_account=user, to_account=bank, amount=50_00, external_ref="UTR3")
    today = recon.local_today()
    p = tmp_path / "s.csv"
    p.write_text(f"date,amount,reference\n{today},500.00,UTR1\n{today},-200.00,UTR2\n", encoding="utf-8")
    results = recon.run(conn, p, "bank:hdfc", today, today)
    got = sorted((r.status, (r.bank or r.ledger).reference) for r in results)
    assert got == [("ledger_only", "UTR3"), ("matched", "UTR1"), ("matched", "UTR2")]


def test_ledger_line_before_first_bank_line_is_still_reported(conn, make_account, tmp_path):
    # The silent-drop bug: the period used to be inferred from the bank lines,
    # so a deposit dated before the bank's first line vanished and the run
    # came back clean.
    bank = make_account("bank:hdfc", allow_negative=True)
    user = make_account("wallet:u")
    core.transfer(conn, client=CLIENT, key="d1", from_account=bank, to_account=user, amount=500_00, external_ref="UTR1")
    core.transfer(conn, client=CLIENT, key="d2", from_account=bank, to_account=user, amount=70_00, external_ref="UTR2")
    today = recon.local_today()
    tomorrow = today + timedelta(days=1)
    p = tmp_path / "s.csv"
    p.write_text(f"date,amount,reference\n{tomorrow},500.00,UTR1\n", encoding="utf-8")
    results = recon.run(conn, p, "bank:hdfc", today, tomorrow)
    got = sorted((r.status, (r.bank or r.ledger).reference) for r in results)
    assert got == [("ledger_only", "UTR2"), ("matched", "UTR1")]


def test_statement_line_outside_period_is_an_error(conn, make_account, tmp_path):
    make_account("bank:hdfc", allow_negative=True)
    p = tmp_path / "s.csv"
    p.write_text("date,amount,reference\n2026-10-31,1.00,X\n2026-11-02,1.00,Y\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"s\.csv:2: dated 2026-10-31, outside the period"):
        recon.run(conn, p, "bank:hdfc", date(2026, 11, 1), date(2026, 11, 30))


def test_unknown_account_is_an_error(conn, tmp_path):
    p = tmp_path / "s.csv"
    p.write_text("date,amount,reference\n2026-11-02,1.00,Y\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no ledger account"):
        recon.run(conn, p, "bank:typo", date(2026, 11, 1), date(2026, 11, 30))
