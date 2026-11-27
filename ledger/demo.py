"""End-to-end demo: a small wallet business for one day, its events delivered
to a webhook, then a bank reconciliation.

    python -m ledger.demo

Uses its own database (DEMO_DATABASE_URL, or `ledger_demo` on the bundled
local Postgres) and wipes it first. Writes demo/statement.csv and
demo/recon_report.csv.
"""
from __future__ import annotations

import csv
import json
import os
import secrets
import threading
from collections import Counter
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg

from . import core, db, recon, relay

OUT = db.ROOT / "demo"


def main() -> int:
    owner_url = os.environ.get("DEMO_DATABASE_URL") or db.local_url("ledger_demo")
    db.migrate(owner_url)
    # Setup is the owner's job: wiping the demo, and creating the one account
    # that may go negative. Everything after runs as the app role.
    with psycopg.connect(owner_url, autocommit=True) as owner:
        owner.execute("TRUNCATE ledger_closes, outbox, entries, transactions, accounts RESTART IDENTITY CASCADE")
        bank = core.create_account(owner, "bank:hdfc", "INR", allow_negative=True)["id"]

    pool = db.make_pool(db.as_app_role(owner_url), max_size=2)
    with pool.connection() as conn:
        try:
            core.create_account(conn, "mint:sneaky", "INR", allow_negative=True)
        except core.Forbidden as exc:
            print(f"app role refused a mint: {exc}")
        asha = core.create_account(conn, "wallet:asha", "INR")["id"]
        ravi = core.create_account(conn, "wallet:ravi", "INR")["id"]
        fees = core.create_account(conn, "revenue:fees", "INR")["id"]

        def t(key, src, dst, rupees, memo, ref=None):
            return core.transfer(conn, client="wallet-app", key=key, from_account=src, to_account=dst,
                                 amount=recon.to_minor(rupees), memo=memo, external_ref=ref)

        t("dep-1", bank, asha, "5000", "UPI deposit", "UTR001")
        t("dep-2", bank, ravi, "2000", "UPI deposit", "UTR002")
        t("p2p-1", asha, ravi, "1200", "Asha pays Ravi")
        fee = t("fee-1", ravi, fees, "10", "transfer fee")
        t("wd-1", ravi, bank, "1500", "withdrawal to bank", "UTR003")
        t("dep-3", bank, asha, "750", "UPI deposit", "UTR004")
        t("dep-4", bank, asha, "300", "cash deposit at branch")  # no reference on the ledger side
        t("wd-2", ravi, bank, "400", "withdrawal to bank", "UTR005")

        # A retry of a deposit the client thought had failed: replays, pays nothing.
        assert t("dep-1", bank, asha, "5000", "UPI deposit", "UTR001").replayed
        # The bank's webhook for UTR001 redelivered with a fresh key: refused, not credited twice.
        try:
            t("webhook-redelivery-77", bank, asha, "5000", "UPI deposit", "UTR001")
        except core.DuplicateReference as exc:
            print(f"redelivered webhook refused: {exc}")
        # Waive the fee: a reversal, not an edit.
        core.reverse(conn, client="wallet-app", key="waive-fee-1", transaction_id=fee.transaction_id)

        # Card-style holds. Asha checks in to a hotel: 2,000 pre-authorised,
        # 1,800 captured at checkout, the rest released. Ravi books a ride
        # for 120 and cancels it. A third hold is still open at day end.
        hotel = core.create_account(conn, "merchant:hotel", "INR")["id"]
        rides = core.create_account(conn, "merchant:rides", "INR")["id"]

        def h(key, payer, payee, rupees):
            return core.authorize(conn, client="wallet-app", key=key, account=payer, payee=payee,
                                  amount=recon.to_minor(rupees)).hold_id

        stay = h("preauth-hotel-1", asha, hotel, "2000")
        core.capture(conn, client="wallet-app", key="checkout-hotel-1", hold_id=stay,
                     amount=recon.to_minor("1800"))
        ride = h("ride-1", ravi, rides, "120")
        core.void(conn, hold_id=ride)
        h("ride-2", ravi, rides, "95")
        try:
            h("ride-3", ravi, rides, "1300")  # 1,300 balance but only 1,205 available
        except core.InsufficientFunds as exc:
            print(f"hold refused (amounts in paise): {exc}")

        print("balances (held is reserved by open holds; available = balance - held)")
        print(f"  {'':<14} {'balance':>10} {'held':>8} {'available':>10}")
        for acct in (bank, asha, ravi, fees, hotel, rides):
            a = core.get_account(conn, acct)
            print(f"  {a['name']:<14} {recon.fmt(a['balance']):>10} {recon.fmt(a['held']):>8}"
                  f" {recon.fmt(a['available']):>10}")
        health = core.check_invariants(conn)
        print(f"invariants ok: {health['ok']}")
        deliver_events(conn)

        # The bank's view of the same day, with the problems real statements have.
        today = recon.local_today()
        statement = [
            (today, "5000.00", "UTR001", "UPI/ASHA"),
            (today, "2000.00", "UTR002", "UPI/RAVI"),
            (today, "-1500.00", "UTR003", "NEFT/RAVI"),
            (today, "745.00", "UTR004", "UPI/ASHA less charges"),     # bank deducted 5.00
            (today - timedelta(days=1), "300.00", "CASHDEP", "CASH DEPOSIT"),  # booked a day early, no shared ref
            (today, "1000.00", "UTR006", "UPI/UNKNOWN"),              # money arrived, ledger never credited it
            # UTR005 (the 400.00 withdrawal) has not left the bank yet.
        ]
        OUT.mkdir(exist_ok=True)
        stmt_path = OUT / "statement.csv"
        with open(stmt_path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["date", "amount", "reference", "description"])
            for d, amount, ref, desc in statement:
                w.writerow([d.isoformat(), amount, ref, desc])

        results = recon.run(conn, stmt_path, "bank:hdfc", today - timedelta(days=1), today)
    pool.close()

    report = OUT / "recon_report.csv"
    recon.write_report(results, report)
    print("\nreconciliation of bank:hdfc")
    print(recon.summarise(results))
    print(f"\nwrote {stmt_path.relative_to(db.ROOT)} and {report.relative_to(db.ROOT)}")
    return 0


def deliver_events(conn) -> None:
    """Send the day's events to a local receiver that is down for its first request."""
    secret = secrets.token_hex(16)
    seen: Counter = Counter()
    state = {"fail": 1}

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            if state["fail"]:
                state["fail"] -= 1
                self.send_response(503)
            elif not relay.verify(secret, body, self.headers[relay.SIGNATURE_HEADER]):
                self.send_response(401)
            else:
                seen[json.loads(body)["type"]] += 1
                self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/ledger-events"
        counts = relay.run_once(conn, url, secret, relay.Policy(base_delay=0.0))
    finally:
        server.shutdown()
        server.server_close()
    print(f"\nevents: {counts['delivered']} delivered, signatures checked; the receiver's one outage "
          f"(HTTP 503) was retried")
    for kind, n in sorted(seen.items()):
        print(f"  {kind:<22} {n}")


if __name__ == "__main__":
    raise SystemExit(main())
