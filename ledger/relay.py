"""Deliver outbox events to a webhook receiver.

    LEDGER_WEBHOOK_SECRET=... python -m ledger.relay --url https://example.com/hooks/ledger
    LEDGER_WEBHOOK_SECRET=... python -m ledger.relay --url ... --once   # drain what is due, then exit

Events are written by database triggers in the same transaction as the money
they describe (see the outbox section of schema.sql). This process only moves
them out. Delivery is at least once, never exactly once: a relay that crashes
after the receiver answered but before recording it will send that event
again. Receivers must dedupe on the Ledger-Event-Id header, which is the same
on every attempt, as is the body.

How a batch is taken, so several relays can run at once:

1. Claim. One UPDATE picks due events with FOR UPDATE SKIP LOCKED, so two
   relays never claim the same row at the same moment, bumps `attempts`, and
   pushes `next_attempt_at` forward by a lease. No transaction stays open
   while sending.
2. Send each event, signed with HMAC-SHA256 over "timestamp.body".
3. Record. A 2xx marks it delivered. Anything else schedules a retry with
   exponential backoff and jitter, or marks it dead when attempts run out.
   The failure write is fenced on `attempts`: if the lease ran out and
   another relay claimed the event meanwhile, this relay's stale failure does
   not move the newer claim's clock.

If a relay dies mid-batch, its claimed events come due again when the lease
ends, and another relay sends them.

Events are not delivered in a guaranteed order: a failing event does not hold
back the ones behind it. Each payload carries its ids and created_at, and the
statement endpoint is the ordered source of truth for an account.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import timedelta

import psycopg
from psycopg.rows import dict_row

from . import db

SIGNATURE_HEADER = "Ledger-Signature"
MIN_SECRET_LENGTH = 16


@dataclass(frozen=True)
class Policy:
    max_attempts: int = 12          # with the delays below, about a day and a half of retrying
    base_delay: float = 2.0         # seconds before the second attempt
    max_delay: float = 6 * 3600.0   # backoff stops doubling here
    timeout: float = 10.0           # per request
    batch: int = 20
    lease: float | None = None      # seconds; default covers a whole batch timing out

    @property
    def lease_seconds(self) -> float:
        return self.lease if self.lease is not None else self.batch * self.timeout + 5


def sign(secret: str, body: bytes, timestamp: int) -> str:
    mac = hmac.new(secret.encode(), str(timestamp).encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={mac}"


def verify(secret: str, body: bytes, header: str, *, tolerance: int = 300, now: float | None = None) -> bool:
    """For receivers: True if `header` signs `body` with `secret` and is recent.

    The timestamp is inside the signature, so a captured request cannot be
    replayed with a fresh timestamp, and an old one is refused by `tolerance`.
    """
    try:
        parts = dict(p.split("=", 1) for p in header.split(","))
        timestamp = int(parts["t"])
        given = parts["v1"]
    except (KeyError, ValueError):
        return False
    if abs((time.time() if now is None else now) - timestamp) > tolerance:
        return False
    return hmac.compare_digest(sign(secret, body, timestamp), f"t={timestamp},v1={given}")


def backoff(attempts: int, policy: Policy, rng: random.Random | None = None) -> float:
    """Seconds to wait after the `attempts`-th failure: doubling, capped, with jitter."""
    delay = min(policy.max_delay, policy.base_delay * 2 ** (attempts - 1))
    return delay * (rng or random).uniform(0.5, 1.0)


def body_of(event: dict) -> bytes:
    """The bytes sent for an event. Identical on every attempt."""
    return json.dumps({"id": event["id"], "type": event["event_type"],
                       "created_at": event["created_at"].isoformat(), "data": event["payload"]},
                      separators=(",", ":"), sort_keys=True).encode()


def claim(conn: psycopg.Connection, policy: Policy) -> list[dict]:
    rows = conn.cursor(row_factory=dict_row).execute(
        """UPDATE outbox
              SET attempts = attempts + 1,
                  next_attempt_at = now() + %(lease)s
            WHERE id IN (SELECT id FROM outbox
                          WHERE delivered_at IS NULL AND dead_at IS NULL AND next_attempt_at <= now()
                          ORDER BY next_attempt_at, id
                          LIMIT %(batch)s
                          FOR UPDATE SKIP LOCKED)
        RETURNING id, event_type, subject_id, payload, created_at, attempts""",
        {"lease": timedelta(seconds=policy.lease_seconds), "batch": policy.batch},
    ).fetchall()
    return sorted(rows, key=lambda r: r["id"])


def send(url: str, secret: str, event: dict, timeout: float) -> str | None:
    """POST one event. Returns None on a 2xx, else a short reason."""
    body = body_of(event)
    request = urllib.request.Request(url, data=body, method="POST", headers={
        "Content-Type": "application/json",
        "User-Agent": "ledger-relay",
        "Ledger-Event-Id": str(event["id"]),
        "Ledger-Event-Type": event["event_type"],
        "Ledger-Delivery-Attempt": str(event["attempts"]),
        SIGNATURE_HEADER: sign(secret, body, int(time.time())),
    })
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return None if 200 <= response.status < 300 else f"HTTP {response.status}"
    except urllib.error.HTTPError as exc:
        return f"HTTP {exc.code}"
    except (urllib.error.URLError, OSError) as exc:  # refused, DNS, timeout
        reason = getattr(exc, "reason", exc)
        return f"{type(reason).__name__}: {reason}"[:500]


def mark_delivered(conn: psycopg.Connection, event_id: int) -> None:
    conn.execute(
        """UPDATE outbox SET delivered_at = now(), last_error = NULL
            WHERE id = %s AND delivered_at IS NULL AND dead_at IS NULL""",
        (event_id,),
    )


def mark_failed(conn: psycopg.Connection, event: dict, error: str, policy: Policy) -> bool:
    """Schedule a retry, or mark dead. Returns True if this made the event dead."""
    dead = event["attempts"] >= policy.max_attempts
    conn.execute(
        """UPDATE outbox
              SET last_error = %(error)s,
                  next_attempt_at = now() + %(delay)s,
                  dead_at = CASE WHEN %(dead)s THEN now() END
            WHERE id = %(id)s AND attempts = %(attempts)s
              AND delivered_at IS NULL AND dead_at IS NULL""",
        {"error": error, "delay": timedelta(seconds=backoff(event["attempts"], policy)), "dead": dead,
         "id": event["id"], "attempts": event["attempts"]},
    )
    return dead


def run_once(conn: psycopg.Connection, url: str, secret: str, policy: Policy = Policy()) -> dict:
    """Claim and send batches until nothing is due. Returns counts."""
    counts = {"delivered": 0, "failed": 0, "dead": 0}
    while events := claim(conn, policy):
        for event in events:
            error = send(url, secret, event, policy.timeout)
            if error is None:
                mark_delivered(conn, event["id"])
                counts["delivered"] += 1
            elif mark_failed(conn, event, error, policy):
                counts["dead"] += 1
            else:
                counts["failed"] += 1
    return counts


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Deliver ledger events to a webhook receiver.")
    ap.add_argument("--url", required=True, help="the receiver's endpoint")
    ap.add_argument("--once", action="store_true", help="drain what is due, then exit")
    ap.add_argument("--poll", type=float, default=1.0, help="seconds to wait when nothing is due")
    ap.add_argument("--max-attempts", type=int, default=Policy.max_attempts)
    ap.add_argument("--timeout", type=float, default=Policy.timeout)
    ap.add_argument("--batch", type=int, default=Policy.batch)
    args = ap.parse_args(argv)

    secret = os.environ.get("LEDGER_WEBHOOK_SECRET", "")
    if len(secret) < MIN_SECRET_LENGTH:
        ap.error(f"set LEDGER_WEBHOOK_SECRET to a shared secret of at least {MIN_SECRET_LENGTH} characters")
    policy = Policy(max_attempts=args.max_attempts, timeout=args.timeout, batch=args.batch)

    with psycopg.connect(db.app_url(), autocommit=True) as conn:
        while True:
            counts = run_once(conn, args.url, secret, policy)
            if any(counts.values()):
                print(f"delivered {counts['delivered']}, will retry {counts['failed']}, dead {counts['dead']}",
                      flush=True)
            if args.once:
                return 0
            time.sleep(args.poll)


if __name__ == "__main__":
    raise SystemExit(main())
