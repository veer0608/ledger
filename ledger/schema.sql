-- Double-entry ledger schema. Run as the OWNER role (see db.migrate). Safe to re-run.
--
-- Amounts are bigint minor units (paise for INR). There are no floats anywhere.
--
-- Two roles:
--   owner       runs this file, owns the tables. Trusted, like any DBA.
--   ledger_app  what the service connects as. It can read, insert, and call
--               two SECURITY DEFINER functions. It cannot UPDATE, DELETE,
--               TRUNCATE, ALTER, disable triggers, or create an account that
--               may go negative. Every rule below holds against this role.
--
-- The rules:
--   1. every transaction's entries sum to zero, checked at COMMIT
--   2. an account's balance is derived from its entries by a trigger and
--      cannot be written directly
--   3. entries and transactions are append-only; mistakes are fixed by reversal
--   4. an entry's currency must match both its account and its transaction
--   5. an external reference (UTR) is posted at most once, unless reversed
--   6. money on hold cannot be spent, a hold resolves exactly once, and a
--      capture moves exactly what it says to the hold's payee

CREATE TABLE IF NOT EXISTS accounts (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name           text        NOT NULL UNIQUE,
    currency       char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    allow_negative boolean     NOT NULL DEFAULT false,
    balance        bigint      NOT NULL DEFAULT 0,
    -- Sum of this account's pending holds. Available = balance - held.
    held           bigint      NOT NULL DEFAULT 0 CONSTRAINT held_nonnegative CHECK (held >= 0),
    created_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (id, currency),
    CONSTRAINT balance_floor CHECK (allow_negative OR balance - held >= 0)
);

-- Upgrade a database created before holds existed.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = current_schema() AND table_name = 'accounts' AND column_name = 'held') THEN
        ALTER TABLE accounts ADD COLUMN held bigint NOT NULL DEFAULT 0 CONSTRAINT held_nonnegative CHECK (held >= 0);
        ALTER TABLE accounts DROP CONSTRAINT balance_floor,
                             ADD CONSTRAINT balance_floor CHECK (allow_negative OR balance - held >= 0);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS transactions (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Idempotency keys are scoped to the client that sent them, so two
    -- clients that both number their requests from 1 never collide.
    client_id       text        NOT NULL CHECK (length(client_id) BETWEEN 1 AND 100),
    idempotency_key text        NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    request_hash    text        NOT NULL,
    currency        char(3)     NOT NULL,
    memo            text,
    external_ref    text,
    -- A transaction can be reversed at most once: the UNIQUE makes a second
    -- reversal fail even when two arrive at the same instant.
    reverses_id     bigint UNIQUE REFERENCES transactions (id),
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, idempotency_key),
    UNIQUE (id, currency)
);

CREATE TABLE IF NOT EXISTS entries (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id bigint      NOT NULL,
    account_id     bigint      NOT NULL,
    currency       char(3)     NOT NULL,
    amount         bigint      NOT NULL CHECK (amount <> 0),
    created_at     timestamptz NOT NULL DEFAULT now(),
    -- Composite keys tie the entry's currency to both parents, so a USD entry
    -- can never land on an INR account or inside an INR transaction.
    FOREIGN KEY (transaction_id, currency) REFERENCES transactions (id, currency),
    FOREIGN KEY (account_id, currency)     REFERENCES accounts (id, currency)
);

CREATE INDEX IF NOT EXISTS entries_account_idx     ON entries (account_id, id);
CREATE INDEX IF NOT EXISTS entries_transaction_idx ON entries (transaction_id);
CREATE INDEX IF NOT EXISTS transactions_ref_idx    ON transactions (external_ref) WHERE external_ref IS NOT NULL;


-- Rule 2a: inserting an entry moves its account's balance. SECURITY DEFINER
-- because the app role has no UPDATE on accounts; this function is the only
-- path by which a balance changes.
CREATE OR REPLACE FUNCTION ledger_apply_entry() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    UPDATE accounts SET balance = balance + NEW.amount WHERE id = NEW.account_id;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS entries_apply ON entries;
CREATE TRIGGER entries_apply
    AFTER INSERT ON entries
    FOR EACH ROW EXECUTE FUNCTION ledger_apply_entry();


-- Rule 2b: nothing else may write accounts.balance, not even the owner short
-- of disabling triggers. The update issued by ledger_apply_entry runs at
-- trigger depth 2; a direct UPDATE runs at depth 1.
CREATE OR REPLACE FUNCTION ledger_guard_balance() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' AND (NEW.balance <> 0 OR NEW.held <> 0) THEN
        RAISE EXCEPTION 'accounts open at balance 0 with nothing held; post a transaction to fund one'
            USING ERRCODE = 'check_violation';
    END IF;
    IF TG_OP = 'UPDATE' AND pg_trigger_depth() < 2
       AND (NEW.balance IS DISTINCT FROM OLD.balance OR NEW.held IS DISTINCT FROM OLD.held) THEN
        RAISE EXCEPTION 'balance and held are derived from entries and holds; post a transaction instead'
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS accounts_guard_balance ON accounts;
CREATE TRIGGER accounts_guard_balance
    BEFORE INSERT OR UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION ledger_guard_balance();


-- Rule 3: history is append-only. The app role has no UPDATE/DELETE/TRUNCATE
-- grant at all; these triggers also stop the owner's row-level edits.
CREATE OR REPLACE FUNCTION ledger_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; post a reversal instead', TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation';
END $$;

DROP TRIGGER IF EXISTS entries_append_only ON entries;
CREATE TRIGGER entries_append_only
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();

DROP TRIGGER IF EXISTS transactions_append_only ON transactions;
CREATE TRIGGER transactions_append_only
    BEFORE UPDATE OR DELETE ON transactions
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();


-- Rule 1: at COMMIT, every transaction has two or more entries summing to zero.
-- Deferred so the entries of one transaction can be inserted one at a time.
-- It fires from both tables so a transaction with no entries at all is caught too.
CREATE OR REPLACE FUNCTION ledger_check_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    txn   bigint;
    total bigint;
    legs  int;
BEGIN
    IF TG_TABLE_NAME = 'entries' THEN
        txn := NEW.transaction_id;
    ELSE
        txn := NEW.id;
    END IF;

    SELECT coalesce(sum(amount), 0), count(*) INTO total, legs
    FROM entries WHERE transaction_id = txn;

    IF legs < 2 THEN
        RAISE EXCEPTION 'transaction % has % entries; needs at least 2', txn, legs
            USING ERRCODE = 'check_violation';
    END IF;
    IF total <> 0 THEN
        RAISE EXCEPTION 'transaction % does not balance: entries sum to %', txn, total
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS entries_balanced ON entries;
CREATE CONSTRAINT TRIGGER entries_balanced
    AFTER INSERT ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_check_balanced();

DROP TRIGGER IF EXISTS transactions_balanced ON transactions;
CREATE CONSTRAINT TRIGGER transactions_balanced
    AFTER INSERT ON transactions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_check_balanced();


-- Rule 5: one live posting per external reference. A webhook redelivered
-- under a fresh idempotency key must not credit the same UTR twice. A plain
-- UNIQUE index cannot express "unless that posting was reversed" (the usual
-- fix for money sent to the wrong account), so a trigger checks it,
-- serialised per reference by an advisory lock so two arriving together
-- cannot both pass. Reversals carry the original's reference and are exempt.
CREATE OR REPLACE FUNCTION ledger_one_live_ref() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    existing bigint;
BEGIN
    IF NEW.external_ref IS NULL OR NEW.reverses_id IS NOT NULL THEN
        RETURN NEW;
    END IF;
    PERFORM pg_advisory_xact_lock(724502, hashtext(NEW.external_ref));
    -- Read committed: this statement's snapshot is taken after the lock is
    -- granted, so it sees a competing posting that just committed.
    SELECT t.id INTO existing
    FROM transactions t
    WHERE t.external_ref = NEW.external_ref
      AND t.reverses_id IS NULL
      AND NOT EXISTS (SELECT 1 FROM transactions r WHERE r.reverses_id = t.id)
    LIMIT 1;
    IF existing IS NOT NULL THEN
        RAISE EXCEPTION 'external_ref % is already posted as transaction %', NEW.external_ref, existing
            USING ERRCODE = 'unique_violation', CONSTRAINT = 'transactions_external_ref_live';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS transactions_one_live_ref ON transactions;
CREATE TRIGGER transactions_one_live_ref
    BEFORE INSERT ON transactions
    FOR EACH ROW EXECUTE FUNCTION ledger_one_live_ref();


-- ------------------------------------------------------------------ holds
-- A hold reserves money now and moves it later: a card authorisation, a UPI
-- mandate, a hotel pre-auth. While pending it lowers the account's available
-- balance (balance - held) without moving anything. It ends exactly once:
--   captured  money moves to the payee (all of it, or less: a partial capture)
--   voided    released, nothing moves
--   expired   released by the expiry job after expires_at
CREATE TABLE IF NOT EXISTS holds (
    id                     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_id              text        NOT NULL CHECK (length(client_id) BETWEEN 1 AND 100),
    idempotency_key        text        NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    request_hash           text        NOT NULL,
    account_id             bigint      NOT NULL,
    payee_id               bigint      NOT NULL,
    currency               char(3)     NOT NULL,
    amount                 bigint      NOT NULL CHECK (amount > 0),
    status                 text        NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending', 'captured', 'voided', 'expired')),
    captured_amount        bigint      CHECK (captured_amount > 0 AND captured_amount <= amount),
    capture_transaction_id bigint      UNIQUE REFERENCES transactions (id),
    expires_at             timestamptz NOT NULL,
    created_at             timestamptz NOT NULL DEFAULT now(),
    resolved_at            timestamptz,
    UNIQUE (client_id, idempotency_key),
    FOREIGN KEY (account_id, currency) REFERENCES accounts (id, currency),
    FOREIGN KEY (payee_id, currency)   REFERENCES accounts (id, currency),
    CHECK (account_id <> payee_id),
    CHECK (expires_at > created_at),
    CHECK ((status = 'captured') = (capture_transaction_id IS NOT NULL AND captured_amount IS NOT NULL)),
    CHECK ((status = 'pending') = (resolved_at IS NULL))
);

CREATE INDEX IF NOT EXISTS holds_pending_expiry_idx ON holds (expires_at) WHERE status = 'pending';


-- A hold changes state once, from pending, and nothing but its resolution
-- may change. The clock rules live here too, so no code path can capture an
-- expired hold or expire a live one.
CREATE OR REPLACE FUNCTION ledger_hold_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'holds are never deleted; void one instead' USING ERRCODE = 'restrict_violation';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'pending' THEN
            RAISE EXCEPTION 'a hold starts pending' USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.status <> 'pending' THEN
        RAISE EXCEPTION 'hold % is already %', OLD.id, OLD.status USING ERRCODE = 'restrict_violation';
    END IF;
    IF NEW.status = 'pending' THEN
        RAISE EXCEPTION 'an update to hold % must resolve it', OLD.id USING ERRCODE = 'restrict_violation';
    END IF;
    IF (NEW.id, NEW.client_id, NEW.idempotency_key, NEW.request_hash, NEW.account_id, NEW.payee_id,
        NEW.currency, NEW.amount, NEW.expires_at, NEW.created_at)
       IS DISTINCT FROM
       (OLD.id, OLD.client_id, OLD.idempotency_key, OLD.request_hash, OLD.account_id, OLD.payee_id,
        OLD.currency, OLD.amount, OLD.expires_at, OLD.created_at) THEN
        RAISE EXCEPTION 'only the resolution of hold % can change', OLD.id USING ERRCODE = 'restrict_violation';
    END IF;
    IF NEW.status = 'captured' AND now() >= OLD.expires_at THEN
        RAISE EXCEPTION 'hold % expired at %', OLD.id, OLD.expires_at USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.status = 'expired' AND now() < OLD.expires_at THEN
        RAISE EXCEPTION 'hold % does not expire until %', OLD.id, OLD.expires_at USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS holds_guard ON holds;
CREATE TRIGGER holds_guard
    BEFORE INSERT OR UPDATE OR DELETE ON holds
    FOR EACH ROW EXECUTE FUNCTION ledger_hold_guard();


-- accounts.held is derived from pending holds, the same way balance is
-- derived from entries. Opening a hold raises it; resolving one lowers it.
CREATE OR REPLACE FUNCTION ledger_hold_apply() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE accounts SET held = held + NEW.amount WHERE id = NEW.account_id;
    ELSIF OLD.status = 'pending' AND NEW.status <> 'pending' THEN
        UPDATE accounts SET held = held - OLD.amount WHERE id = OLD.account_id;
    END IF;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS holds_apply ON holds;
CREATE TRIGGER holds_apply
    AFTER INSERT OR UPDATE ON holds
    FOR EACH ROW EXECUTE FUNCTION ledger_hold_apply();


-- At COMMIT, a captured hold must point at a transaction that moved exactly
-- captured_amount from the hold's account to its payee. Without this, the app
-- role could mark a hold captured against any transaction and release the
-- reservation without paying the payee.
CREATE OR REPLACE FUNCTION ledger_check_capture() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status <> 'captured' THEN
        RETURN NULL;
    END IF;
    IF (SELECT array_agg(ARRAY[account_id, amount] ORDER BY account_id, amount)
        FROM entries WHERE transaction_id = NEW.capture_transaction_id)
       IS DISTINCT FROM
       (SELECT array_agg(ARRAY[a, m] ORDER BY a, m)
        FROM (VALUES (NEW.account_id, -NEW.captured_amount), (NEW.payee_id, NEW.captured_amount)) v (a, m)) THEN
        RAISE EXCEPTION 'hold % is marked captured but transaction % does not move % from account % to %',
            NEW.id, NEW.capture_transaction_id, NEW.captured_amount, NEW.account_id, NEW.payee_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS holds_capture_matches ON holds;
CREATE CONSTRAINT TRIGGER holds_capture_matches
    AFTER UPDATE ON holds
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_check_capture();


CREATE OR REPLACE VIEW ledger_held_drift AS
SELECT a.id, a.name, a.held,
       coalesce(sum(h.amount) FILTER (WHERE h.status = 'pending'), 0) AS pending_sum
FROM accounts a
LEFT JOIN holds h ON h.account_id = a.id
GROUP BY a.id
HAVING a.held <> coalesce(sum(h.amount) FILTER (WHERE h.status = 'pending'), 0);


-- Row locks for a posting, taken in id order so opposite transfers queue
-- instead of deadlocking. SECURITY DEFINER because SELECT ... FOR UPDATE
-- needs UPDATE privilege, which the app role deliberately does not have.
CREATE OR REPLACE FUNCTION ledger_lock_accounts(ids bigint[]) RETURNS SETOF accounts
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT * FROM accounts WHERE id = ANY(ids) ORDER BY id FOR UPDATE
$$;


-- Audit view: any row here means a stored balance disagrees with its entries.
-- It should always be empty; /health reports it.
CREATE OR REPLACE VIEW ledger_drift AS
SELECT a.id, a.name, a.balance, coalesce(sum(e.amount), 0) AS entries_sum
FROM accounts a
LEFT JOIN entries e ON e.account_id = a.id
GROUP BY a.id
HAVING a.balance <> coalesce(sum(e.amount), 0);


-- The app role. Created without LOGIN; db.migrate sets its password.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ledger_app') THEN
        CREATE ROLE ledger_app NOLOGIN;
    END IF;
END $$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO ledger_app;

REVOKE ALL ON accounts, transactions, entries, ledger_drift FROM PUBLIC, ledger_app;
GRANT SELECT ON accounts, transactions, entries, ledger_drift TO ledger_app;
-- Column-level INSERT grants: the app cannot set a balance, cannot backdate
-- created_at, and cannot create an account with allow_negative (a mint).
-- Those are created by the owner.
GRANT INSERT (name, currency) ON accounts TO ledger_app;
GRANT INSERT (client_id, idempotency_key, request_hash, currency, memo, external_ref, reverses_id)
    ON transactions TO ledger_app;
GRANT INSERT (transaction_id, account_id, currency, amount) ON entries TO ledger_app;

-- Holds: the app opens them, and resolves them by changing only the
-- resolution columns. The guard trigger decides which changes are legal.
REVOKE ALL ON holds, ledger_held_drift FROM PUBLIC, ledger_app;
GRANT SELECT ON holds, ledger_held_drift TO ledger_app;
GRANT INSERT (client_id, idempotency_key, request_hash, account_id, payee_id, currency, amount, expires_at)
    ON holds TO ledger_app;
GRANT UPDATE (status, captured_amount, capture_transaction_id, resolved_at) ON holds TO ledger_app;

REVOKE ALL ON FUNCTION ledger_apply_entry(), ledger_hold_apply(), ledger_lock_accounts(bigint[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ledger_lock_accounts(bigint[]) TO ledger_app;
