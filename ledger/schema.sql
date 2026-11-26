-- Double-entry ledger schema. Safe to re-run.
--
-- Amounts are bigint minor units (paise for INR). There are no floats anywhere.
-- The database, not the application, enforces the rules that keep money correct:
--   1. every transaction's entries sum to zero, checked at COMMIT
--   2. an account's balance is derived from its entries by a trigger and
--      cannot be written directly
--   3. entries and transactions are append-only; mistakes are fixed by reversal
--   4. an entry's currency must match both its account and its transaction

CREATE TABLE IF NOT EXISTS accounts (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name           text        NOT NULL UNIQUE,
    currency       char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    allow_negative boolean     NOT NULL DEFAULT false,
    balance        bigint      NOT NULL DEFAULT 0,
    created_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (id, currency),
    CONSTRAINT balance_floor CHECK (allow_negative OR balance >= 0)
);

CREATE TABLE IF NOT EXISTS transactions (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key text        NOT NULL UNIQUE,
    request_hash    text        NOT NULL,
    currency        char(3)     NOT NULL,
    memo            text,
    external_ref    text,
    -- A transaction can be reversed at most once: the UNIQUE makes a second
    -- reversal fail even when two arrive at the same instant.
    reverses_id     bigint UNIQUE REFERENCES transactions (id),
    created_at      timestamptz NOT NULL DEFAULT now(),
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


-- Rule 2a: inserting an entry moves its account's balance.
CREATE OR REPLACE FUNCTION ledger_apply_entry() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE accounts SET balance = balance + NEW.amount WHERE id = NEW.account_id;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS entries_apply ON entries;
CREATE TRIGGER entries_apply
    AFTER INSERT ON entries
    FOR EACH ROW EXECUTE FUNCTION ledger_apply_entry();


-- Rule 2b: nothing else may write accounts.balance. The update issued by
-- ledger_apply_entry runs at trigger depth 2; a direct UPDATE runs at depth 1.
CREATE OR REPLACE FUNCTION ledger_guard_balance() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.balance <> 0 THEN
        RAISE EXCEPTION 'accounts open at balance 0; post a transaction to fund one'
            USING ERRCODE = 'check_violation';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.balance IS DISTINCT FROM OLD.balance AND pg_trigger_depth() < 2 THEN
        RAISE EXCEPTION 'balance is derived from entries; post a transaction instead'
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS accounts_guard_balance ON accounts;
CREATE TRIGGER accounts_guard_balance
    BEFORE INSERT OR UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION ledger_guard_balance();


-- Rule 3: history is append-only.
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


-- Audit view: any row here means a stored balance disagrees with its entries.
-- It should always be empty; /health reports it.
CREATE OR REPLACE VIEW ledger_drift AS
SELECT a.id, a.name, a.balance, coalesce(sum(e.amount), 0) AS entries_sum
FROM accounts a
LEFT JOIN entries e ON e.account_id = a.id
GROUP BY a.id
HAVING a.balance <> coalesce(sum(e.amount), 0);
