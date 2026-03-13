-- Double-entry ledger: every entry_id must balance.
-- The constraint trigger validates debits = credits per entry_id.

-- Balanced entry (succeeds):
INSERT INTO api.journal (entry_id, value, direction, account_id)
VALUES
    ('cccccccc-0001-4000-8000-000000000001', 100, 'debit', 1),
    ('cccccccc-0001-4000-8000-000000000001', 100, 'credit', 2);

-- Unbalanced entry (rejected by the constraint trigger):
-- INSERT INTO api.journal (entry_id, value, direction, account_id)
-- VALUES
--     ('dddddddd-0001-4000-8000-000000000001', 100, 'debit', 1),
--     ('dddddddd-0001-4000-8000-000000000001', 50, 'credit', 2);
-- ERROR: double-entry violation for entry_id ...
