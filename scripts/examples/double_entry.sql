-- Double-entry ledger: every entry_id must balance.
-- The constraint trigger validates debits = credits per entry_id.

-- Balanced entry (succeeds):
INSERT INTO api.journal (entry_id, value, direction, account)
VALUES
    ('cccccccc-0001-4000-8000-000000000001', 100, 'debit', 'cash'),
    ('cccccccc-0001-4000-8000-000000000001', 100, 'credit', 'revenue');

-- Unbalanced entry (rejected by the constraint trigger):
-- INSERT INTO api.journal (entry_id, value, direction, account)
-- VALUES
--     ('dddddddd-0001-4000-8000-000000000001', 100, 'debit', 'cash'),
--     ('dddddddd-0001-4000-8000-000000000001', 50, 'credit', 'revenue');
-- ERROR: double-entry violation for entry_id ...
