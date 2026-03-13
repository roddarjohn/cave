-- All operations go through the API view.
-- Only INSERT is allowed; UPDATE and DELETE are rejected.

-- A single entry with auto-generated entry_id:
INSERT INTO api.transactions (value, account, category)
VALUES (100, 'cash', 'revenue');

-- Correlated entries sharing the same entry_id:
INSERT INTO api.transactions (entry_id, value, account, category)
VALUES
    ('bbbbbbbb-0001-4000-8000-000000000001', 250, 'cash', 'revenue'),
    ('bbbbbbbb-0001-4000-8000-000000000001', -250, 'accounts_receivable', 'revenue');

-- Balance per account:
SELECT account, SUM(value) AS balance
FROM api.transactions
GROUP BY account;
