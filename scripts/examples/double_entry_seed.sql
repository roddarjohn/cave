INSERT INTO finance.accounts (id, name, category)
VALUES
    (1, 'cash', 'asset'),
    (2, 'revenue', 'income'),
    (3, 'supplies', 'expense');

INSERT INTO finance.journal (id, entry_id, value, direction, account_id)
VALUES
    (1, 'aaaaaaaa-0001-4000-8000-000000000001', 500, 'debit', 1),
    (2, 'aaaaaaaa-0001-4000-8000-000000000001', 500, 'credit', 2),
    (3, 'aaaaaaaa-0002-4000-8000-000000000002', 200, 'debit', 3),
    (4, 'aaaaaaaa-0002-4000-8000-000000000002', 200, 'credit', 1);
