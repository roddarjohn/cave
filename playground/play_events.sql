-- =====================================================================
-- Playground: ledger events
--
-- Run this after setting up the playground database:
--   cd playground && just init && just fr "initial" && just migrate
--
-- Then paste this into a psql session:
--   just db-shell < play_events.sql
-- =====================================================================

-- =====================================================================
-- 1. Create invoices (append-only) and invoice lines (EAV)
-- =====================================================================

INSERT INTO private.invoices (customer_id, amount) VALUES (1, 700), (2, 450);

\echo ''
\echo '=== Invoices ==='
SELECT * FROM private.invoices ORDER BY id;

INSERT INTO private.invoice_lines (invoice_id, department, amount)
VALUES
    (1, 'consulting',  500),
    (1, 'hosting',     200),
    (2, 'consulting',  300),
    (2, 'support',     150);

\echo ''
\echo '=== Invoice lines ==='
SELECT * FROM private.invoice_lines ORDER BY id;

-- =====================================================================
-- 2. Post invoices 1 and 2 to the ledger
-- =====================================================================
-- Each line produces two balanced entries:
--   debit  accounts_receivable
--   credit revenue

\echo ''
\echo '=== Post invoices 1, 2 ==='
SELECT * FROM private.private_ledger_post(
    p_invoice_ids => ARRAY[1, 2]
);

\echo ''
\echo '=== Ledger balances after posting ==='
SELECT invoice_id, department, account, balance
FROM private.ledger_balances
ORDER BY invoice_id, department, account;

-- =====================================================================
-- 3. Idempotency: calling again with same invoices is a no-op
-- =====================================================================

\echo ''
\echo '=== Post again (should return 0 rows) ==='
SELECT * FROM private.private_ledger_post(
    p_invoice_ids => ARRAY[1, 2]
);

-- =====================================================================
-- 4. Amend a line item via EAV, then re-post
-- =====================================================================
-- Update invoice 1 consulting from 500 to 600 (EAV update).
-- Re-posting inserts correcting +100 entries for that department.

UPDATE private.invoice_lines
SET amount = 600
WHERE invoice_id = 1 AND department = 'consulting';

\echo ''
\echo '=== Re-post after amending invoice 1 ==='
SELECT * FROM private.private_ledger_post(
    p_invoice_ids => ARRAY[1]
);

\echo ''
\echo '=== Ledger balances after amendment ==='
SELECT invoice_id, department, account, balance
FROM private.ledger_balances
ORDER BY invoice_id, department, account;

-- =====================================================================
-- 5. Full ledger (all entries)
-- =====================================================================

\echo ''
\echo '=== Full ledger ==='
SELECT id, entry_id, invoice_id, department, account, direction, value, created_at
FROM private.ledger
ORDER BY id;

-- =====================================================================
-- 6. Simple event: ad-hoc inventory adjustment
-- =====================================================================
-- Simple events insert directly without diffing.

SELECT * FROM private.private_inventory_adjust(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_value     => 100,
    p_reason    => 'purchase_order_1001'
);

SELECT * FROM private.private_inventory_adjust(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_value     => -20,
    p_reason    => 'shipment_5001'
);

\echo ''
\echo '=== Inventory balances ==='
SELECT warehouse, sku, balance
FROM private.inventory_balances
ORDER BY warehouse, sku;
