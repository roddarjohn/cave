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

INSERT INTO private.invoice_lines (invoice_id, account, amount)
VALUES
    (1, 'consulting',  500),
    (1, 'hosting',     200),
    (2, 'consulting',  300),
    (2, 'support',     150);

\echo ''
\echo '=== Invoice lines ==='
SELECT * FROM private.invoice_lines ORDER BY id;

-- =====================================================================
-- 2. Recognize revenue for invoices 1 and 2
-- =====================================================================
-- The reconcile event joins invoice_lines to produce one ledger entry
-- per line item, diffing against existing balances.

\echo ''
\echo '=== Recognize revenue for invoices 1, 2 ==='
SELECT * FROM private.private_revenue_recognize(
    p_invoice_ids => ARRAY[1, 2]
);

\echo ''
\echo '=== Revenue balances after recognition ==='
SELECT invoice_id, account, balance
FROM private.revenue_balances
ORDER BY invoice_id, account;

-- =====================================================================
-- 3. Idempotency: calling again with same invoices is a no-op
-- =====================================================================

\echo ''
\echo '=== Recognize again (should return 0 rows) ==='
SELECT * FROM private.private_revenue_recognize(
    p_invoice_ids => ARRAY[1, 2]
);

-- =====================================================================
-- 4. Amend a line item via EAV, then re-reconcile
-- =====================================================================
-- Update invoice 1 consulting from 500 to 600 (EAV update).
-- Re-reconciling inserts a single correcting +100 entry.

UPDATE private.invoice_lines
SET amount = 600
WHERE invoice_id = 1 AND account = 'consulting';

\echo ''
\echo '=== Re-reconcile after amending invoice 1 ==='
SELECT * FROM private.private_revenue_recognize(
    p_invoice_ids => ARRAY[1]
);

\echo ''
\echo '=== Revenue balances after amendment ==='
SELECT invoice_id, account, balance
FROM private.revenue_balances
ORDER BY invoice_id, account;

-- =====================================================================
-- 5. Full ledger (all entries)
-- =====================================================================

\echo ''
\echo '=== Full revenue ledger ==='
SELECT id, invoice_id, account, value, created_at
FROM private.revenue
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
