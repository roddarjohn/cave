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
-- 1. Seed invoice line items (the transactional source)
-- =====================================================================

INSERT INTO private.invoice_lines (id, invoice_id, account, amount)
VALUES
    (1, 1001, 'consulting',  500),
    (2, 1001, 'hosting',     200),
    (3, 1002, 'consulting',  300),
    (4, 1002, 'support',     150);

\echo ''
\echo '=== Invoice lines ==='
SELECT * FROM private.invoice_lines ORDER BY id;

-- =====================================================================
-- 2. Recognize revenue for invoices 1001 and 1002
-- =====================================================================
-- The reconcile event joins invoice_lines to produce one ledger entry
-- per line item, diffing against existing balances.

\echo ''
\echo '=== Recognize revenue for invoices 1001, 1002 ==='
SELECT * FROM private.private_revenue_recognize(
    p_invoice_ids => ARRAY[1001, 1002]
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
    p_invoice_ids => ARRAY[1001, 1002]
);

-- =====================================================================
-- 4. Amend a line item, then re-reconcile
-- =====================================================================
-- Update invoice 1001 consulting from 500 to 600.
-- Re-reconciling inserts a single correcting +100 entry.

UPDATE private.invoice_lines SET amount = 600 WHERE id = 1;

\echo ''
\echo '=== Re-reconcile after amending invoice 1001 ==='
SELECT * FROM private.private_revenue_recognize(
    p_invoice_ids => ARRAY[1001]
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
FROM private.api_revenue
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
