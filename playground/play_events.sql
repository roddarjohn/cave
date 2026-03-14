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
-- 1. Simple event: ad-hoc stock adjustments
-- =====================================================================
-- The "adjust" event is a simple insert — fire and forget.

-- Receive goods at the east warehouse.
SELECT * FROM private.private_inventory_adjust(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_value     => 100,
    p_reason    => 'purchase_order_1001'
);

SELECT * FROM private.private_inventory_adjust(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-B',
    p_value     => 50,
    p_reason    => 'purchase_order_1001'
);

SELECT * FROM private.private_inventory_adjust(
    p_warehouse => 'west',
    p_sku       => 'WIDGET-A',
    p_value     => 75,
    p_reason    => 'purchase_order_1002'
);

-- Ship some out.
SELECT * FROM private.private_inventory_adjust(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_value     => -20,
    p_reason    => 'shipment_5001'
);

SELECT * FROM private.private_inventory_adjust(
    p_warehouse => 'west',
    p_sku       => 'WIDGET-A',
    p_value     => -10,
    p_reason    => 'shipment_5002'
);

-- Check current balances after adjustments.
\echo ''
\echo '=== Balances after adjustments ==='
SELECT warehouse, sku, balance
FROM private.inventory_balances
ORDER BY warehouse, sku;

-- Inspect the raw ledger entries.
\echo ''
\echo '=== All inventory entries so far ==='
SELECT id, warehouse, sku, value, reason, source, created_at
FROM private.api_inventory
ORDER BY id;

-- =====================================================================
-- 2. Diff event: reconciliation
-- =====================================================================
-- The "reconcile" event compares desired state against current
-- balances and inserts only the correcting deltas.

-- Warehouse count says east/WIDGET-A should be 100 (currently 80).
-- This inserts a +20 correcting row.
\echo ''
\echo '=== Reconcile east/WIDGET-A to 100 ==='
SELECT * FROM private.private_inventory_reconcile(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_value     => 100,
    p_source    => 'warehouse_count_march'
);

\echo ''
\echo '=== Balances after reconciliation ==='
SELECT warehouse, sku, balance
FROM private.inventory_balances
ORDER BY warehouse, sku;

-- =====================================================================
-- 3. Idempotency: calling reconcile again with same target is a no-op
-- =====================================================================
\echo ''
\echo '=== Reconcile east/WIDGET-A to 100 again (should return 0 rows) ==='
SELECT * FROM private.private_inventory_reconcile(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_value     => 100,
    p_source    => 'warehouse_count_march_recheck'
);

-- =====================================================================
-- 4. Reconcile to zero — zeroes out a position
-- =====================================================================
\echo ''
\echo '=== Reconcile west/WIDGET-A to 0 (sold out) ==='
SELECT * FROM private.private_inventory_reconcile(
    p_warehouse => 'west',
    p_sku       => 'WIDGET-A',
    p_value     => 0,
    p_source    => 'physical_count_west'
);

\echo ''
\echo '=== Balances after zeroing out west/WIDGET-A ==='
SELECT warehouse, sku, balance
FROM private.inventory_balances
ORDER BY warehouse, sku;

-- =====================================================================
-- 5. New SKU via reconcile — inserts full amount
-- =====================================================================
\echo ''
\echo '=== Reconcile new SKU east/GADGET-X to 200 ==='
SELECT * FROM private.private_inventory_reconcile(
    p_warehouse => 'east',
    p_sku       => 'GADGET-X',
    p_value     => 200,
    p_source    => 'new_product_launch'
);

-- =====================================================================
-- 6. Mix adjustments and reconciliations
-- =====================================================================

-- Some more adjustments come in during the day.
SELECT * FROM private.private_inventory_adjust(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-B',
    p_value     => -5,
    p_reason    => 'damaged_goods'
);

SELECT * FROM private.private_inventory_adjust(
    p_warehouse => 'east',
    p_sku       => 'GADGET-X',
    p_value     => -30,
    p_reason    => 'shipment_5010'
);

-- End-of-day reconcile for east/WIDGET-B to 40 (currently 45).
\echo ''
\echo '=== Reconcile east/WIDGET-B to 40 ==='
SELECT * FROM private.private_inventory_reconcile(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-B',
    p_value     => 40,
    p_source    => 'eod_count'
);

-- =====================================================================
-- 7. Final state
-- =====================================================================
\echo ''
\echo '=== Final balances ==='
SELECT warehouse, sku, balance
FROM private.inventory_balances
ORDER BY warehouse, sku;

\echo ''
\echo '=== Full ledger (all entries) ==='
SELECT id, warehouse, sku, value, reason, source, created_at
FROM private.api_inventory
ORDER BY id;

\echo ''
\echo '=== Entry count by type ==='
SELECT
    CASE
        WHEN source IS NOT NULL THEN 'reconcile'
        WHEN reason IS NOT NULL THEN 'adjust'
        ELSE 'direct'
    END AS event_type,
    count(*) AS entries,
    sum(value) AS net_value
FROM private.api_inventory
GROUP BY 1
ORDER BY 1;
