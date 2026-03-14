-- Diff event: declarative reconciliation.
--
-- The caller describes the *desired* stock levels; the system
-- computes and inserts only the correcting delta rows.
--
-- Generated function for LedgerEvent(name="reconcile"):
--   ops.ops_inventory_reconcile(
--       p_warehouse TEXT, p_sku TEXT, p_value INTEGER, p_source VARCHAR
--   ) RETURNS SETOF api.inventory

-- Reconcile to desired stock levels:
SELECT * FROM ops.ops_inventory_reconcile(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_value     => 200,
    p_source    => 'monthly_count'
);
-- Returns the inserted correcting rows.

-- Run again with the same desired state — idempotent, no new rows:
SELECT * FROM ops.ops_inventory_reconcile(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_value     => 200,
    p_source    => 'monthly_count'
);
-- Returns empty set (no correction needed).

-- Current balances are visible through the balance view:
SELECT warehouse, sku, balance
FROM ops.inventory_balances
ORDER BY warehouse, sku;
