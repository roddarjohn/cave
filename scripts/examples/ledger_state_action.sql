-- StateAction: declarative reconciliation.
--
-- The caller describes the *desired* stock levels; the system
-- computes and inserts only the correcting delta rows.
--
-- Generated functions for StateAction(name="reconcile"):
--   ops.inventory_reconcile_begin()
--   ops.inventory_reconcile_apply(p_source TEXT DEFAULT NULL)

-- Step 1: open the staging session.
SELECT ops.inventory_reconcile_begin();

-- Step 2: populate the staging table with desired stock levels.
INSERT INTO _inventory_reconcile (warehouse, sku, value)
VALUES
    ('east', 'WIDGET-A', 200),
    ('east', 'WIDGET-B', 50),
    ('west', 'WIDGET-A', 100);

-- Step 3: apply — inserts only the correcting entries and returns
-- the number of delta rows written.
SELECT * FROM ops.inventory_reconcile_apply(p_source => 'monthly_count');
-- Returns: delta = 3  (all three are new)

-- Run again with the same desired state — idempotent, no new rows:
SELECT ops.inventory_reconcile_begin();
INSERT INTO _inventory_reconcile (warehouse, sku, value)
VALUES
    ('east', 'WIDGET-A', 200),
    ('east', 'WIDGET-B', 50),
    ('west', 'WIDGET-A', 100);
SELECT * FROM ops.inventory_reconcile_apply(p_source => 'monthly_count');
-- Returns: delta = 0

-- Current balances are visible through the balance view:
SELECT warehouse, sku, balance
FROM ops.inventory_balances
ORDER BY warehouse, sku;
