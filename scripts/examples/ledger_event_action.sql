-- Simple event: explicit delta insert.
--
-- The caller specifies the exact delta value to record.
-- One function call per event.
--
-- Generated function for LedgerEvent(name="adjust"):
--   ops.ops_inventory_adjust(
--       p_warehouse VARCHAR, p_sku VARCHAR,
--       p_value INTEGER, p_reason VARCHAR
--   ) RETURNS SETOF api.inventory

-- Record a positive delta (goods received):
SELECT * FROM ops.ops_inventory_adjust(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_value     => 50,
    p_reason    => 'purchase_order_9821'
);

-- Record a negative delta (goods shipped):
SELECT * FROM ops.ops_inventory_adjust(
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_value     => -10,
    p_reason    => 'shipment_5034'
);

-- Current balances:
SELECT warehouse, sku, balance
FROM ops.inventory_balances
ORDER BY warehouse, sku;
