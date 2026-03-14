-- EventAction: explicit delta insert.
--
-- The caller specifies the exact delta value to record.
-- No staging table; one function call per event.
--
-- Generated function for EventAction(name="adjust"):
--   ops.inventory_adjust(
--       p_value    INTEGER,
--       p_warehouse TEXT,
--       p_sku       TEXT,
--       p_reason    TEXT DEFAULT NULL
--   )

-- Record a positive delta (goods received):
SELECT ops.inventory_adjust(
    p_value     => 50,
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_reason    => 'purchase_order_9821'
);

-- Record a negative delta (goods shipped):
SELECT ops.inventory_adjust(
    p_value     => -10,
    p_warehouse => 'east',
    p_sku       => 'WIDGET-A',
    p_reason    => 'shipment_5034'
);

-- Reason is optional (defaults to NULL):
SELECT ops.inventory_adjust(
    p_value     => 5,
    p_warehouse => 'west',
    p_sku       => 'WIDGET-B'
);

-- Current balances:
SELECT warehouse, sku, balance
FROM ops.inventory_balances
ORDER BY warehouse, sku;
