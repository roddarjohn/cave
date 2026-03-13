-- All operations go through the API view.
-- Only INSERT is allowed; UPDATE and DELETE are rejected.

-- Log a status change:
INSERT INTO api.order_events (value, order_id, status)
VALUES (1, 'ORD-001', 'placed');

-- Log multiple events at once:
INSERT INTO api.order_events (value, order_id, status)
VALUES
    (1, 'ORD-001', 'confirmed'),
    (1, 'ORD-002', 'placed');

-- Current status per order (most recent event):
SELECT * FROM ops.order_events_latest;
