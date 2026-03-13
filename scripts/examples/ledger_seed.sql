INSERT INTO ops.order_events (id, entry_id, created_at, value, order_id, status)
VALUES
    (1, 'aaaaaaaa-0001-4000-8000-000000000001', '2025-01-15 09:00:00+00', 1, 'ORD-001', 'placed'),
    (2, 'aaaaaaaa-0002-4000-8000-000000000002', '2025-01-15 09:05:00+00', 1, 'ORD-002', 'placed'),
    (3, 'aaaaaaaa-0003-4000-8000-000000000003', '2025-01-15 10:30:00+00', 1, 'ORD-001', 'confirmed'),
    (4, 'aaaaaaaa-0004-4000-8000-000000000004', '2025-01-16 14:00:00+00', 1, 'ORD-001', 'shipped'),
    (5, 'aaaaaaaa-0005-4000-8000-000000000005', '2025-01-16 15:00:00+00', 1, 'ORD-002', 'confirmed');
