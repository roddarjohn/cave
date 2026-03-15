INSERT INTO public.customers (id, name, email)
VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO public.customers (id, name, email)
VALUES (2, 'Bob', 'bob@example.com');
INSERT INTO public.orders (id, customer_id, total, status)
VALUES (1, 1, 49.99, 'paid');
INSERT INTO public.orders (id, customer_id, total, status)
VALUES (2, 2, 120.00, 'pending');
INSERT INTO public.orders (id, customer_id, total, status)
VALUES (3, 1, 15.50, 'cancelled');
