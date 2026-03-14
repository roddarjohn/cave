-- All operations go through the API view.
-- The triggers decompose each column into attribute rows
-- in the underlying EAV tables.

INSERT INTO api.products (color, weight, is_active, price)
VALUES ('red', 2.5, TRUE, 999);

INSERT INTO api.products (color, weight, is_active, price)
VALUES ('blue', 1.0, TRUE, 499);

-- The pivot view reconstructs columns, so SELECTs look normal:
SELECT * FROM api.products;
