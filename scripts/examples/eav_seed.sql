-- Insert directly into backing tables to simulate
-- what the INSTEAD OF triggers would do.
INSERT INTO private.products_entity (id)
VALUES (1), (2);
INSERT INTO private.products_attribute
(
    id, entity_id, attribute_name,
    string_value, float_value, boolean_value,
    integer_value
)
VALUES
(1, 1, 'color', 'red', NULL, NULL, NULL),
(2, 1, 'weight', NULL, 2.5, NULL, NULL),
(3, 1, 'is_active', NULL, NULL, TRUE, NULL),
(4, 1, 'price', NULL, NULL, NULL, 999),
(5, 2, 'color', 'blue', NULL, NULL, NULL),
(6, 2, 'weight', NULL, 1.0, NULL, NULL),
(7, 2, 'is_active', NULL, NULL, TRUE, NULL),
(8, 2, 'price', NULL, NULL, NULL, 499);
