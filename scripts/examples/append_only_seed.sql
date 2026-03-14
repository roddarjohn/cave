-- Insert directly into backing tables to simulate
-- what the INSTEAD OF triggers would do.
INSERT INTO private.employees_attributes
(id, name, department)
VALUES
(1, 'Alice', 'Engineering'),
(2, 'Bob', 'Marketing'),
(3, 'Alice', 'Management');
INSERT INTO private.employees_root
(id, employees_attributes_id)
VALUES
(1, 3),
(2, 2);
