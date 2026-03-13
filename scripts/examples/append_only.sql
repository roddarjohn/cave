-- All operations go through the API view.
-- The triggers manage the root and attributes tables for you.

INSERT INTO api.employees (name, department)
VALUES ('Alice', 'Engineering');

INSERT INTO api.employees (name, department)
VALUES ('Bob', 'Marketing');

-- Alice moves to Management.  This appends a new row to the
-- attributes table rather than updating in place.
UPDATE api.employees
SET department = 'Management'
WHERE id = 1;
