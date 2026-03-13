-- All operations go through the API view.
-- The INSTEAD OF triggers route them to the backing table.

INSERT INTO api.users (name, email)
VALUES ('Alice', 'alice@example.com');

INSERT INTO api.users (name, email)
VALUES ('Bob', 'bob@example.com');

UPDATE api.users
SET email = 'alice@newdomain.com'
WHERE id = 1;

DELETE FROM api.users WHERE id = 2;
