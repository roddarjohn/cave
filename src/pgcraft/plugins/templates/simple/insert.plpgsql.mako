BEGIN
INSERT INTO ${base_table} (${cols})
VALUES (${new_cols})
RETURNING ${returning_cols} INTO NEW;
RETURN NEW;
END;
