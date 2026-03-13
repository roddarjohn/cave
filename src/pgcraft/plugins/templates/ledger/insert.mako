BEGIN
INSERT INTO ${base_table} (${cols})
VALUES (${new_cols})
RETURNING * INTO NEW;
RETURN NEW;
END;
