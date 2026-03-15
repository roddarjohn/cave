BEGIN
UPDATE ${base_table}
SET ${set_clause}
WHERE id = OLD.id
RETURNING ${returning_cols} INTO NEW;
RETURN NEW;
END;
