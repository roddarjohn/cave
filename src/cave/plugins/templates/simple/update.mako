BEGIN
UPDATE ${base_table}
SET ${set_clause}
WHERE id = OLD.id
RETURNING * INTO NEW;
RETURN NEW;
END;
