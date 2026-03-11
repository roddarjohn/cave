BEGIN
DELETE FROM ${entity_table} WHERE id = OLD.id;
RETURN OLD;
END;
