BEGIN
WITH new_attr AS (
    INSERT INTO ${attr_table} (${attr_cols})
    VALUES (${new_cols})
    RETURNING id
)
UPDATE ${root_table}
SET ${attr_fk_col} = new_attr.id
FROM new_attr
WHERE ${root_table}.id = OLD.id;
RETURN NEW;
END;
