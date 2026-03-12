BEGIN
WITH new_attr AS (
    INSERT INTO ${attr_table} (${attr_cols})
    VALUES (${new_cols})
    RETURNING id
)
INSERT INTO ${root_table} (${attr_fk_col})
SELECT id FROM new_attr;
RETURN NEW;
END;
