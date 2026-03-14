DECLARE
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS ${staging_table} (
        ${',\n        '.join(col_defs)}
    ) ON COMMIT DELETE ROWS;
    TRUNCATE ${staging_table};
END;
