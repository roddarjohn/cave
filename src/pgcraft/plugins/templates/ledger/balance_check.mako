DECLARE
    _bad RECORD;
BEGIN
    SELECT ${dim_cols}, SUM(value) AS balance
    INTO _bad
    FROM ${table}
    WHERE (${dim_cols}) IN (
        SELECT ${dim_cols} FROM new_entries
    )
    GROUP BY ${dim_cols}
    HAVING SUM(value) < ${min_balance}
    LIMIT 1;

    IF FOUND THEN
        RAISE EXCEPTION 'ledger balance violation on ${table}: balance % is below minimum ${min_balance} for (${dim_cols}) = (${dim_format})',
            _bad.balance, ${dim_values};
    END IF;

    RETURN NULL;
END;
