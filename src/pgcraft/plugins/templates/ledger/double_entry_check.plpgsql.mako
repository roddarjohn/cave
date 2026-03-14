DECLARE
    _bad RECORD;
BEGIN
    SELECT ${entry_id_col},
           SUM(CASE WHEN ${direction_col} = '${debit}' THEN value ELSE 0 END) AS debits,
           SUM(CASE WHEN ${direction_col} = '${credit}' THEN value ELSE 0 END) AS credits
    INTO _bad
    FROM ${table}
    WHERE ${entry_id_col} IN (SELECT ${entry_id_col} FROM new_entries)
    GROUP BY ${entry_id_col}
    HAVING SUM(CASE WHEN ${direction_col} = '${debit}' THEN value ELSE 0 END)
        <> SUM(CASE WHEN ${direction_col} = '${credit}' THEN value ELSE 0 END)
    LIMIT 1;

    IF FOUND THEN
        RAISE EXCEPTION 'double-entry violation for entry_id %: debits=% credits=%',
            _bad.${entry_id_col}, _bad.debits, _bad.credits;
    END IF;

    RETURN NULL;
END;
