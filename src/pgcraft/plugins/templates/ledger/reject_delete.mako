BEGIN
RAISE EXCEPTION 'cannot delete immutable ledger entries from ${view}';
END;
