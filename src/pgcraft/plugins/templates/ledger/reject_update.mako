BEGIN
RAISE EXCEPTION 'cannot update immutable ledger entries in ${view}';
END;
