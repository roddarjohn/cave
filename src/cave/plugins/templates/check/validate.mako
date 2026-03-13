BEGIN
% for check_expr, check_name in checks:
IF NOT (${check_expr}) THEN
    RAISE EXCEPTION 'check constraint "${check_name}" violated';
END IF;
% endfor
RETURN NEW;
END;
