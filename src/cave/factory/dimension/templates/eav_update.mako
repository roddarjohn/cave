BEGIN
% for attr_name, value_col, nullable in mappings:
% if not nullable:
IF NEW.${attr_name} IS NULL THEN
    RAISE EXCEPTION 'attribute ${attr_name} cannot be null';
END IF;
% endif
IF NEW.${attr_name} IS NOT NULL
   AND (
       SELECT ${value_col}
       FROM ${attr_table}
       WHERE entity_id = OLD.id AND attribute_name = '${attr_name}'
       ORDER BY created_at DESC, id DESC
       LIMIT 1
   ) IS DISTINCT FROM NEW.${attr_name} THEN
    INSERT INTO ${attr_table} (entity_id, attribute_name, ${value_col})
    VALUES (OLD.id, '${attr_name}', NEW.${attr_name});
END IF;
% endfor
RETURN NEW;
END;
