BEGIN
% for attr_name, value_col in mappings:
IF NEW.${attr_name} IS NOT NULL THEN
    INSERT INTO ${attr_table} (entity_id, attribute_name, ${value_col})
    VALUES (OLD.id, '${attr_name}', NEW.${attr_name});
END IF;
% endfor
RETURN NEW;
END;
