"""Unit tests for pgcraft.utils.trigger."""

from sqlalchemy import MetaData

from pgcraft.utils.trigger import register_view_triggers

_NAMING_DEFAULTS = {
    "fn_key": "%(schema)s_%(table_name)s_%(op)s",
    "tr_key": "%(schema)s_%(table_name)s_%(op)s",
}


class TestRegisterViewTriggers:
    def test_function_registered_in_metadata(self):
        """After the call, a function should appear in metadata.info."""
        metadata = MetaData()
        register_view_triggers(
            metadata=metadata,
            view_schema="api",
            view_fullname="api.mytable",
            tablename="mytable",
            ops=[("insert", "BEGIN RETURN NEW; END;")],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="fn_key",
            trigger_key="tr_key",
        )
        functions = metadata.info.get("functions")
        assert functions is not None
        assert len(functions.functions) == 1

    def test_trigger_registered_in_metadata(self):
        metadata = MetaData()
        register_view_triggers(
            metadata=metadata,
            view_schema="api",
            view_fullname="api.mytable",
            tablename="mytable",
            ops=[("insert", "BEGIN RETURN NEW; END;")],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="fn_key",
            trigger_key="tr_key",
        )
        triggers = metadata.info.get("triggers")
        assert triggers is not None
        assert len(triggers.triggers) == 1

    def test_function_name_follows_naming_template(self):
        metadata = MetaData()
        register_view_triggers(
            metadata=metadata,
            view_schema="api",
            view_fullname="api.products",
            tablename="products",
            ops=[("insert", "BEGIN RETURN NEW; END;")],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="fn_key",
            trigger_key="tr_key",
        )
        functions = metadata.info["functions"]
        fn = functions.functions[0]
        assert fn.name == "api_products_insert"

    def test_trigger_name_follows_naming_template(self):
        metadata = MetaData()
        register_view_triggers(
            metadata=metadata,
            view_schema="api",
            view_fullname="api.products",
            tablename="products",
            ops=[("insert", "BEGIN RETURN NEW; END;")],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="fn_key",
            trigger_key="tr_key",
        )
        triggers = metadata.info["triggers"]
        tr = triggers.triggers[0]
        assert tr.name == "api_products_insert"

    def test_multiple_ops_registered(self):
        """Each op tuple should produce one function and one trigger."""
        metadata = MetaData()
        ops = [
            ("insert", "BEGIN RETURN NEW; END;"),
            ("update", "BEGIN RETURN NEW; END;"),
            ("delete", "BEGIN RETURN OLD; END;"),
        ]
        register_view_triggers(
            metadata=metadata,
            view_schema="api",
            view_fullname="api.orders",
            tablename="orders",
            ops=ops,
            naming_defaults=_NAMING_DEFAULTS,
            function_key="fn_key",
            trigger_key="tr_key",
        )
        assert len(metadata.info["functions"].functions) == len(ops)
        assert len(metadata.info["triggers"].triggers) == len(ops)

    def test_function_schema_is_view_schema(self):
        metadata = MetaData()
        register_view_triggers(
            metadata=metadata,
            view_schema="myapi",
            view_fullname="myapi.tbl",
            tablename="tbl",
            ops=[("insert", "BEGIN RETURN NEW; END;")],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="fn_key",
            trigger_key="tr_key",
        )
        fn = metadata.info["functions"].functions[0]
        assert fn.schema == "myapi"

    def test_trigger_target_is_view_fullname(self):
        metadata = MetaData()
        register_view_triggers(
            metadata=metadata,
            view_schema="api",
            view_fullname="api.tbl",
            tablename="tbl",
            ops=[("insert", "BEGIN RETURN NEW; END;")],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="fn_key",
            trigger_key="tr_key",
        )
        tr = metadata.info["triggers"].triggers[0]
        assert tr.on == "api.tbl"

    def test_function_language_is_plpgsql(self):
        metadata = MetaData()
        register_view_triggers(
            metadata=metadata,
            view_schema="api",
            view_fullname="api.tbl",
            tablename="tbl",
            ops=[("insert", "BEGIN RETURN NEW; END;")],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="fn_key",
            trigger_key="tr_key",
        )
        fn = metadata.info["functions"].functions[0]
        assert fn.language == "plpgsql"

    def test_body_rendered_in_function(self):
        """Pre-rendered body should appear in the function definition."""
        metadata = MetaData()
        register_view_triggers(
            metadata=metadata,
            view_schema="api",
            view_fullname="api.tbl",
            tablename="tbl",
            ops=[
                (
                    "insert",
                    "BEGIN INSERT INTO s.actual_table; RETURN NEW; END;",
                )
            ],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="fn_key",
            trigger_key="tr_key",
        )
        fn = metadata.info["functions"].functions[0]
        assert "s.actual_table" in fn.definition

    def test_metadata_naming_convention_overrides_defaults(self):
        """Custom naming convention on metadata takes precedence."""
        metadata = MetaData(
            naming_convention={
                "fn_key": "custom_%(table_name)s_%(op)s",
                "tr_key": "custom_%(table_name)s_%(op)s",
            }
        )
        register_view_triggers(
            metadata=metadata,
            view_schema="api",
            view_fullname="api.orders",
            tablename="orders",
            ops=[("insert", "BEGIN RETURN NEW; END;")],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="fn_key",
            trigger_key="tr_key",
        )
        fn = metadata.info["functions"].functions[0]
        assert fn.name == "custom_orders_insert"
