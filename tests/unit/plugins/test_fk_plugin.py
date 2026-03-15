"""Unit tests for TableFKPlugin."""

import pytest
from sqlalchemy import Column, ForeignKeyConstraint, Integer, String, Table

from pgcraft.errors import PGCraftValidationError
from pgcraft.fk import PGCraftFK
from pgcraft.plugins.fk import TableFKPlugin
from tests.unit.plugins.conftest import make_ctx


class TestTableFKPlugin:
    def test_appends_fk_constraint_to_table(self):
        from pgcraft.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("org_id", Integer),
                PGCraftFK(
                    columns=["{org_id}"],
                    references=["public.orgs.id"],
                    name="fk_org",
                ),
            ]
        )
        SimpleTablePlugin().run(ctx)
        TableFKPlugin().run(ctx)
        table = ctx["primary"]
        fk_names = [
            c.name
            for c in table.constraints
            if isinstance(c, ForeignKeyConstraint)
        ]
        assert "fk_org" in fk_names

    def test_cascade_options(self):
        from pgcraft.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("org_id", Integer),
                PGCraftFK(
                    columns=["{org_id}"],
                    references=["public.orgs.id"],
                    name="fk_org",
                    ondelete="CASCADE",
                    onupdate="SET NULL",
                ),
            ]
        )
        SimpleTablePlugin().run(ctx)
        TableFKPlugin().run(ctx)
        table = ctx["primary"]
        fk_constraint = next(
            c for c in table.constraints if getattr(c, "name", None) == "fk_org"
        )
        assert fk_constraint.ondelete == "CASCADE"
        assert fk_constraint.onupdate == "SET NULL"

    def test_no_fks_is_noop(self):
        from pgcraft.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(schema_items=[Column("name", String)])
        SimpleTablePlugin().run(ctx)
        TableFKPlugin().run(ctx)
        assert isinstance(ctx["primary"], Table)

    def test_unknown_column_raises(self):
        from pgcraft.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("org_id", Integer),
                PGCraftFK(
                    columns=["{nonexistent}"],
                    references=["public.orgs.id"],
                    name="fk_bad",
                ),
            ]
        )
        SimpleTablePlugin().run(ctx)
        with pytest.raises(
            PGCraftValidationError,
            match="nonexistent",
        ):
            TableFKPlugin().run(ctx)

    def test_custom_table_key(self):
        from pgcraft.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("org_id", Integer),
                PGCraftFK(
                    columns=["{org_id}"],
                    references=["public.orgs.id"],
                    name="fk_org",
                ),
            ]
        )
        SimpleTablePlugin(table_key="my_table").run(ctx)
        TableFKPlugin(table_key="my_table").run(ctx)
        table = ctx["my_table"]
        fk_names = [
            c.name
            for c in table.constraints
            if isinstance(c, ForeignKeyConstraint)
        ]
        assert "fk_org" in fk_names

    def test_requires_dynamic_table_key(self):
        plugin = TableFKPlugin()
        assert "primary" in plugin.resolved_requires()
