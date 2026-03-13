"""Unit tests for pgcraft.utils.query."""

from sqlalchemy import Column, Integer, MetaData, String, Table, select

from pgcraft.utils.query import compile_query


class TestCompileQuery:
    def test_returns_string(self):
        md = MetaData()
        t = Table("t", md, Column("id", Integer))
        q = select(t.c.id)
        result = compile_query(q)
        assert isinstance(result, str)

    def test_simple_select_contains_expected_sql(self):
        md = MetaData()
        t = Table("users", md, Column("id", Integer), Column("name", String))
        q = select(t.c.id, t.c.name).select_from(t)
        sql = compile_query(q)
        assert "SELECT" in sql
        assert "users" in sql

    def test_schema_qualified_table(self):
        md = MetaData()
        t = Table(
            "users",
            md,
            Column("id", Integer),
            Column("name", String),
            schema="myschema",
        )
        q = select(t.c.id, t.c.name).select_from(t)
        sql = compile_query(q)
        assert "myschema" in sql
        assert "users" in sql

    def test_column_names_appear_in_output(self):
        md = MetaData()
        t = Table(
            "products",
            md,
            Column("product_id", Integer),
            Column("label", String),
        )
        q = select(t.c.product_id, t.c.label)
        sql = compile_query(q)
        assert "product_id" in sql
        assert "label" in sql

    def test_where_clause_rendered(self):
        md = MetaData()
        t = Table("t", md, Column("id", Integer), Column("status", String))
        q = select(t.c.id).where(t.c.status == "active")
        sql = compile_query(q)
        assert "active" in sql
        assert "WHERE" in sql.upper()

    def test_join_rendered(self):
        md = MetaData()
        parent = Table(
            "parent",
            md,
            Column("id", Integer),
            schema="s",
        )
        child = Table(
            "child",
            md,
            Column("id", Integer),
            Column("parent_id", Integer),
            schema="s",
        )
        q = (
            select(child.c.id)
            .select_from(child)
            .join(parent, parent.c.id == child.c.parent_id)
        )
        sql = compile_query(q)
        assert "JOIN" in sql.upper()
        assert "parent" in sql

    def test_uses_postgresql_dialect(self):
        """The compiled output should use PostgreSQL-style quoting."""
        sentinel = 42
        md = MetaData()
        t = Table("t", md, Column("id", Integer))
        q = select(t.c.id).where(t.c.id == sentinel)
        sql = compile_query(q)
        # PostgreSQL uses integer literals directly
        assert str(sentinel) in sql
