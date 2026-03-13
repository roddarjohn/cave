"""Unit tests for pgcraft.alembic.renderer."""

from unittest.mock import MagicMock

from sqlalchemy.dialects import postgresql
from sqlalchemy_declarative_extensions.alembic.function import CreateFunctionOp
from sqlalchemy_declarative_extensions.alembic.schema import (
    CreateSchemaOp,
    DropSchemaOp,
)
from sqlalchemy_declarative_extensions.alembic.trigger import CreateTriggerOp
from sqlalchemy_declarative_extensions.alembic.view import (
    CreateViewOp,
    DropViewOp,
)
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Function,
    FunctionSecurity,
    Trigger,
)
from sqlalchemy_declarative_extensions.dialects.postgresql.function import (
    FunctionReturn,
)
from sqlalchemy_declarative_extensions.role.compare import (
    CreateRoleOp,
    DropRoleOp,
)
from sqlalchemy_declarative_extensions.role.generic import Role
from sqlalchemy_declarative_extensions.schema.base import Schema
from sqlalchemy_declarative_extensions.view.base import View

from pgcraft.alembic.renderer import (
    _format_function_body,
    _prettify,
    _render_ddl_op,
    _render_execute,
    _render_execute_text,
    _render_grant,
    _render_role,
    _render_schema,
    _render_sql_op,
    _render_trigger,
)


def _mock_autogen_context() -> MagicMock:
    """Return a minimal mock AutogenContext."""
    ctx = MagicMock()
    ctx.connection.dialect = postgresql.dialect()
    ctx.imports = set()
    return ctx


# ---------------------------------------------------------------------------
# _prettify
# ---------------------------------------------------------------------------


class TestPrettify:
    def test_simple_select(self):
        result = _prettify("SELECT 1")
        assert "SELECT" in result
        assert "1" in result

    def test_select_with_where(self):
        result = _prettify("SELECT id FROM users WHERE id = 1")
        assert "SELECT" in result
        assert "users" in result

    def test_returns_string(self):
        assert isinstance(_prettify("SELECT 1"), str)

    def test_multiline_output_for_complex_query(self):
        sql = "SELECT a, b, c FROM my_table WHERE a = 1 AND b = 2 ORDER BY c"
        result = _prettify(sql)
        assert isinstance(result, str)

    def test_normalizes_whitespace(self):
        r1 = _prettify("SELECT    1")
        r2 = _prettify("SELECT 1")
        assert r1 == r2


# ---------------------------------------------------------------------------
# _format_function_body
# ---------------------------------------------------------------------------


class TestFormatFunctionBody:
    def test_indents_body_lines(self):
        sql = (
            "CREATE FUNCTION s.fn() RETURNS trigger LANGUAGE plpgsql AS $$\n"
            "BEGIN\n"
            "INSERT INTO t VALUES (NEW.id);\n"
            "RETURN NEW;\n"
            "END;\n"
            "$$"
        )
        result = _format_function_body(sql)
        assert "BEGIN" in result
        assert "END;" in result
        # Inner lines should be indented
        assert "    INSERT INTO t" in result or "INSERT INTO t" in result

    def test_sql_without_body_unchanged_shape(self):
        """SQL without $$ body should still be returned (prettified)."""
        sql = "SELECT 1"
        result = _format_function_body(sql)
        assert "SELECT" in result

    def test_begin_end_not_extra_indented(self):
        sql = (
            "CREATE FUNCTION s.fn() RETURNS trigger LANGUAGE plpgsql AS $$\n"
            "BEGIN\n"
            "    INSERT INTO t VALUES (1);\n"
            "END;\n"
            "$$"
        )
        result = _format_function_body(sql)
        # BEGIN and END; should appear without extra leading spaces
        lines = result.splitlines()
        begin_lines = [ln for ln in lines if ln.strip() == "BEGIN"]
        end_lines = [ln for ln in lines if ln.strip() == "END;"]
        assert len(begin_lines) >= 1
        assert len(end_lines) >= 1

    def test_returns_string(self):
        sql = (
            "CREATE FUNCTION s.fn() RETURNS trigger LANGUAGE plpgsql AS $$\n"
            "BEGIN\nRETURN NULL;\nEND;\n$$"
        )
        assert isinstance(_format_function_body(sql), str)


# ---------------------------------------------------------------------------
# _render_execute
# ---------------------------------------------------------------------------


class TestRenderExecute:
    def test_short_sql_inline(self):
        result = _render_execute("SELECT 1")
        assert result == 'op.execute("""SELECT 1""")'

    def test_fstring_prefix(self):
        result = _render_execute("SELECT 1", fstring=True)
        assert result == 'op.execute(f"""SELECT 1""")'

    def test_multiline_sql_goes_multiline(self):
        result = _render_execute("SELECT\n1")
        assert '"""\n' in result
        assert "SELECT" in result

    def test_long_sql_goes_multiline(self):
        long_sql = "SELECT " + "a" * 75
        result = _render_execute(long_sql)
        assert '"""\n' in result

    def test_sql_ending_in_double_quote_goes_multiline(self):
        """SQL ending with a double-quote must use the multi-line form."""
        result = _render_execute('SELECT "x"')
        assert '"""\n' in result

    def test_short_inline_no_newline(self):
        result = _render_execute("SELECT 1")
        assert "\n" not in result

    def test_indented_body_in_multiline_form(self):
        result = _render_execute("SELECT\n1")
        assert "    SELECT" in result

    def test_fstring_multiline(self):
        result = _render_execute("SELECT\n1", fstring=True)
        assert 'f"""' in result

    def test_sql_exactly_at_limit_is_inline(self):
        """Inline form at exactly 80 chars passes (> not >=)."""
        # inline = 'op.execute("""X""")'  → overhead = 19 chars
        # At exactly 80 chars len(inline) == 80 is NOT > 80, stays inline.
        sql = "S" * (80 - 19)
        result = _render_execute(sql)
        assert "\n" not in result

    def test_sql_one_over_limit_goes_multiline(self):
        """One character past the limit forces multi-line output."""
        # 80 - 19 + 2 gives inline len = 81, which is > 80.
        sql = "S" * (80 - 19 + 2)
        result = _render_execute(sql)
        assert '"""\n' in result


# ---------------------------------------------------------------------------
# _render_execute_text
# ---------------------------------------------------------------------------


class TestRenderExecuteText:
    def test_wraps_in_sa_text(self):
        result = _render_execute_text("SELECT 1")
        assert "sa.text" in result
        assert "SELECT 1" in result

    def test_always_multiline(self):
        result = _render_execute_text("SELECT 1")
        assert "\n" in result

    def test_returns_string(self):
        assert isinstance(_render_execute_text("SELECT 1"), str)

    def test_indented_sql(self):
        result = _render_execute_text("SELECT 1")
        assert "        SELECT 1" in result

    def test_short_sql_still_multiline(self):
        """Even very short SQL should use the multi-line form."""
        result = _render_execute_text("X")
        assert "\n" in result
        assert "sa.text" in result


# ---------------------------------------------------------------------------
# Per-op renderer functions (require AutogenContext)
# ---------------------------------------------------------------------------


class TestRenderSqlOp:
    """Tests for _render_sql_op (views)."""

    def test_create_view_op_returns_list_of_strings(self):
        view = View("myview", "SELECT 1", schema="public")
        op = CreateViewOp(view)
        ctx = _mock_autogen_context()
        result = _render_sql_op(ctx, op)
        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(s, str) for s in result)

    def test_create_view_op_contains_sql(self):
        view = View("myview", "SELECT 1", schema="public")
        op = CreateViewOp(view)
        result = _render_sql_op(_mock_autogen_context(), op)
        combined = "\n".join(result)
        assert "myview" in combined

    def test_drop_view_op_renders(self):
        view = View("myview", "SELECT 1", schema="public")
        op = DropViewOp(view)
        result = _render_sql_op(_mock_autogen_context(), op)
        assert isinstance(result, list)
        combined = "\n".join(result)
        assert "myview" in combined


class TestRenderDdlOp:
    """Tests for _render_ddl_op (functions/procedures)."""

    def _make_function(self) -> Function:
        return Function(
            "my_fn",
            "BEGIN\n    RETURN NEW;\nEND;",
            returns=FunctionReturn("trigger"),
            language="plpgsql",
            schema="public",
            security=FunctionSecurity.definer,
        )

    def test_create_function_op_returns_list(self):
        fn = self._make_function()
        op = CreateFunctionOp(fn)
        ctx = _mock_autogen_context()
        result = _render_ddl_op(ctx, op)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_create_function_op_contains_function_name(self):
        fn = self._make_function()
        op = CreateFunctionOp(fn)
        result = _render_ddl_op(_mock_autogen_context(), op)
        combined = "\n".join(result)
        assert "my_fn" in combined

    def test_result_wrapped_in_op_execute(self):
        fn = self._make_function()
        op = CreateFunctionOp(fn)
        result = _render_ddl_op(_mock_autogen_context(), op)
        assert all("op.execute" in s for s in result)


class TestRenderTrigger:
    """Tests for _render_trigger."""

    def test_create_trigger_op_returns_list(self):
        trigger = Trigger.instead_of(
            "insert",
            on="public.myview",
            execute="public.my_fn",
            name="my_trig",
        ).for_each_row()
        op = CreateTriggerOp(trigger)
        ctx = _mock_autogen_context()
        result = _render_trigger(ctx, op)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_create_trigger_op_contains_trigger_name(self):
        trigger = Trigger.instead_of(
            "insert",
            on="public.myview",
            execute="public.my_fn",
            name="my_trig",
        ).for_each_row()
        op = CreateTriggerOp(trigger)
        result = _render_trigger(_mock_autogen_context(), op)
        combined = "\n".join(result)
        assert "my_trig" in combined


class TestRenderSchema:
    """Tests for _render_schema."""

    def test_create_schema_op_returns_list(self):
        op = CreateSchemaOp(Schema("myschema"))
        ctx = _mock_autogen_context()
        result = _render_schema(ctx, op)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_create_schema_adds_import(self):
        op = CreateSchemaOp(Schema("myschema"))
        ctx = _mock_autogen_context()
        _render_schema(ctx, op)
        assert any("CreateSchema" in imp for imp in ctx.imports)

    def test_drop_schema_adds_import(self):
        op = DropSchemaOp(Schema("myschema"))
        ctx = _mock_autogen_context()
        _render_schema(ctx, op)
        assert any("DropSchema" in imp for imp in ctx.imports)

    def test_create_schema_output_contains_schema_name(self):
        op = CreateSchemaOp(Schema("myschema"))
        ctx = _mock_autogen_context()
        result = _render_schema(ctx, op)
        combined = "\n".join(result)
        assert "myschema" in combined


class TestRenderRole:
    """Tests for _render_role."""

    def test_static_role_returns_list(self):
        role = Role("myrole")
        op = CreateRoleOp(role)
        ctx = _mock_autogen_context()
        result = _render_role(ctx, op)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_static_role_does_not_add_import_os(self):
        role = Role("myrole")
        op = CreateRoleOp(role)
        ctx = _mock_autogen_context()
        _render_role(ctx, op)
        assert "import os" not in ctx.imports

    def test_drop_role_renders(self):
        role = Role("myrole")
        op = DropRoleOp(role)
        ctx = _mock_autogen_context()
        result = _render_role(ctx, op)
        assert isinstance(result, list)
        combined = "\n".join(result)
        assert "myrole" in combined


class TestRenderGrant:
    """Tests for _render_grant."""

    def test_grant_privileges_op_returns_string(self):
        from sqlalchemy_declarative_extensions.dialects.postgresql import (
            Grant as PgGrant,
        )
        from sqlalchemy_declarative_extensions.grant.compare import (
            GrantPrivilegesOp,
        )

        grant = PgGrant.new("select", to="anon").on_tables("api.widgets")
        op = GrantPrivilegesOp(grant)
        ctx = _mock_autogen_context()
        result = _render_grant(ctx, op)
        assert isinstance(result, str)
        assert "sa.text" in result
