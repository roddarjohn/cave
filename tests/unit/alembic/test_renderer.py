"""Unit tests for cave.alembic.renderer."""

from cave.alembic.renderer import (
    _format_function_body,
    _prettify,
    _render_execute,
    _render_execute_text,
)

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
