"""Unit tests for cave.patches.view_render."""

from unittest.mock import MagicMock, patch

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_declarative_extensions.view.base import View

from cave.patches.view_render import _patched_render_definition, apply


def _make_pg_conn():
    """Return a MagicMock that looks like a PostgreSQL connection."""
    mock_conn = MagicMock()
    mock_conn.engine.dialect.name = "postgresql"
    mock_trans = MagicMock()
    mock_nested = MagicMock()
    mock_nested.__enter__ = MagicMock(return_value=mock_trans)
    mock_nested.__exit__ = MagicMock(return_value=False)
    mock_conn.begin_nested.return_value = mock_nested
    return mock_conn


class TestApply:
    def test_patches_view_render_definition(self):
        """After apply(), View.render_definition must be the patched version."""
        apply()
        assert View.render_definition is _patched_render_definition

    def test_apply_idempotent(self):
        """Calling apply() twice must not raise."""
        apply()
        apply()
        assert View.render_definition is _patched_render_definition


class TestPatchedRenderDefinitionFallback:
    def test_non_postgresql_dialect_returns_prettified_sql(self):
        """Non-PostgreSQL dialects fall back to pglast without DB access."""
        view = View("test_view", "SELECT 1")
        mock_conn = MagicMock()
        mock_conn.engine.dialect.name = "sqlite"
        result = _patched_render_definition(view, mock_conn)
        assert result.endswith(";")
        assert "SELECT" in result

    def test_using_connection_false_skips_db(self):
        """using_connection=False must skip the DB path entirely."""
        view = View("test_view", "SELECT 1")
        mock_conn = MagicMock()
        mock_conn.engine.dialect.name = "postgresql"
        result = _patched_render_definition(
            view, mock_conn, using_connection=False
        )
        assert result.endswith(";")
        mock_conn.begin_nested.assert_not_called()

    def test_fallback_returns_string(self):
        view = View("test_view", "SELECT 1")
        mock_conn = MagicMock()
        mock_conn.engine.dialect.name = "sqlite"
        result = _patched_render_definition(view, mock_conn)
        assert isinstance(result, str)

    def test_non_postgresql_contains_select(self):
        view = View("v", "SELECT id, name FROM t")
        mock_conn = MagicMock()
        mock_conn.engine.dialect.name = "mysql"
        result = _patched_render_definition(view, mock_conn)
        assert "SELECT" in result.upper()


class TestPatchedRenderDefinitionSQLAlchemyErrorFallback:
    def test_sqla_error_falls_back_to_pglast(self):
        """SQLAlchemyError inside the nested transaction → pglast fallback."""
        view = View("test_view", "SELECT 1")
        mock_conn = _make_pg_conn()
        mock_conn.execute.side_effect = SQLAlchemyError("column not found")
        result = _patched_render_definition(view, mock_conn)
        assert result.endswith(";")
        assert "SELECT" in result

    def test_sqla_error_rollback_called(self):
        """trans.rollback() must be called via the finally block."""
        view = View("test_view", "SELECT 1")
        mock_conn = _make_pg_conn()
        mock_conn.execute.side_effect = SQLAlchemyError("oops")
        _patched_render_definition(view, mock_conn)
        # The finally block always calls trans.rollback()
        mock_conn.begin_nested.return_value.__enter__.return_value.rollback.assert_called()


class TestPatchedRenderDefinitionHappyPath:
    def test_same_definition_returns_escaped(self):
        """When the DB returns the same definition, it is returned directly."""
        view = View("test_view", "SELECT 1")
        mock_conn = _make_pg_conn()
        mock_view_obj = MagicMock()
        mock_view_obj.definition = "SELECT 1"

        with patch(
            "cave.patches.view_render.get_view",
            return_value=mock_view_obj,
        ):
            result = _patched_render_definition(view, mock_conn)

        assert "SELECT" in result

    def test_different_definition_triggers_second_normalize(self):
        """When definitions differ, a second normalization round is done."""
        view = View("test_view", "SELECT 1")
        mock_conn = _make_pg_conn()
        mock_view1 = MagicMock()
        mock_view1.definition = "SELECT 1 AS x"
        mock_view2 = MagicMock()
        mock_view2.definition = "SELECT 1 AS x"

        call_count = 0

        def _get_view(_conn, _name):
            nonlocal call_count
            call_count += 1
            return mock_view1 if call_count == 1 else mock_view2

        with patch("cave.patches.view_render.get_view", side_effect=_get_view):
            result = _patched_render_definition(view, mock_conn)

        assert "SELECT" in result
        assert call_count == 2

    def test_double_normalize_result_is_string(self):
        view = View("v", "SELECT 1")
        mock_conn = _make_pg_conn()
        mock_view1 = MagicMock()
        mock_view1.definition = "SELECT 1 AS normalized"
        mock_view2 = MagicMock()
        mock_view2.definition = "SELECT 1 AS normalized"

        views = [mock_view1, mock_view2]
        idx = 0

        def _get_view(_conn, _name):
            nonlocal idx
            v = views[idx % len(views)]
            idx += 1
            return v

        with patch("cave.patches.view_render.get_view", side_effect=_get_view):
            result = _patched_render_definition(view, mock_conn)

        assert isinstance(result, str)
