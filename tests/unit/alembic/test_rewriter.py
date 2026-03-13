"""Unit tests for pgcraft.alembic.rewriter."""

from unittest.mock import MagicMock

from alembic.autogenerate.rewriter import Rewriter
from alembic.operations import ops as alembic_ops
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy_declarative_extensions.alembic.schema import CreateSchemaOp
from sqlalchemy_declarative_extensions.schema.base import Schema

from pgcraft.alembic.rewriter import (
    _sort_ops,
    pgcraft_process_revision_directives,
)


class TestCaveProcessRevisionDirectives:
    def test_is_rewriter_instance(self):
        assert isinstance(pgcraft_process_revision_directives, Rewriter)


class TestSortOps:
    def _make_context(self, metadata=None):
        ctx = MagicMock()
        ctx.opts = {"target_metadata": metadata} if metadata is not None else {}
        return ctx

    def test_empty_ops_returns_empty(self):
        ctx = self._make_context()
        result = _sort_ops(ctx, [])
        assert result == []

    def test_no_metadata_in_context(self):
        """Missing target_metadata key must not raise."""
        ctx = self._make_context()
        op = alembic_ops.CreateTableOp(
            "t", [Column("id", Integer, primary_key=True)]
        )
        result = _sort_ops(ctx, [op])
        assert len(result) == 1

    def test_with_metadata_in_context(self):
        """target_metadata present must use its FK graph."""
        metadata = MetaData()
        ctx = self._make_context(metadata)
        op = alembic_ops.CreateTableOp(
            "t", [Column("id", Integer, primary_key=True)]
        )
        result = _sort_ops(ctx, [op])
        assert len(result) == 1

    def test_schema_before_table_ordering(self):
        """Table ops must be sorted after schema ops."""
        ctx = self._make_context()
        table_op = alembic_ops.CreateTableOp(
            "mytable",
            [Column("id", Integer, primary_key=True)],
            schema="s",
        )
        schema_op = CreateSchemaOp(Schema("s"))
        result = _sort_ops(ctx, [table_op, schema_op])
        schema_idx = next(
            i for i, op in enumerate(result) if isinstance(op, CreateSchemaOp)
        )
        table_idx = next(
            i
            for i, op in enumerate(result)
            if isinstance(op, alembic_ops.CreateTableOp)
        )
        assert schema_idx < table_idx

    def test_expand_update_ops_called(self):
        """UpdateViewOp must be split into Drop + Create before sorting."""
        from sqlalchemy_declarative_extensions.alembic.view import (
            UpdateViewOp,
        )
        from sqlalchemy_declarative_extensions.view.base import View

        ctx = self._make_context()
        v_old = View("v", "SELECT 1", schema="s")
        v_new = View("v", "SELECT 2", schema="s")
        update_op = UpdateViewOp(v_old, v_new)
        result = _sort_ops(ctx, [update_op])
        types = [type(op).__name__ for op in result]
        assert "DropViewOp" in types
        assert "CreateViewOp" in types

    def test_fk_graph_built_from_metadata(self):
        """FK relationships in metadata must influence sorting order."""
        metadata = MetaData()
        Table(
            "parent",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="s",
        )
        from sqlalchemy import ForeignKey

        Table(
            "child",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("pid", ForeignKey("s.parent.id")),
            schema="s",
        )
        ctx = self._make_context(metadata)
        parent_op = alembic_ops.CreateTableOp(
            "parent", [Column("id", Integer, primary_key=True)], schema="s"
        )
        child_op = alembic_ops.CreateTableOp(
            "child", [Column("id", Integer, primary_key=True)], schema="s"
        )
        result = _sort_ops(ctx, [child_op, parent_op])
        names = [op.table_name for op in result]
        assert names.index("parent") < names.index("child")

    def test_returns_list(self):
        ctx = self._make_context()
        result = _sort_ops(ctx, [])
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# _reorder_upgrade / _reorder_downgrade via the Rewriter callable
# ---------------------------------------------------------------------------


class TestRewriterDispatch:
    """Exercise _reorder_upgrade and _reorder_downgrade via the Rewriter.

    The Rewriter is callable: pgcraft_process_revision_directives(
        context, revision, directives
    ) dispatches to the registered @rewrites handlers.
    """

    def _make_context(self, metadata=None):
        ctx = MagicMock()
        ctx.opts = {"target_metadata": metadata} if metadata is not None else {}
        return ctx

    def _make_script(
        self,
        upgrade_ops: list | None = None,
        downgrade_ops: list | None = None,
    ) -> alembic_ops.MigrationScript:
        return alembic_ops.MigrationScript(
            rev_id="test_rev",
            upgrade_ops=alembic_ops.UpgradeOps(ops=upgrade_ops or []),
            downgrade_ops=alembic_ops.DowngradeOps(ops=downgrade_ops or []),
        )

    def test_reorder_upgrade_called_with_empty_ops(self):
        """Empty UpgradeOps must pass through without error."""
        ctx = self._make_context()
        script = self._make_script()
        pgcraft_process_revision_directives(ctx, "test_rev", [script])
        assert script.upgrade_ops.ops == []

    def test_reorder_downgrade_called_with_empty_ops(self):
        """Empty DowngradeOps must pass through without error."""
        ctx = self._make_context()
        script = self._make_script()
        pgcraft_process_revision_directives(ctx, "test_rev", [script])
        assert script.downgrade_ops.ops == []

    def test_reorder_upgrade_sorts_ops(self):
        """Upgrade ops are sorted: schema before table."""
        from sqlalchemy_declarative_extensions.alembic.schema import (
            CreateSchemaOp,
        )
        from sqlalchemy_declarative_extensions.schema.base import Schema

        table_op = alembic_ops.CreateTableOp(
            "mytable",
            [Column("id", Integer, primary_key=True)],
            schema="s",
        )
        schema_op = CreateSchemaOp(Schema("s"))
        script = self._make_script(upgrade_ops=[table_op, schema_op])
        ctx = self._make_context()
        pgcraft_process_revision_directives(ctx, "rev", [script])
        types = [type(op).__name__ for op in script.upgrade_ops.ops]
        assert types.index("CreateSchemaOp") < types.index("CreateTableOp")

    def test_reorder_downgrade_sorts_ops(self):
        """Downgrade ops are also sorted by the rewriter."""
        from sqlalchemy_declarative_extensions.alembic.schema import (
            DropSchemaOp,
        )
        from sqlalchemy_declarative_extensions.schema.base import Schema

        table_op = alembic_ops.DropTableOp("mytable", schema="s")
        schema_op = DropSchemaOp(Schema("s"))
        script = self._make_script(downgrade_ops=[schema_op, table_op])
        ctx = self._make_context()
        pgcraft_process_revision_directives(ctx, "rev", [script])
        # After sorting drops: table must be dropped before schema
        types = [type(op).__name__ for op in script.downgrade_ops.ops]
        assert types.index("DropTableOp") < types.index("DropSchemaOp")
