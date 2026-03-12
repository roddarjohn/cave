"""Unit tests for DimensionFactory orchestration and FactoryContext.

These tests exercise the machinery of the plugin runner in isolation:
plugin resolution order, singleton enforcement, and FactoryContext
collision detection / ordering hints.  They use minimal stub plugins
to avoid depending on any storage-layer plugin implementation.
"""

import pytest
from sqlalchemy import Column, Integer, MetaData, String

from cave.config import CaveConfig
from cave.errors import CaveValidationError
from cave.factory.base import DimensionFactory
from cave.factory.context import FactoryContext
from cave.plugin import Plugin, singleton

# ---------------------------------------------------------------------------
# Stub plugins
# ---------------------------------------------------------------------------


class _WriterPlugin(Plugin):
    """Writes a fixed value to a ctx key during create_tables."""

    def __init__(self, key: str, value: object = "sentinel") -> None:
        self.key = key
        self.value = value

    def create_tables(self, ctx: FactoryContext) -> None:
        ctx[self.key] = self.value


class _ReaderPlugin(Plugin):
    """Reads a ctx key during create_tables; records the value read."""

    def __init__(self, key: str) -> None:
        self.key = key
        self.read_value: object = None

    def create_tables(self, ctx: FactoryContext) -> None:
        self.read_value = ctx[self.key]


class _LifecyclePlugin(Plugin):
    """Records the order in which lifecycle hooks are called."""

    def __init__(self, log: list[str], name: str = "p") -> None:
        self.log = log
        self.name = name

    def pk_columns(self, _ctx: FactoryContext) -> list[Column] | None:
        self.log.append(f"{self.name}.pk_columns")
        return None

    def extra_columns(self, _ctx: FactoryContext) -> list[Column]:
        self.log.append(f"{self.name}.extra_columns")
        return []

    def create_tables(self, _ctx: FactoryContext) -> None:
        self.log.append(f"{self.name}.create_tables")

    def create_views(self, _ctx: FactoryContext) -> None:
        self.log.append(f"{self.name}.create_views")

    def create_triggers(self, _ctx: FactoryContext) -> None:
        self.log.append(f"{self.name}.create_triggers")

    def post_create(self, _ctx: FactoryContext) -> None:
        self.log.append(f"{self.name}.post_create")


class _PKPlugin(Plugin):
    """Stub PK plugin that returns a fixed column list."""

    def pk_columns(self, _ctx: FactoryContext) -> list[Column]:
        return [Column("id", Integer, primary_key=True)]


class _ExtraColPlugin(Plugin):
    """Stub that appends a fixed extra column."""

    def __init__(self, col: Column) -> None:
        self.col = col

    def extra_columns(self, _ctx: FactoryContext) -> list[Column]:
        return [self.col]


@singleton("__pk__")
class _SingletonA(Plugin):
    pass


@singleton("__pk__")
class _SingletonB(Plugin):
    pass


# ---------------------------------------------------------------------------
# FactoryContext
# ---------------------------------------------------------------------------


class TestFactoryContext:
    def _ctx(self) -> FactoryContext:
        return FactoryContext(
            tablename="t",
            schemaname="s",
            metadata=MetaData(),
            dimensions=[],
            plugins=[],
        )

    def test_setitem_getitem_roundtrip(self):
        ctx = self._ctx()
        ctx["key"] = "value"
        assert ctx["key"] == "value"

    def test_setitem_collision_raises(self):
        ctx = self._ctx()
        ctx["key"] = "first"
        with pytest.raises(KeyError, match="already set"):
            ctx["key"] = "second"

    def test_getitem_missing_includes_ordering_hint(self):
        ctx = self._ctx()
        with pytest.raises(KeyError, match="plugin"):
            ctx["missing"]

    def test_getitem_missing_lists_set_keys(self):
        ctx = self._ctx()
        ctx["other"] = 1
        with pytest.raises(KeyError, match="other"):
            ctx["missing"]

    def test_contains_false_when_absent(self):
        ctx = self._ctx()
        assert "key" not in ctx

    def test_contains_true_after_set(self):
        ctx = self._ctx()
        ctx["key"] = "value"
        assert "key" in ctx

    def test_set_force_true_overwrites(self):
        ctx = self._ctx()
        ctx["key"] = "old"
        ctx.set("key", "new", force=True)
        assert ctx["key"] == "new"

    def test_set_force_false_raises_when_exists(self):
        ctx = self._ctx()
        ctx["key"] = "old"
        with pytest.raises(KeyError):
            ctx.set("key", "new", force=False)

    def test_set_new_key_without_force(self):
        ctx = self._ctx()
        ctx.set("key", "value")
        assert ctx["key"] == "value"


# ---------------------------------------------------------------------------
# Plugin resolution
# ---------------------------------------------------------------------------


class TestPluginResolution:
    def _factory(self, **kwargs):
        return DimensionFactory(
            "t", "s", MetaData(), [Column("name", String)], **kwargs
        )

    def test_default_plugins_empty_by_default(self):
        # With no plugins declared, factory runs fine with empty list.
        self._factory(plugins=[])

    def test_plugins_kwarg_replaces_defaults(self):
        log: list[str] = []
        plugin = _LifecyclePlugin(log)
        self._factory(plugins=[plugin])
        assert "p.create_tables" in log

    def test_extra_plugins_appended_after_plugins(self):
        log: list[str] = []
        base = _LifecyclePlugin(log, "base")
        extra = _LifecyclePlugin(log, "extra")
        self._factory(plugins=[base], extra_plugins=[extra])
        base_idx = log.index("base.create_tables")
        extra_idx = log.index("extra.create_tables")
        assert extra_idx > base_idx

    def test_cave_global_plugins_run_before_factory_plugins(self):
        log: list[str] = []
        global_plugin = _LifecyclePlugin(log, "global")
        factory_plugin = _LifecyclePlugin(log, "factory")
        cave = CaveConfig(plugins=[global_plugin])
        self._factory(cave=cave, plugins=[factory_plugin])
        global_idx = log.index("global.create_tables")
        factory_idx = log.index("factory.create_tables")
        assert global_idx < factory_idx


# ---------------------------------------------------------------------------
# Lifecycle ordering
# ---------------------------------------------------------------------------


class TestLifecycleOrdering:
    def test_phases_execute_in_order(self):
        log: list[str] = []
        plugin = _LifecyclePlugin(log)
        DimensionFactory(
            "t", "s", MetaData(), [Column("name", String)], plugins=[plugin]
        )
        expected_phases = [
            "p.pk_columns",
            "p.extra_columns",
            "p.create_tables",
            "p.create_views",
            "p.create_triggers",
            "p.post_create",
        ]
        # All phases appear in log in the expected order.
        indices = [log.index(phase) for phase in expected_phases]
        assert indices == sorted(indices)

    def test_writer_runs_before_reader_when_ordered_correctly(self):
        writer = _WriterPlugin("table")
        reader = _ReaderPlugin("table")
        DimensionFactory("t", "s", MetaData(), [], plugins=[writer, reader])
        assert reader.read_value == "sentinel"

    def test_reader_before_writer_raises(self):
        writer = _WriterPlugin("table")
        reader = _ReaderPlugin("table")
        with pytest.raises(KeyError, match="plugin"):
            DimensionFactory("t", "s", MetaData(), [], plugins=[reader, writer])


# ---------------------------------------------------------------------------
# Singleton enforcement
# ---------------------------------------------------------------------------


class TestSingletonEnforcement:
    def test_two_plugins_same_group_raises(self):
        with pytest.raises(CaveValidationError, match="__pk__"):
            DimensionFactory(
                "t",
                "s",
                MetaData(),
                [],
                plugins=[_SingletonA(), _SingletonB()],
            )

    def test_same_group_different_name_in_error(self):
        with pytest.raises(
            CaveValidationError, match=r"_SingletonA|_SingletonB"
        ):
            DimensionFactory(
                "t",
                "s",
                MetaData(),
                [],
                plugins=[_SingletonA(), _SingletonB()],
            )

    def test_single_plugin_in_group_is_fine(self):
        DimensionFactory("t", "s", MetaData(), [], plugins=[_SingletonA()])

    def test_different_groups_are_fine(self):
        @singleton("__group_x__")
        class _X(Plugin):
            pass

        @singleton("__group_y__")
        class _Y(Plugin):
            pass

        DimensionFactory("t", "s", MetaData(), [], plugins=[_X(), _Y()])


# ---------------------------------------------------------------------------
# PK and extra column resolution
# ---------------------------------------------------------------------------


class TestColumnResolution:
    def test_first_pk_plugin_wins(self):
        col_a = Column("pk_a", Integer, primary_key=True)
        col_b = Column("pk_b", Integer, primary_key=True)

        class _PKA(Plugin):
            def pk_columns(self, _ctx):
                return [col_a]

        class _PKB(Plugin):
            def pk_columns(self, _ctx):
                return [col_b]

        log: list[list[Column]] = []

        class _Capture(Plugin):
            def create_tables(self, ctx):
                log.append(list(ctx.pk_columns))

        DimensionFactory(
            "t", "s", MetaData(), [], plugins=[_PKA(), _PKB(), _Capture()]
        )
        assert log[0][0].key == "pk_a"

    def test_none_pk_defers_to_next(self):
        col = Column("id", Integer, primary_key=True)

        class _Defer(Plugin):
            def pk_columns(self, _ctx):
                return None

        class _PK(Plugin):
            def pk_columns(self, _ctx):
                return [col]

        log: list[list[Column]] = []

        class _Capture(Plugin):
            def create_tables(self, ctx):
                log.append(list(ctx.pk_columns))

        DimensionFactory(
            "t", "s", MetaData(), [], plugins=[_Defer(), _PK(), _Capture()]
        )
        assert len(log[0]) == 1

    def test_extra_columns_concatenated(self):
        col_a = Column("extra_a", String)
        col_b = Column("extra_b", String)
        log: list[list[Column]] = []

        class _Capture(Plugin):
            def create_tables(self, ctx):
                log.append(list(ctx.extra_columns))

        DimensionFactory(
            "t",
            "s",
            MetaData(),
            [],
            plugins=[
                _ExtraColPlugin(col_a),
                _ExtraColPlugin(col_b),
                _Capture(),
            ],
        )
        keys = [c.key for c in log[0]]
        assert "extra_a" in keys
        assert "extra_b" in keys
