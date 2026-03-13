"""Unit tests for ResourceFactory orchestration and FactoryContext.

These tests exercise the machinery of the plugin runner in isolation:
plugin resolution order, singleton enforcement, dependency-based
sorting, and FactoryContext collision detection / ordering hints.
They use minimal stub plugins to avoid depending on any storage-layer
plugin implementation.
"""

import pytest
from sqlalchemy import Column, Integer, MetaData, String

from cave.check import CaveCheck
from cave.columns import PrimaryKeyColumns
from cave.config import CaveConfig
from cave.errors import CaveValidationError
from cave.factory.base import ResourceFactory, _sort_plugins
from cave.factory.context import FactoryContext
from cave.plugin import (
    Dynamic,
    Plugin,
    produces,
    requires,
    singleton,
)

# -------------------------------------------------------------------
# Stub plugins
# -------------------------------------------------------------------


class _WriterPlugin(Plugin):
    """Writes a fixed value to a ctx key during run."""

    def __init__(self, key: str, value: object = "sentinel") -> None:
        self.key = key
        self.value = value

    def run(self, ctx: FactoryContext) -> None:
        ctx[self.key] = self.value


class _ReaderPlugin(Plugin):
    """Reads a ctx key during run; records the value read."""

    def __init__(self, key: str) -> None:
        self.key = key
        self.read_value: object = None

    def run(self, ctx: FactoryContext) -> None:
        self.read_value = ctx[self.key]


class _RunLogPlugin(Plugin):
    """Records when run is called."""

    def __init__(self, log: list[str], name: str = "p") -> None:
        self.log = log
        self.name = name

    def run(self, _ctx: FactoryContext) -> None:
        self.log.append(f"{self.name}.run")


@singleton("__pk__")
class _SingletonA(Plugin):
    pass


@singleton("__pk__")
class _SingletonB(Plugin):
    pass


# -------------------------------------------------------------------
# FactoryContext
# -------------------------------------------------------------------


class TestFactoryContext:
    def _ctx(self) -> FactoryContext:
        return FactoryContext(
            tablename="t",
            schemaname="s",
            metadata=MetaData(),
            schema_items=[],
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


# -------------------------------------------------------------------
# Plugin resolution
# -------------------------------------------------------------------


class TestPluginResolution:
    def _factory(self, **kwargs):
        return ResourceFactory(
            "t",
            "s",
            MetaData(),
            [Column("name", String)],
            **kwargs,
        )

    def test_default_plugins_empty_by_default(self):
        self._factory(plugins=[])

    def test_plugins_kwarg_replaces_defaults(self):
        log: list[str] = []
        plugin = _RunLogPlugin(log)
        self._factory(plugins=[plugin])
        assert "p.run" in log

    def test_extra_plugins_appended_after_plugins(self):
        log: list[str] = []
        base = _RunLogPlugin(log, "base")
        extra = _RunLogPlugin(log, "extra")
        self._factory(plugins=[base], extra_plugins=[extra])
        base_idx = log.index("base.run")
        extra_idx = log.index("extra.run")
        assert extra_idx > base_idx

    def test_cave_global_plugins_run_before_factory(self):
        log: list[str] = []
        global_plugin = _RunLogPlugin(log, "global")
        factory_plugin = _RunLogPlugin(log, "factory")
        cave = CaveConfig(plugins=[global_plugin])
        self._factory(cave=cave, plugins=[factory_plugin])
        global_idx = log.index("global.run")
        factory_idx = log.index("factory.run")
        assert global_idx < factory_idx


# -------------------------------------------------------------------
# Run ordering
# -------------------------------------------------------------------


class TestRunOrdering:
    def test_writer_runs_before_reader(self):
        writer = _WriterPlugin("table")
        reader = _ReaderPlugin("table")
        ResourceFactory(
            "t",
            "s",
            MetaData(),
            [],
            plugins=[writer, reader],
        )
        assert reader.read_value == "sentinel"

    def test_reader_before_writer_raises_without_decl(self):
        writer = _WriterPlugin("table")
        reader = _ReaderPlugin("table")
        with pytest.raises(KeyError, match="plugin"):
            ResourceFactory(
                "t",
                "s",
                MetaData(),
                [],
                plugins=[reader, writer],
            )


# -------------------------------------------------------------------
# Dependency sorting
# -------------------------------------------------------------------


class TestDependencySorting:
    def test_sort_fixes_reversed_order(self):
        @produces("x")
        class _A(Plugin):
            def run(self, ctx: FactoryContext) -> None:
                ctx["x"] = 1

        @requires("x")
        class _B(Plugin):
            def run(self, ctx: FactoryContext) -> None:
                self.got = ctx["x"]

        a, b = _A(), _B()
        sorted_list = _sort_plugins([b, a])
        assert sorted_list.index(a) < sorted_list.index(b)

    def test_sort_detects_cycle(self):
        @produces("x")
        @requires("y")
        class _A(Plugin):
            pass

        @produces("y")
        @requires("x")
        class _B(Plugin):
            pass

        with pytest.raises(CaveValidationError, match="Circular"):
            _sort_plugins([_A(), _B()])

    def test_two_plugins_produce_same_key_allowed(self):
        log: list[str] = []

        @produces("key")
        class _First(Plugin):
            def run(self, ctx: FactoryContext) -> None:
                log.append("first")
                ctx["key"] = "original"

        @produces("key")
        @requires("key")
        class _Override(Plugin):
            def run(self, ctx: FactoryContext) -> None:
                log.append("override")
                ctx.set("key", "replaced", force=True)

        ResourceFactory(
            "t",
            "s",
            MetaData(),
            [],
            plugins=[_Override(), _First()],
        )
        assert log == ["first", "override"]

    def test_sort_external_require_ignored(self):
        @requires("already_in_db")
        class _NeedsExternal(Plugin):
            def run(self, ctx: FactoryContext) -> None:
                pass

        result = _sort_plugins([_NeedsExternal()])
        assert len(result) == 1

    def test_sort_preserves_order_when_no_deps(self):
        plugins = [_RunLogPlugin([], f"p{i}") for i in range(4)]
        assert _sort_plugins(plugins) == plugins

    def test_declared_deps_fix_reversed_list_order(self):
        log: list[str] = []

        @produces("tbl")
        class _W(Plugin):
            def run(self, ctx: FactoryContext) -> None:
                log.append("write")
                ctx["tbl"] = "value"

        @requires("tbl")
        class _R(Plugin):
            def run(self, ctx: FactoryContext) -> None:
                log.append("read")
                _ = ctx["tbl"]

        ResourceFactory(
            "t",
            "s",
            MetaData(),
            [],
            plugins=[_R(), _W()],
        )
        assert log == ["write", "read"]


# -------------------------------------------------------------------
# Dynamic validation
# -------------------------------------------------------------------


class TestDynamicValidation:
    def test_produces_dynamic_missing_init_kwarg_raises(self):
        with pytest.raises(TypeError, match="table_key"):

            @produces(Dynamic("table_key"))
            class _Bad(Plugin):
                def __init__(self) -> None:
                    pass

    def test_requires_dynamic_missing_init_kwarg_raises(self):
        with pytest.raises(TypeError, match="source_key"):

            @requires(Dynamic("source_key"))
            class _Bad(Plugin):
                def __init__(self) -> None:
                    pass

    def test_dynamic_valid_kwarg_does_not_raise(self):
        @produces(Dynamic("table_key"))
        class _Good(Plugin):
            def __init__(self, table_key: str = "primary") -> None:
                self.table_key = table_key

    def test_resolved_produces_substitutes_dynamic(self):
        @produces(Dynamic("table_key"))
        class _P(Plugin):
            def __init__(self, table_key: str = "primary") -> None:
                self.table_key = table_key

        assert _P(table_key="custom").resolved_produces() == ["custom"]
        assert _P().resolved_produces() == ["primary"]

    def test_resolved_requires_substitutes_dynamic(self):
        @requires(Dynamic("src"))
        class _P(Plugin):
            def __init__(self, src: str = "raw") -> None:
                self.src = src

        assert _P(src="data").resolved_requires() == ["data"]


# -------------------------------------------------------------------
# Singleton enforcement (decorator-driven validators)
# -------------------------------------------------------------------


class TestSingletonEnforcement:
    def test_two_plugins_same_group_raises(self):
        with pytest.raises(CaveValidationError, match="__pk__"):
            ResourceFactory(
                "t",
                "s",
                MetaData(),
                [],
                plugins=[_SingletonA(), _SingletonB()],
            )

    def test_same_group_different_name_in_error(self):
        with pytest.raises(
            CaveValidationError,
            match=r"_SingletonA|_SingletonB",
        ):
            ResourceFactory(
                "t",
                "s",
                MetaData(),
                [],
                plugins=[_SingletonA(), _SingletonB()],
            )

    def test_single_plugin_in_group_is_fine(self):
        ResourceFactory(
            "t",
            "s",
            MetaData(),
            [],
            plugins=[_SingletonA()],
        )

    def test_different_groups_are_fine(self):
        @singleton("__group_x__")
        class _X(Plugin):
            pass

        @singleton("__group_y__")
        class _Y(Plugin):
            pass

        ResourceFactory(
            "t",
            "s",
            MetaData(),
            [],
            plugins=[_X(), _Y()],
        )


# -------------------------------------------------------------------
# PK columns via ctx store
# -------------------------------------------------------------------


class TestPKColumnsViaStore:
    def test_pk_plugin_stores_pk_columns(self):
        """SerialPKPlugin writes pk_columns to the ctx store."""
        log: list[str] = []

        @requires("pk_columns")
        class _Capture(Plugin):
            def run(self, ctx: FactoryContext) -> None:
                pk = ctx["pk_columns"]
                log.append(pk.first_key)

        from cave.plugins.pk import SerialPKPlugin

        ResourceFactory(
            "t",
            "s",
            MetaData(),
            [],
            plugins=[SerialPKPlugin(), _Capture()],
        )
        assert log == ["id"]

    def test_custom_pk_name_via_store(self):
        log: list[str] = []

        @requires("pk_columns")
        class _Capture(Plugin):
            def run(self, ctx: FactoryContext) -> None:
                log.append(ctx["pk_columns"].first_key)

        from cave.plugins.pk import SerialPKPlugin

        ResourceFactory(
            "t",
            "s",
            MetaData(),
            [],
            plugins=[
                SerialPKPlugin(column_name="uid"),
                _Capture(),
            ],
        )
        assert log == ["uid"]

    def test_pk_columns_iterable_for_unpacking(self):
        """PrimaryKeyColumns can be unpacked with *."""
        pk = PrimaryKeyColumns([Column("id", Integer, primary_key=True)])
        cols = list(pk)
        assert len(cols) == 1
        assert cols[0].key == "id"


# -------------------------------------------------------------------
# CaveCheck in schema_items
# -------------------------------------------------------------------


class TestCaveCheckInSchemaItems:
    def test_factory_accepts_cave_check(self):
        """ResourceFactory does not reject CaveCheck in schema_items."""
        ResourceFactory(
            "t",
            "s",
            MetaData(),
            [
                Column("price", Integer),
                CaveCheck("{price} > 0", name="pos"),
            ],
            plugins=[],
        )

    def test_ctx_columns_filters_out_cave_check(self):
        """FactoryContext.columns returns only Column instances."""
        ctx = FactoryContext(
            tablename="t",
            schemaname="s",
            metadata=MetaData(),
            schema_items=[
                Column("price", Integer),
                CaveCheck("{price} > 0", name="pos"),
                Column("qty", Integer),
            ],
            plugins=[],
        )
        cols = ctx.columns
        assert len(cols) == 2
        assert all(isinstance(c, Column) for c in cols)
        names = {c.name for c in cols}
        assert names == {"price", "qty"}
