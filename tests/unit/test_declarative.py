"""Unit tests for cave.declarative.register decorator."""
# ruff: noqa: RUF012

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from cave.check import CaveCheck
from cave.columns import PrimaryKeyColumns
from cave.declarative import register
from cave.errors import CaveValidationError
from cave.factory.context import FactoryContext
from cave.plugin import Plugin, requires
from cave.plugins.api import APIPlugin
from cave.plugins.pk import SerialPKPlugin
from cave.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin


class Base(DeclarativeBase):
    pass


# -------------------------------------------------------------------
# Stub plugins
# -------------------------------------------------------------------


@requires("__root__")
class _CapturePlugin(Plugin):
    """Record what the ctx store contains when run is called."""

    captured: dict = {}

    def run(self, ctx: FactoryContext) -> None:
        _CapturePlugin.captured = {
            "pk_columns": ctx["pk_columns"],
            "__root__": ctx["__root__"],
            "primary": ctx["primary"],
            "tablename": ctx.tablename,
            "schemaname": ctx.schemaname,
            "schema_items": ctx.schema_items,
        }


# -------------------------------------------------------------------
# Basic decorator behavior
# -------------------------------------------------------------------


class TestRegisterDecorator:
    def setup_method(self):
        _CapturePlugin.captured = {}
        # Clear Base metadata between tests.
        Base.metadata.clear()

    def test_creates_table_on_metadata(self):
        md = MetaData()

        @register(
            metadata=md,
            plugins=[SerialPKPlugin(), SimpleTablePlugin()],
        )
        class User:
            __tablename__ = "users"
            __table_args__ = {"schema": "public"}

            name = Column(String)

        assert "public.users" in md.tables

    def test_sets_dunder_table(self):
        md = MetaData()

        @register(
            metadata=md,
            plugins=[SerialPKPlugin(), SimpleTablePlugin()],
        )
        class User:
            __tablename__ = "users"
            __table_args__ = {"schema": "public"}

            name = Column(String)

        assert isinstance(User.__table__, Table)
        assert User.__table__.name == "users"

    def test_table_has_pk_from_plugin(self):
        md = MetaData()

        @register(
            metadata=md,
            plugins=[SerialPKPlugin(), SimpleTablePlugin()],
        )
        class Item:
            __tablename__ = "items"
            __table_args__ = {"schema": "dim"}

            label = Column(String)

        pk_cols = [c for c in Item.__table__.columns if c.primary_key]
        assert len(pk_cols) == 1
        assert pk_cols[0].key == "id"

    def test_table_has_declared_columns(self):
        md = MetaData()

        @register(
            metadata=md,
            plugins=[SerialPKPlugin(), SimpleTablePlugin()],
        )
        class Product:
            __tablename__ = "products"
            __table_args__ = {"schema": "dim"}

            name = Column(String)
            code = Column(String)

        col_names = {c.name for c in Product.__table__.columns}
        assert "name" in col_names
        assert "code" in col_names

    def test_column_name_inferred_from_attribute(self):
        md = MetaData()

        @register(
            metadata=md,
            plugins=[SerialPKPlugin(), SimpleTablePlugin()],
        )
        class Widget:
            __tablename__ = "widgets"
            __table_args__ = {"schema": "dim"}

            color = Column(String)

        assert "color" in {c.name for c in Widget.__table__.columns}

    def test_explicit_column_name_preserved(self):
        md = MetaData()

        @register(
            metadata=md,
            plugins=[SerialPKPlugin(), SimpleTablePlugin()],
        )
        class Gadget:
            __tablename__ = "gadgets"
            __table_args__ = {"schema": "dim"}

            email = Column("email_address", String)

        assert "email_address" in {c.name for c in Gadget.__table__.columns}

    def test_runs_api_and_trigger_plugins(self):
        md = MetaData()

        @register(
            metadata=md,
            plugins=[
                SerialPKPlugin(),
                SimpleTablePlugin(),
                APIPlugin(),
                SimpleTriggerPlugin(),
            ],
        )
        class Order:
            __tablename__ = "orders"
            __table_args__ = {"schema": "dim"}

            total = Column(Integer)

        # API view registered
        views = md.info.get("views")
        assert views is not None
        # Triggers registered
        triggers = md.info.get("triggers")
        assert triggers is not None

    def test_capture_plugin_sees_correct_context(self):
        md = MetaData()

        @register(
            metadata=md,
            plugins=[
                SerialPKPlugin(),
                SimpleTablePlugin(),
                _CapturePlugin(),
            ],
        )
        class Thing:
            __tablename__ = "things"
            __table_args__ = {"schema": "inventory"}

            value = Column(String)

        assert _CapturePlugin.captured["tablename"] == "things"
        assert _CapturePlugin.captured["schemaname"] == "inventory"
        pk = _CapturePlugin.captured["pk_columns"]
        assert isinstance(pk, PrimaryKeyColumns)
        assert pk.first_key == "id"

    def test_schema_items_are_declared_columns(self):
        md = MetaData()

        @register(
            metadata=md,
            plugins=[
                SerialPKPlugin(),
                SimpleTablePlugin(),
                _CapturePlugin(),
            ],
        )
        class Entity:
            __tablename__ = "entities"
            __table_args__ = {"schema": "dim"}

            label = Column(String)
            count = Column(Integer)

        items = _CapturePlugin.captured["schema_items"]
        keys = [col.key for col in items if isinstance(col, Column)]
        assert "label" in keys
        assert "count" in keys

    def test_cave_check_collected_from_class_dict(self):
        md = MetaData()

        @register(
            metadata=md,
            plugins=[
                SerialPKPlugin(),
                SimpleTablePlugin(),
                _CapturePlugin(),
            ],
        )
        class Priced:
            __tablename__ = "priced"
            __table_args__ = {"schema": "dim"}

            price = Column(Integer)
            positive_price = CaveCheck("{price} > 0", name="pos")

        items = _CapturePlugin.captured["schema_items"]
        checks = [i for i in items if isinstance(i, CaveCheck)]
        assert len(checks) == 1
        assert checks[0].name == "pos"


# -------------------------------------------------------------------
# ORM mapping via base=
# -------------------------------------------------------------------


class TestRegisterWithBase:
    def setup_method(self):
        Base.metadata.clear()

    def test_select_works_after_mapping(self):
        @register(
            base=Base,
            plugins=[SerialPKPlugin(), SimpleTablePlugin()],
        )
        class Customer:
            __tablename__ = "customers"
            __table_args__ = {"schema": "public"}

            name = Column(String)

        stmt = select(Customer)
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "customers" in sql

    def test_mapped_class_has_column_attributes(self):
        @register(
            base=Base,
            plugins=[SerialPKPlugin(), SimpleTablePlugin()],
        )
        class Account:
            __tablename__ = "accounts"
            __table_args__ = {"schema": "public"}

            email = Column(String)

        # After mapping, class attributes reference columns.
        assert hasattr(Account, "id")
        assert hasattr(Account, "email")

    def test_uses_base_metadata(self):
        @register(
            base=Base,
            plugins=[SerialPKPlugin(), SimpleTablePlugin()],
        )
        class Tenant:
            __tablename__ = "tenants"
            __table_args__ = {"schema": "public"}

            name = Column(String)

        assert "public.tenants" in Base.metadata.tables


# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------


class TestRegisterValidation:
    def setup_method(self):
        Base.metadata.clear()

    def test_declarative_subclass_raises(self):
        with pytest.raises(CaveValidationError, match="__table__"):

            @register(plugins=[], metadata=MetaData())
            class Bad(Base):
                __tablename__ = "bad"
                __table_args__ = {"schema": "s"}

                id: Mapped[int] = mapped_column(primary_key=True)

    def test_no_metadata_or_base_raises(self):
        with pytest.raises(CaveValidationError, match="metadata"):

            @register(plugins=[])
            class Bad:
                __tablename__ = "x"
                __table_args__ = {"schema": "s"}

    def test_no_tablename_raises(self):
        with pytest.raises(CaveValidationError, match="__tablename__"):

            @register(metadata=MetaData(), plugins=[])
            class Bad:
                __table_args__ = {"schema": "s"}

    def test_no_schema_raises(self):
        with pytest.raises(CaveValidationError, match="schema"):

            @register(metadata=MetaData(), plugins=[])
            class Bad:
                __tablename__ = "x"

    def test_no_root_raises(self):
        with pytest.raises(CaveValidationError, match="__root__"):

            @register(
                metadata=MetaData(),
                plugins=[SerialPKPlugin()],
            )
            class Bad:
                __tablename__ = "x"
                __table_args__ = {"schema": "s"}

    def test_table_args_as_tuple(self):
        """Schema extracted from tuple-form __table_args__."""
        md = MetaData()

        @register(
            metadata=md,
            plugins=[SerialPKPlugin(), SimpleTablePlugin()],
        )
        class TupleArgs:
            __tablename__ = "tuple_test"
            __table_args__ = ({"schema": "dim"},)

            val = Column(String)

        assert "dim.tuple_test" in md.tables


# -------------------------------------------------------------------
# __root__ via factory path
# -------------------------------------------------------------------


class TestRootKeyViaFactory:
    """Verify __root__ is set by table/view-producing plugins."""

    def test_simple_table_plugin_sets_root(self):
        @requires("__root__")
        class _Check(Plugin):
            root = None

            def run(self, ctx: FactoryContext) -> None:
                _Check.root = ctx["__root__"]

        from cave.factory.base import ResourceFactory

        ResourceFactory(
            "t",
            "s",
            MetaData(),
            [Column("x", String)],
            plugins=[
                SerialPKPlugin(),
                SimpleTablePlugin(),
                _Check(),
            ],
        )
        assert _Check.root is not None
        assert isinstance(_Check.root, Table)
