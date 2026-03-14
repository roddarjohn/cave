"""Unit tests for build_metadata — no database connection required."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Integer,
    MetaData,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from pgcraft.runtime.builder import _build_column, build_metadata
from pgcraft.runtime.config import ColumnConfig, DimensionConfig

# ---------------------------------------------------------------------------
# _build_column
# ---------------------------------------------------------------------------


class TestBuildColumn:
    def test_text_type(self):
        col = _build_column(ColumnConfig(name="label", type="text"))
        assert isinstance(col.type, Text)
        assert col.name == "label"

    def test_integer_type(self):
        col = _build_column(ColumnConfig(name="count", type="integer"))
        assert isinstance(col.type, Integer)

    def test_bigint_type(self):
        col = _build_column(ColumnConfig(name="n", type="bigint"))
        assert isinstance(col.type, BigInteger)

    def test_boolean_type(self):
        col = _build_column(ColumnConfig(name="flag", type="boolean"))
        assert isinstance(col.type, Boolean)

    def test_timestamptz_type(self):
        col = _build_column(ColumnConfig(name="ts", type="timestamptz"))
        assert isinstance(col.type, DateTime)
        assert col.type.timezone is True

    def test_date_type(self):
        col = _build_column(ColumnConfig(name="d", type="date"))
        assert isinstance(col.type, Date)

    def test_uuid_type(self):
        col = _build_column(ColumnConfig(name="uid", type="uuid"))
        assert isinstance(col.type, UUID)

    def test_jsonb_type(self):
        col = _build_column(ColumnConfig(name="data", type="jsonb"))
        assert isinstance(col.type, JSONB)

    def test_nullable_true(self):
        col = _build_column(ColumnConfig(name="x", type="text", nullable=True))
        assert col.nullable is True

    def test_nullable_false(self):
        col = _build_column(ColumnConfig(name="x", type="text", nullable=False))
        assert col.nullable is False

    def test_no_default(self):
        col = _build_column(ColumnConfig(name="x", type="text"))
        assert col.server_default is None

    def test_server_default_set(self):
        col = _build_column(
            ColumnConfig(name="ts", type="timestamptz", default="now()")
        )
        assert col.server_default is not None
        assert "now()" in str(col.server_default.arg)


# ---------------------------------------------------------------------------
# build_metadata
# ---------------------------------------------------------------------------


class TestBuildMetadata:
    def _simple_config(self, **kwargs) -> DimensionConfig:  # type: ignore[return]
        defaults = {
            "table_name": "product",
            "columns": [ColumnConfig(name="label", type="text")],
        }
        defaults.update(kwargs)
        return DimensionConfig(**defaults)

    def test_returns_metadata(self):
        cfg = self._simple_config()
        meta = build_metadata(cfg, schema="tenant_abc")
        assert isinstance(meta, MetaData)

    def test_table_registered_on_metadata(self):
        cfg = self._simple_config(table_name="product")
        meta = build_metadata(cfg, schema="tenant_abc")
        assert "tenant_abc.product" in meta.tables

    def test_schema_in_table_key(self):
        cfg = self._simple_config(table_name="order")
        meta = build_metadata(cfg, schema="tenant_xyz")
        assert "tenant_xyz.order" in meta.tables

    def test_declared_column_present(self):
        cfg = self._simple_config(
            columns=[ColumnConfig(name="label", type="text")]
        )
        meta = build_metadata(cfg, schema="s")
        table = meta.tables["s.product"]
        assert "label" in table.c

    def test_pk_column_present(self):
        cfg = self._simple_config()
        meta = build_metadata(cfg, schema="s")
        table = meta.tables["s.product"]
        pk_cols = [c for c in table.c if c.primary_key]
        assert len(pk_cols) == 1

    def test_serial_pk_is_integer(self):
        cfg = self._simple_config(pk="serial")
        meta = build_metadata(cfg, schema="s")
        table = meta.tables["s.product"]
        pk = next(c for c in table.c if c.primary_key)
        assert isinstance(pk.type, Integer)

    def test_uuidv4_pk_is_uuid(self):
        cfg = self._simple_config(pk="uuidv4")
        meta = build_metadata(cfg, schema="s")
        table = meta.tables["s.product"]
        pk = next(c for c in table.c if c.primary_key)
        assert isinstance(pk.type, UUID)
        assert "gen_random_uuid" in str(pk.server_default.arg)

    def test_uuidv7_pk_is_uuid_with_correct_default(self):
        cfg = self._simple_config(pk="uuidv7")
        meta = build_metadata(cfg, schema="s")
        table = meta.tables["s.product"]
        pk = next(c for c in table.c if c.primary_key)
        assert isinstance(pk.type, UUID)
        assert "uuid_generate_v7" in str(pk.server_default.arg)

    def test_uuidv7_registers_extension_on_metadata(self):
        cfg = self._simple_config(pk="uuidv7")
        meta = build_metadata(cfg, schema="s")
        assert "pg_uuidv7" in meta.info.get("pgcraft_extensions", set())

    def test_serial_does_not_register_extensions(self):
        cfg = self._simple_config(pk="serial")
        meta = build_metadata(cfg, schema="s")
        assert not meta.info.get("pgcraft_extensions", set())

    def test_multiple_columns(self):
        cfg = DimensionConfig(
            table_name="product",
            columns=[
                ColumnConfig(name="label", type="text"),
                ColumnConfig(name="price", type="numeric"),
                ColumnConfig(name="active", type="boolean"),
            ],
        )
        meta = build_metadata(cfg, schema="s")
        table = meta.tables["s.product"]
        col_names = {c.name for c in table.c if not c.primary_key}
        assert {"label", "price", "active"}.issubset(col_names)

    def test_schema_registered_on_metadata_info(self):
        # pgcraft_configure_metadata should populate metadata.info["schemas"]
        cfg = self._simple_config()
        meta = build_metadata(cfg, schema="tenant_abc")
        schemas = meta.info.get("schemas")
        assert schemas is not None
        schema_names = {s.name for s in schemas.schemas}
        assert "tenant_abc" in schema_names
