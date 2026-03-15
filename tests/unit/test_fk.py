"""Unit tests for pgcraft.fk module."""

import pytest
from sqlalchemy import MetaData

from pgcraft.errors import PGCraftValidationError
from pgcraft.fk import (
    DimensionRef,
    PGCraftFK,
    collect_fks,
    register_dimension,
    resolve_fk_reference,
)


class TestPGCraftFK:
    def test_basic_construction(self):
        fk = PGCraftFK(
            columns=["{org_id}"],
            references=["public.orgs.id"],
            name="fk_org",
        )
        assert fk.columns == ["{org_id}"]
        assert fk.references == ["public.orgs.id"]
        assert fk.name == "fk_org"
        assert fk.ondelete is None
        assert fk.onupdate is None

    def test_cascade_options(self):
        fk = PGCraftFK(
            columns=["{org_id}"],
            references=["public.orgs.id"],
            name="fk_org",
            ondelete="CASCADE",
            onupdate="SET NULL",
        )
        assert fk.ondelete == "CASCADE"
        assert fk.onupdate == "SET NULL"

    def test_multi_column(self):
        fk = PGCraftFK(
            columns=["{a}", "{b}"],
            references=["public.other.x", "public.other.y"],
            name="fk_ab",
        )
        assert fk.columns == ["{a}", "{b}"]
        assert len(fk.references) == 2

    def test_is_frozen(self):
        fk = PGCraftFK(
            columns=["{a}"],
            references=["t.a"],
            name="fk_a",
        )
        with pytest.raises(AttributeError):
            fk.name = "changed"  # type: ignore[misc]

    def test_column_names(self):
        fk = PGCraftFK(
            columns=["{org_id}"],
            references=["customer.id"],
            name="fk_org",
        )
        assert fk.column_names() == ["org_id"]

    def test_column_names_multi(self):
        fk = PGCraftFK(
            columns=["{a}", "{b}"],
            references=["t.x", "t.y"],
            name="fk_ab",
        )
        assert fk.column_names() == ["a", "b"]

    def test_resolve_identity(self):
        fk = PGCraftFK(
            columns=["{org_id}"],
            references=["customer.id"],
            name="fk_org",
        )
        assert fk.resolve(lambda c: c) == ["org_id"]


class TestDimensionRegistry:
    def test_register_and_resolve(self):
        metadata = MetaData()
        register_dimension(
            metadata,
            "customer",
            DimensionRef(schema="dim", table="customer"),
        )
        result = resolve_fk_reference(metadata, "customer.id")
        assert result == "dim.customer.id"

    def test_resolve_append_only_root(self):
        metadata = MetaData()
        register_dimension(
            metadata,
            "customer",
            DimensionRef(schema="dim", table="customer_root"),
        )
        result = resolve_fk_reference(metadata, "customer.id")
        assert result == "dim.customer_root.id"

    def test_three_part_passthrough(self):
        metadata = MetaData()
        result = resolve_fk_reference(metadata, "dim.customer.id")
        assert result == "dim.customer.id"

    def test_unknown_dimension_raises(self):
        metadata = MetaData()
        with pytest.raises(PGCraftValidationError, match="unknown"):
            resolve_fk_reference(metadata, "nonexistent.id")

    def test_error_lists_known_dimensions(self):
        metadata = MetaData()
        register_dimension(
            metadata,
            "org",
            DimensionRef(schema="dim", table="org"),
        )
        with pytest.raises(PGCraftValidationError, match="org"):
            resolve_fk_reference(metadata, "bogus.id")

    def test_resolve_references_method(self):
        metadata = MetaData()
        register_dimension(
            metadata,
            "org",
            DimensionRef(schema="dim", table="org"),
        )
        fk = PGCraftFK(
            columns=["{org_id}"],
            references=["org.id"],
            name="fk_org",
        )
        resolved = fk.resolve_references(metadata)
        assert resolved == ["dim.org.id"]

    def test_resolve_references_mixed(self):
        """Two-part resolved, three-part passed through."""
        metadata = MetaData()
        register_dimension(
            metadata,
            "org",
            DimensionRef(schema="dim", table="org"),
        )
        fk = PGCraftFK(
            columns=["{a}", "{b}"],
            references=["org.id", "other.schema.col"],
            name="fk_mixed",
        )
        resolved = fk.resolve_references(metadata)
        assert resolved == ["dim.org.id", "other.schema.col"]


class TestCollectFKs:
    def test_filters_fks_from_mixed_list(self):
        from sqlalchemy import Column, Integer

        items = [
            Column("org_id", Integer),
            PGCraftFK(
                columns=["{org_id}"],
                references=["org.id"],
                name="fk_org",
            ),
        ]
        result = collect_fks(items)
        assert len(result) == 1
        assert result[0].name == "fk_org"

    def test_empty_list(self):
        assert collect_fks([]) == []

    def test_no_fks_in_list(self):
        from sqlalchemy import Column, String

        assert collect_fks([Column("x", String)]) == []
