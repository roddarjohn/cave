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
    def test_basic_raw_references(self):
        fk = PGCraftFK(
            raw_references={"{org_id}": "public.orgs.id"},
            name="fk_org",
        )
        assert fk.raw_references == {"{org_id}": "public.orgs.id"}
        assert fk.references == {}
        assert fk.name == "fk_org"
        assert fk.ondelete is None
        assert fk.onupdate is None

    def test_basic_references(self):
        fk = PGCraftFK(
            references={"{org_id}": "org.id"},
            name="fk_org",
        )
        assert fk.references == {"{org_id}": "org.id"}
        assert fk.raw_references == {}

    def test_cascade_options(self):
        fk = PGCraftFK(
            raw_references={"{org_id}": "public.orgs.id"},
            name="fk_org",
            ondelete="CASCADE",
            onupdate="SET NULL",
        )
        assert fk.ondelete == "CASCADE"
        assert fk.onupdate == "SET NULL"

    def test_multi_column_raw(self):
        fk = PGCraftFK(
            raw_references={
                "{a}": "public.other.x",
                "{b}": "public.other.y",
            },
            name="fk_ab",
        )
        assert len(fk.raw_references) == 2

    def test_both_references_raises(self):
        with pytest.raises(PGCraftValidationError, match="not both"):
            PGCraftFK(
                references={"{a}": "org.id"},
                raw_references={"{a}": "public.orgs.id"},
                name="fk_bad",
            )

    def test_neither_references_raises(self):
        with pytest.raises(
            PGCraftValidationError,
            match="provide either",
        ):
            PGCraftFK(name="fk_bad")

    def test_is_frozen(self):
        fk = PGCraftFK(
            references={"{a}": "t.a"},
            name="fk_a",
        )
        with pytest.raises(AttributeError):
            fk.name = "changed"  # type: ignore[misc]

    def test_column_names(self):
        fk = PGCraftFK(
            references={"{org_id}": "customer.id"},
            name="fk_org",
        )
        assert fk.column_names() == ["org_id"]

    def test_column_names_multi(self):
        fk = PGCraftFK(
            raw_references={
                "{a}": "s.t.x",
                "{b}": "s.t.y",
            },
            name="fk_ab",
        )
        assert fk.column_names() == ["a", "b"]

    def test_resolve_identity(self):
        fk = PGCraftFK(
            references={"{org_id}": "customer.id"},
            name="fk_org",
        )
        assert fk.resolve(lambda c: c) == ["org_id"]

    def test_resolve_references_raw_passthrough(self):
        metadata = MetaData()
        fk = PGCraftFK(
            raw_references={"{org_id}": "public.orgs.id"},
            name="fk_org",
        )
        resolved = fk.resolve_references(metadata)
        assert resolved == ["public.orgs.id"]


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

    def test_three_part_raises(self):
        """Three-part refs must use raw_references."""
        metadata = MetaData()
        with pytest.raises(
            PGCraftValidationError,
            match="raw_references",
        ):
            resolve_fk_reference(metadata, "dim.customer.id")

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
            references={"{org_id}": "org.id"},
            name="fk_org",
        )
        resolved = fk.resolve_references(metadata)
        assert resolved == ["dim.org.id"]


class TestCollectFKs:
    def test_filters_fks_from_mixed_list(self):
        from sqlalchemy import Column, Integer

        items = [
            Column("org_id", Integer),
            PGCraftFK(
                references={"{org_id}": "org.id"},
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
