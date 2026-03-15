"""Unit tests for pgcraft.fk module."""

import pytest

from pgcraft.fk import PGCraftFK, collect_fks


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
            references=[
                "public.other.x",
                "public.other.y",
            ],
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
            references=["public.orgs.id"],
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
            references=["public.orgs.id"],
            name="fk_org",
        )
        assert fk.resolve(lambda c: c) == ["org_id"]


class TestCollectFKs:
    def test_filters_fks_from_mixed_list(self):
        from sqlalchemy import Column, Integer

        items = [
            Column("org_id", Integer),
            PGCraftFK(
                columns=["{org_id}"],
                references=["public.orgs.id"],
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
