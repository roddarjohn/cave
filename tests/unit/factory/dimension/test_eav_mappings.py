"""Unit tests for EAV mapping construction.

Pure Python tests — no database required.
"""

from sqlalchemy import Column, Float, Integer, String

from pgcraft.plugins.eav import _build_eav_mappings


class TestBuildEAVMappings:
    def test_nullable_true_by_default(self):
        cols = [Column("color", String)]
        assert _build_eav_mappings(cols)[0].nullable is True

    def test_explicit_nullable_true(self):
        cols = [Column("color", String, nullable=True)]
        assert _build_eav_mappings(cols)[0].nullable is True

    def test_explicit_nullable_false(self):
        cols = [Column("color", String, nullable=False)]
        assert _build_eav_mappings(cols)[0].nullable is False

    def test_mixed_columns(self):
        cols = [
            Column("required", Integer, nullable=False),
            Column("optional", Float),
        ]
        mappings = _build_eav_mappings(cols)
        assert mappings[0].nullable is False
        assert mappings[1].nullable is True
