"""Unit tests for pgcraft.utils.naming_convention."""

import hashlib

from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)

from pgcraft.utils.naming_convention import (
    _cols,
    _make_token,
    pgcraft_build_naming_conventions,
)

# ---------------------------------------------------------------------------
# pgcraft_build_naming_conventions structure
# ---------------------------------------------------------------------------


class TestBuildNamingConventionStructure:
    def test_returns_dict(self):
        assert isinstance(pgcraft_build_naming_conventions(), dict)

    def test_contains_required_keys(self):
        conv = pgcraft_build_naming_conventions()
        for key in ("fk", "uq", "ix", "pk", "ck"):
            assert key in conv

    def test_contains_token_callable_keys(self):
        conv = pgcraft_build_naming_conventions()
        for key in ("fk_name", "uq_name", "ix_name", "pk_name", "ck_name"):
            assert key in conv

    def test_template_strings_reference_tokens(self):
        conv = pgcraft_build_naming_conventions()
        assert conv["fk"] == "%(fk_name)s"
        assert conv["uq"] == "%(uq_name)s"
        assert conv["ix"] == "%(ix_name)s"
        assert conv["pk"] == "%(pk_name)s"
        assert conv["ck"] == "%(ck_name)s"

    def test_token_callables_are_callable(self):
        conv = pgcraft_build_naming_conventions()
        for key in ("fk_name", "uq_name", "ix_name", "pk_name", "ck_name"):
            assert callable(conv[key])


class TestCols:
    def test_single_column(self):
        md = MetaData()
        t = Table(
            "t",
            md,
            Column("id", Integer, primary_key=True),
            Column("email", String),
        )
        uq = UniqueConstraint(t.c.email)
        assert _cols(uq, t) == "email"

    def test_multiple_columns(self):
        md = MetaData()
        t = Table(
            "t",
            md,
            Column("id", Integer, primary_key=True),
            Column("a", String),
            Column("b", String),
        )
        uq = UniqueConstraint(t.c.a, t.c.b)
        result = _cols(uq, t)
        assert result == "a_b"


# ---------------------------------------------------------------------------
# Token callable: unique constraint naming
# ---------------------------------------------------------------------------


class TestUniqueConstraintNaming:
    def _make_metadata(self) -> tuple[MetaData, Table]:
        conv = pgcraft_build_naming_conventions()
        md = MetaData(naming_convention=conv)
        t = Table(
            "users",
            md,
            Column("id", Integer, primary_key=True),
            Column("email", String),
            schema="myschema",
        )
        return md, t

    def test_uq_name_format(self):
        _md, t = self._make_metadata()
        conv = pgcraft_build_naming_conventions()
        uq = UniqueConstraint(t.c.email)
        name = conv["uq_name"](uq, t)
        assert name == "uq__users__email"

    def test_uq_name_multi_column(self):
        conv = pgcraft_build_naming_conventions()
        md = MetaData(naming_convention=conv)
        t = Table(
            "orders",
            md,
            Column("id", Integer, primary_key=True),
            Column("a", String),
            Column("b", String),
        )
        uq = UniqueConstraint(t.c.a, t.c.b)
        name = conv["uq_name"](uq, t)
        assert name == "uq__orders__a_b"


# ---------------------------------------------------------------------------
# Token callable: FK naming
# ---------------------------------------------------------------------------


class TestFKNaming:
    def test_fk_name_format(self):
        conv = pgcraft_build_naming_conventions()
        md = MetaData(naming_convention=conv)
        Table(
            "parent",
            md,
            Column("id", Integer, primary_key=True),
            schema="s",
        )
        child = Table(
            "child",
            md,
            Column("id", Integer, primary_key=True),
            Column("parent_id", ForeignKey("s.parent.id")),
            schema="s",
        )
        for fk_constraint in child.foreign_key_constraints:
            name = conv["fk_name"](fk_constraint, child)
        assert name == "fk__child__parent_id__parent"


# ---------------------------------------------------------------------------
# Token callable: truncation with MD5 digest
# ---------------------------------------------------------------------------


class TestTokenTruncation:
    def test_long_name_truncated_to_max_length(self):
        max_len = 20
        conv = pgcraft_build_naming_conventions(max_length=max_len)
        md = MetaData(naming_convention=conv)
        t = Table(
            "my_very_long_table",
            md,
            Column("id", Integer, primary_key=True),
            Column("some_col", String),
        )
        uq = UniqueConstraint(t.c.some_col)
        name = conv["uq_name"](uq, t)
        assert len(name) <= max_len

    def test_long_name_ends_with_digest(self):
        """Truncated names must end with an 8-char MD5 hex digest."""
        conv = pgcraft_build_naming_conventions(max_length=20)
        md = MetaData(naming_convention=conv)
        t = Table(
            "my_very_long_table",
            md,
            Column("id", Integer, primary_key=True),
            Column("some_col", String),
        )
        uq = UniqueConstraint(t.c.some_col)
        name = conv["uq_name"](uq, t)
        # Verify the suffix is a valid 8-char hex string
        suffix = name[-8:]
        assert all(c in "0123456789abcdef" for c in suffix)

    def test_short_name_not_truncated(self):
        """Short names should not be modified."""
        conv = pgcraft_build_naming_conventions(max_length=63)
        md = MetaData(naming_convention=conv)
        t = Table(
            "t",
            md,
            Column("id", Integer, primary_key=True),
            Column("x", String),
        )
        uq = UniqueConstraint(t.c.x)
        name = conv["uq_name"](uq, t)
        assert name == "uq__t__x"

    def test_truncation_digest_is_md5_based(self):
        """The 8-char digest must match the first 8 chars of MD5(full)."""
        conv = pgcraft_build_naming_conventions(max_length=20)
        md = MetaData(naming_convention=conv)
        t = Table(
            "my_very_long_table",
            md,
            Column("id", Integer, primary_key=True),
            Column("some_col", String),
        )
        uq = UniqueConstraint(t.c.some_col)
        name = conv["uq_name"](uq, t)
        full = "uq__my_very_long_table__some_col"
        expected_digest = hashlib.md5(
            full.encode(), usedforsecurity=False
        ).hexdigest()[:8]
        assert name.endswith(expected_digest)

    def test_max_length_parameter_respected(self):
        """Custom max_length values should be honoured."""
        for max_len in (15, 30, 50):
            conv = pgcraft_build_naming_conventions(max_length=max_len)
            md = MetaData(naming_convention=conv)
            t = Table(
                "a_very_long_table_name",
                md,
                Column("id", Integer, primary_key=True),
                Column("some_very_long_column_name", String),
            )
            uq = UniqueConstraint(t.c.some_very_long_column_name)
            name = conv["uq_name"](uq, t)
            assert len(name) <= max_len


# ---------------------------------------------------------------------------
# _make_token directly
# ---------------------------------------------------------------------------


class TestMakeToken:
    def test_token_without_ref(self):
        token = _make_token("pfx", _cols)
        md = MetaData()
        t = Table(
            "tbl",
            md,
            Column("id", Integer, primary_key=True),
            Column("c", String),
        )
        uq = UniqueConstraint(t.c.c)
        assert token(uq, t) == "pfx__tbl__c"

    def test_token_prefix_in_name(self):
        token = _make_token("mypfx", _cols)
        md = MetaData()
        t = Table(
            "tbl",
            md,
            Column("id", Integer, primary_key=True),
            Column("c", String),
        )
        uq = UniqueConstraint(t.c.c)
        name = token(uq, t)
        assert name.startswith("mypfx__")
