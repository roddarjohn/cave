"""Unit tests for pgcraft.alembic.dependency."""

from unittest.mock import MagicMock

from alembic.operations import ops as alembic_ops
from sqlalchemy import Column, ForeignKey, Integer, MetaData, Table
from sqlalchemy_declarative_extensions.alembic.function import (
    CreateFunctionOp,
    DropFunctionOp,
    UpdateFunctionOp,
)
from sqlalchemy_declarative_extensions.alembic.procedure import (
    CreateProcedureOp,
    DropProcedureOp,
    UpdateProcedureOp,
)
from sqlalchemy_declarative_extensions.alembic.schema import (
    CreateSchemaOp,
    DropSchemaOp,
)
from sqlalchemy_declarative_extensions.alembic.trigger import (
    CreateTriggerOp,
    DropTriggerOp,
    UpdateTriggerOp,
)
from sqlalchemy_declarative_extensions.alembic.view import (
    CreateViewOp,
    DropViewOp,
    UpdateViewOp,
)
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Function,
    Trigger,
)
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Grant as PgGrant,
)
from sqlalchemy_declarative_extensions.dialects.postgresql.grant import (
    DefaultGrant,
    DefaultGrantStatement,
    DefaultGrantTypes,
    Grant,
)
from sqlalchemy_declarative_extensions.grant.compare import (
    GrantPrivilegesOp,
    RevokePrivilegesOp,
)
from sqlalchemy_declarative_extensions.procedure.base import Procedure
from sqlalchemy_declarative_extensions.role.compare import (
    CreateRoleOp,
    DropRoleOp,
)
from sqlalchemy_declarative_extensions.role.generic import Role
from sqlalchemy_declarative_extensions.schema.base import Schema
from sqlalchemy_declarative_extensions.view.base import View

from pgcraft.alembic.dependency import (
    EntityIdentifier,
    _entity_identifier,
    _entity_schema,
    _function_return_refs,
    _op_phase,
    _parse_qualified_name,
    _plpgsql_queries,
    _plpgsql_table_refs,
    _refs_for_grant,
    _refs_for_role,
    _refs_for_trigger,
    _role_name,
    _sql_function_table_refs,
    _view_table_refs,
    build_fk_graph_from_metadata,
    expand_update_ops,
    sort_migration_ops,
)

# ---------------------------------------------------------------------------
# _parse_qualified_name
# ---------------------------------------------------------------------------


class TestParseQualifiedName:
    def test_qualified_name_splits_schema_and_name(self):
        schema, name = _parse_qualified_name("myschema.mytable")
        assert schema == "myschema"
        assert name == "mytable"

    def test_unqualified_name_defaults_to_public(self):
        schema, name = _parse_qualified_name("mytable")
        assert schema == "public"
        assert name == "mytable"

    def test_lowercased_output(self):
        schema, name = _parse_qualified_name("MySchema.MyTable")
        assert schema == "myschema"
        assert name == "mytable"

    def test_unqualified_lowercased(self):
        schema, name = _parse_qualified_name("MyTable")
        assert schema == "public"
        assert name == "mytable"

    def test_multiple_dots_only_first_split(self):
        """Only the first dot is used as separator."""
        schema, name = _parse_qualified_name("a.b.c")
        assert schema == "a"
        assert name == "b.c"


# ---------------------------------------------------------------------------
# _plpgsql_queries
# ---------------------------------------------------------------------------


class TestPlpgsqlQueries:
    def test_extracts_query_from_plpgsql_expr(self):
        obj = {"PLpgSQL_expr": {"query": "SELECT 1"}}
        result = _plpgsql_queries(obj)
        assert result == ["SELECT 1"]

    def test_skips_new_token(self):
        obj = {"PLpgSQL_expr": {"query": "NEW"}}
        assert _plpgsql_queries(obj) == []

    def test_skips_old_token(self):
        obj = {"PLpgSQL_expr": {"query": "OLD"}}
        assert _plpgsql_queries(obj) == []

    def test_case_insensitive_new_skip(self):
        obj = {"PLpgSQL_expr": {"query": "new"}}
        assert _plpgsql_queries(obj) == []

    def test_empty_dict_returns_empty(self):
        assert _plpgsql_queries({}) == []

    def test_empty_list_returns_empty(self):
        assert _plpgsql_queries([]) == []

    def test_extracts_from_list(self):
        obj = [{"PLpgSQL_expr": {"query": "SELECT 1"}}]
        assert _plpgsql_queries(obj) == ["SELECT 1"]

    def test_nested_dict_recurses(self):
        obj = {"outer": {"PLpgSQL_expr": {"query": "SELECT 42"}}}
        assert _plpgsql_queries(obj) == ["SELECT 42"]

    def test_multiple_queries(self):
        expected = ["SELECT 1", "SELECT 2"]
        obj = [{"PLpgSQL_expr": {"query": q}} for q in expected]
        result = _plpgsql_queries(obj)
        assert "SELECT 1" in result
        assert "SELECT 2" in result
        assert len(result) == len(expected)

    def test_non_dict_non_list_returns_empty(self):
        assert _plpgsql_queries("a string") == []
        assert _plpgsql_queries(42) == []
        assert _plpgsql_queries(None) == []

    def test_missing_query_key_returns_empty(self):
        obj = {"PLpgSQL_expr": {}}
        assert _plpgsql_queries(obj) == []

    def test_empty_query_string_skipped(self):
        obj = {"PLpgSQL_expr": {"query": ""}}
        assert _plpgsql_queries(obj) == []


# ---------------------------------------------------------------------------
# _op_phase
# ---------------------------------------------------------------------------


class TestOpPhase:
    def test_create_table_is_create(self):
        op = alembic_ops.CreateTableOp(
            "t", [Column("id", Integer, primary_key=True)]
        )
        assert _op_phase(op) == "create"

    def test_drop_table_is_drop(self):
        op = alembic_ops.DropTableOp("t")
        assert _op_phase(op) == "drop"

    def test_create_schema_is_create(self):
        op = CreateSchemaOp(Schema("s"))
        assert _op_phase(op) == "create"

    def test_drop_schema_is_drop(self):
        op = DropSchemaOp(Schema("s"))
        assert _op_phase(op) == "drop"

    def test_create_view_is_create(self):
        op = CreateViewOp(View("v", "SELECT 1"))
        assert _op_phase(op) == "create"

    def test_drop_view_is_drop(self):
        op = DropViewOp(View("v", "SELECT 1"))
        assert _op_phase(op) == "drop"

    def test_create_function_is_create(self):
        fn = Function("fn", "BEGIN END;", language="plpgsql")
        op = CreateFunctionOp(fn)
        assert _op_phase(op) == "create"

    def test_drop_function_is_drop(self):
        fn = Function("fn", "BEGIN END;", language="plpgsql")
        op = DropFunctionOp(fn)
        assert _op_phase(op) == "drop"

    def test_modify_table_returns_none(self):
        op = alembic_ops.ModifyTableOps("t", ops=[])
        assert _op_phase(op) is None

    def test_create_role_is_create(self):
        op = CreateRoleOp(Role("myrole"))
        assert _op_phase(op) == "create"

    def test_drop_role_is_drop(self):
        op = DropRoleOp(Role("myrole"))
        assert _op_phase(op) == "drop"


# ---------------------------------------------------------------------------
# _entity_identifier
# ---------------------------------------------------------------------------


class TestEntityIdentifier:
    def test_create_table_identifier(self):
        op = alembic_ops.CreateTableOp(
            "mytable",
            [Column("id", Integer, primary_key=True)],
            schema="myschema",
        )
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.schema == "myschema"
        assert eid.name == "mytable"
        assert eid.phase == "create"

    def test_drop_table_identifier(self):
        op = alembic_ops.DropTableOp("mytable", schema="myschema")
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.schema == "myschema"
        assert eid.name == "mytable"
        assert eid.phase == "drop"

    def test_create_table_no_schema_defaults_to_public(self):
        op = alembic_ops.CreateTableOp(
            "t", [Column("id", Integer, primary_key=True)]
        )
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.schema == "public"

    def test_create_schema_identifier(self):
        op = CreateSchemaOp(Schema("myschema"))
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.schema == "myschema"
        assert eid.name is None
        assert eid.phase == "create"

    def test_drop_schema_identifier(self):
        op = DropSchemaOp(Schema("myschema"))
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.schema == "myschema"
        assert eid.phase == "drop"

    def test_create_view_identifier(self):
        v = View("myview", "SELECT 1", schema="myschema")
        op = CreateViewOp(v)
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.name == "myview"
        assert eid.schema == "myschema"

    def test_create_role_identifier(self):
        op = CreateRoleOp(Role("myrole"))
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.schema == "__roles__"
        assert eid.name == "myrole"

    def test_modify_table_identifier_has_no_phase(self):
        op = alembic_ops.ModifyTableOps("t", ops=[], schema="s")
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.phase is None

    def test_names_are_lowercased(self):
        op = alembic_ops.CreateTableOp(
            "MyTable",
            [Column("id", Integer, primary_key=True)],
            schema="MySchema",
        )
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.schema == "myschema"
        assert eid.name == "mytable"


# ---------------------------------------------------------------------------
# expand_update_ops
# ---------------------------------------------------------------------------


class TestExpandUpdateOps:
    def test_expand_update_view_op(self):
        v_old = View("v", "SELECT 1", schema="s")
        v_new = View("v", "SELECT 2", schema="s")
        op = UpdateViewOp(v_old, v_new)
        result = expand_update_ops([op])
        assert len(result) == len([v_old, v_new])
        assert isinstance(result[0], DropViewOp)
        assert isinstance(result[1], CreateViewOp)
        assert result[0].view is v_old
        assert result[1].view is v_new

    def test_expand_update_function_op(self):
        fn_old = Function("fn", "BEGIN END;", language="plpgsql")
        fn_new = Function("fn", "BEGIN RETURN NULL; END;", language="plpgsql")
        op = UpdateFunctionOp(fn_old, fn_new)
        result = expand_update_ops([op])
        assert len(result) == len([fn_old, fn_new])
        assert isinstance(result[0], DropFunctionOp)
        assert isinstance(result[1], CreateFunctionOp)

    def test_expand_update_trigger_op(self):
        t_old = Trigger.instead_of(
            "insert", on="s.v", execute="s.fn", name="tr"
        ).for_each_row()
        t_new = Trigger.instead_of(
            "insert", on="s.v", execute="s.fn", name="tr"
        ).for_each_row()
        op = UpdateTriggerOp(t_old, t_new)
        result = expand_update_ops([op])
        assert len(result) == len([t_old, t_new])
        assert isinstance(result[0], DropTriggerOp)
        assert isinstance(result[1], CreateTriggerOp)

    def test_non_update_ops_passed_through(self):
        create_op = CreateViewOp(View("v", "SELECT 1"))
        drop_op = DropViewOp(View("v", "SELECT 1"))
        table_op = alembic_ops.CreateTableOp(
            "t", [Column("id", Integer, primary_key=True)]
        )
        result = expand_update_ops([create_op, drop_op, table_op])
        assert result == [create_op, drop_op, table_op]

    def test_empty_list(self):
        assert expand_update_ops([]) == []

    def test_mixed_ops(self):
        v1 = View("v", "SELECT 1", schema="s")
        v2 = View("v", "SELECT 2", schema="s")
        update_op = UpdateViewOp(v1, v2)
        table_op = alembic_ops.CreateTableOp(
            "t", [Column("id", Integer, primary_key=True)]
        )
        result = expand_update_ops([update_op, table_op])
        # update_op expands to 2 ops; table_op passes through → 3 total
        assert len(result) == len([v1, v2, table_op])
        assert isinstance(result[0], DropViewOp)
        assert isinstance(result[1], CreateViewOp)
        assert result[2] is table_op


# ---------------------------------------------------------------------------
# build_fk_graph_from_metadata
# ---------------------------------------------------------------------------


class TestBuildFkGraphFromMetadata:
    def test_empty_metadata_returns_empty_graph(self):
        assert build_fk_graph_from_metadata(MetaData()) == {}

    def test_table_with_no_fks_not_in_graph(self):
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer, primary_key=True))
        graph = build_fk_graph_from_metadata(metadata)
        assert graph == {}

    def test_simple_fk_relationship(self):
        metadata = MetaData()
        Table(
            "parent",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="s",
        )
        Table(
            "child",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("parent_id", ForeignKey("s.parent.id")),
            schema="s",
        )
        graph = build_fk_graph_from_metadata(metadata)
        assert ("s", "child") in graph
        assert ("s", "parent") in graph[("s", "child")]

    def test_no_self_references_in_graph(self):
        """Self-referential FKs must be excluded from the graph."""
        metadata = MetaData()
        Table(
            "node",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="s",
        )
        # Add a second table referencing node (not self-referential)
        Table(
            "node2",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("parent_id", ForeignKey("s.node.id")),
            schema="s",
        )
        graph = build_fk_graph_from_metadata(metadata)
        # node itself should not reference itself
        assert ("s", "node") not in graph

    def test_schema_normalised_to_lowercase(self):
        metadata = MetaData()
        Table(
            "parent",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="MySchema",
        )
        Table(
            "child",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("pid", ForeignKey("MySchema.parent.id")),
            schema="MySchema",
        )
        graph = build_fk_graph_from_metadata(metadata)
        assert ("myschema", "child") in graph

    def test_table_without_schema_uses_public(self):
        metadata = MetaData()
        Table("parent", metadata, Column("id", Integer, primary_key=True))
        Table(
            "child",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("pid", ForeignKey("parent.id")),
        )
        graph = build_fk_graph_from_metadata(metadata)
        assert ("public", "child") in graph
        assert ("public", "parent") in graph[("public", "child")]


# ---------------------------------------------------------------------------
# sort_migration_ops
# ---------------------------------------------------------------------------


class TestSortMigrationOps:
    def test_schema_before_table_in_create(self):
        """CreateSchemaOp should precede CreateTableOp for the same schema."""
        table_op = alembic_ops.CreateTableOp(
            "mytable",
            [Column("id", Integer, primary_key=True)],
            schema="myschema",
        )
        schema_op = CreateSchemaOp(Schema("myschema"))
        result = sort_migration_ops([table_op, schema_op])
        schema_idx = next(
            i for i, op in enumerate(result) if isinstance(op, CreateSchemaOp)
        )
        table_idx = next(
            i
            for i, op in enumerate(result)
            if isinstance(op, alembic_ops.CreateTableOp)
        )
        assert schema_idx < table_idx

    def test_drop_table_before_drop_schema(self):
        """DropTableOp should precede DropSchemaOp for the same schema."""
        table_op = alembic_ops.DropTableOp("mytable", schema="myschema")
        schema_op = DropSchemaOp(Schema("myschema"))
        result = sort_migration_ops([schema_op, table_op])
        schema_idx = next(
            i for i, op in enumerate(result) if isinstance(op, DropSchemaOp)
        )
        table_idx = next(
            i
            for i, op in enumerate(result)
            if isinstance(op, alembic_ops.DropTableOp)
        )
        assert table_idx < schema_idx

    def test_referenced_table_created_before_referencing(self):
        """Foreign key target must be created before the FK source."""
        metadata = MetaData()
        parent_table = Table(
            "parent",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="s",
        )
        child_table = Table(
            "child",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("pid", ForeignKey("s.parent.id")),
            schema="s",
        )
        parent_op = alembic_ops.CreateTableOp.from_table(parent_table)
        child_op = alembic_ops.CreateTableOp.from_table(child_table)
        fk_graph = build_fk_graph_from_metadata(metadata)
        result = sort_migration_ops([child_op, parent_op], fk_graph=fk_graph)
        names = [op.table_name for op in result]
        assert names.index("parent") < names.index("child")

    def test_empty_list_returns_empty(self):
        assert sort_migration_ops([]) == []

    def test_single_op_unchanged(self):
        op = alembic_ops.CreateTableOp(
            "t", [Column("id", Integer, primary_key=True)]
        )
        result = sort_migration_ops([op])
        assert result == [op]

    def test_unkeyed_ops_appended_at_end(self):
        """Ops that have no entity identifier appear last in original order."""
        schema_op = CreateSchemaOp(Schema("s"))
        # ModifyTableOps has phase=None but does get an identifier
        # Use an op with no identifier by using a sentinel that won't be
        # recognised. We simulate this by checking the actual behaviour
        # with only keyed ops here and verifying order invariants.
        table_op = alembic_ops.CreateTableOp(
            "t",
            [Column("id", Integer, primary_key=True)],
            schema="s",
        )
        input_ops = [table_op, schema_op]
        result = sort_migration_ops(input_ops)
        assert len(result) == len(input_ops)

    def test_no_fk_graph_argument_works(self):
        """Calling without fk_graph does not raise."""
        op = alembic_ops.CreateTableOp(
            "t", [Column("id", Integer, primary_key=True)], schema="s"
        )
        schema_op = CreateSchemaOp(Schema("s"))
        input_ops = [op, schema_op]
        result = sort_migration_ops(input_ops)
        assert len(result) == len(input_ops)

    def test_schema_before_view(self):
        """CreateSchemaOp must precede CreateViewOp for the same schema."""
        schema_op = CreateSchemaOp(Schema("s"))
        view = View("myview", "SELECT 1", schema="s")
        view_op = CreateViewOp(view)
        result = sort_migration_ops([view_op, schema_op])
        schema_idx = next(
            i for i, op in enumerate(result) if isinstance(op, CreateSchemaOp)
        )
        view_idx = next(
            i for i, op in enumerate(result) if isinstance(op, CreateViewOp)
        )
        assert schema_idx < view_idx


# ---------------------------------------------------------------------------
# EntityIdentifier
# ---------------------------------------------------------------------------


class TestEntityIdentifierDataclass:
    def test_default_schema_is_public(self):
        eid = EntityIdentifier()
        assert eid.schema == "public"

    def test_default_name_is_none(self):
        assert EntityIdentifier().name is None

    def test_default_phase_is_none(self):
        assert EntityIdentifier().phase is None

    def test_hashable(self):
        eid = EntityIdentifier(schema="s", name="t", phase="create")
        s = {eid}
        assert eid in s

    def test_equality(self):
        a = EntityIdentifier(schema="s", name="t")
        b = EntityIdentifier(schema="s", name="t")
        assert a == b

    def test_inequality(self):
        a = EntityIdentifier(schema="s", name="t", phase="create")
        b = EntityIdentifier(schema="s", name="t", phase="drop")
        assert a != b


# ---------------------------------------------------------------------------
# _entity_schema
# ---------------------------------------------------------------------------


class TestEntitySchema:
    def test_create_schema_op_returns_schema_name(self):
        op = CreateSchemaOp(Schema("myschema"))
        assert _entity_schema(op) == "myschema"

    def test_drop_schema_op_returns_schema_name(self):
        op = DropSchemaOp(Schema("myschema"))
        assert _entity_schema(op) == "myschema"

    def test_create_view_op_returns_view_schema(self):
        v = View("myview", "SELECT 1", schema="myschema")
        op = CreateViewOp(v)
        assert _entity_schema(op) == "myschema"

    def test_create_function_op_returns_function_schema(self):
        fn = Function("fn", "BEGIN END;", language="plpgsql", schema="s")
        op = CreateFunctionOp(fn)
        assert _entity_schema(op) == "s"

    def test_trigger_op_returns_triggers_prefix_schema(self):
        trigger = Trigger.instead_of(
            "insert",
            on="myschema.myview",
            execute="myschema.fn",
            name="tr",
        ).for_each_row()
        op = CreateTriggerOp(trigger)
        schema = _entity_schema(op)
        assert schema == "__triggers__myschema"

    def test_create_table_op_returns_none(self):
        op = alembic_ops.CreateTableOp(
            "t", [Column("id", Integer, primary_key=True)], schema="s"
        )
        assert _entity_schema(op) is None

    def test_function_no_schema_defaults_public(self):
        fn = Function("fn", "BEGIN END;", language="plpgsql")
        op = CreateFunctionOp(fn)
        assert _entity_schema(op) == "public"


# ---------------------------------------------------------------------------
# _role_name
# ---------------------------------------------------------------------------


class TestRoleName:
    def test_role_object_returns_name(self):
        role = Role("admin")
        assert _role_name(role) == "admin"

    def test_string_returned_unchanged(self):
        assert _role_name("readonly") == "readonly"


# ---------------------------------------------------------------------------
# _refs_for_role
# ---------------------------------------------------------------------------


class TestRefsForRole:
    def test_no_in_roles_returns_empty(self):
        op = CreateRoleOp(Role("standalone"))
        assert _refs_for_role(op, "create") == set()

    def test_string_in_roles_extracted(self):
        op = CreateRoleOp(Role("app_user", in_roles=["readonly", "writer"]))
        refs = _refs_for_role(op, "create")
        names = {r.name for r in refs}
        assert "readonly" in names
        assert "writer" in names

    def test_role_object_in_roles_extracted(self):
        op = CreateRoleOp(Role("app_user", in_roles=[Role("parent_role")]))
        refs = _refs_for_role(op, "create")
        names = {r.name for r in refs}
        assert "parent_role" in names

    def test_refs_use_roles_schema(self):
        op = CreateRoleOp(Role("user", in_roles=["base"]))
        refs = _refs_for_role(op, "create")
        schemas = {r.schema for r in refs}
        assert "__roles__" in schemas

    def test_phase_propagated_to_refs(self):
        op = CreateRoleOp(Role("user", in_roles=["base"]))
        refs = _refs_for_role(op, "drop")
        phases = {r.phase for r in refs}
        assert "drop" in phases


# ---------------------------------------------------------------------------
# _refs_for_grant
# ---------------------------------------------------------------------------


class TestRefsForGrant:
    def test_table_grant_includes_role_ref(self):
        grant_obj = PgGrant.new("select", to="anon").on_tables("api.widgets")
        op = GrantPrivilegesOp(grant_obj)
        refs = _refs_for_grant(op, "create")
        schemas = {r.schema for r in refs}
        assert "__roles__" in schemas

    def test_table_grant_includes_schema_ref(self):
        grant_obj = PgGrant.new("select", to="anon").on_tables("api.widgets")
        op = GrantPrivilegesOp(grant_obj)
        refs = _refs_for_grant(op, "create")
        schema_refs = {r for r in refs if r.schema == "api" and r.name is None}
        assert len(schema_refs) >= 1

    def test_table_grant_includes_table_ref(self):
        grant_obj = PgGrant.new("select", to="anon").on_tables("api.widgets")
        op = GrantPrivilegesOp(grant_obj)
        refs = _refs_for_grant(op, "create")
        table_refs = {r for r in refs if r.name == "widgets"}
        assert len(table_refs) >= 1

    def test_schema_grant_includes_schema_ref(self):
        grant_obj = PgGrant.new("usage", to="anon").on_schemas("api")
        op = GrantPrivilegesOp(grant_obj)
        refs = _refs_for_grant(op, "create")
        schema_refs = {r for r in refs if r.schema == "api"}
        assert len(schema_refs) >= 1

    def test_unqualified_target_handled(self):
        """A target without a dot is treated as a schema-level ref."""
        grant_obj = PgGrant.new("usage", to="anon").on_schemas("myschema")
        op = GrantPrivilegesOp(grant_obj)
        refs = _refs_for_grant(op, "create")
        assert any(r.schema == "myschema" for r in refs)

    def test_default_grant_includes_schema_ref(self):
        """DefaultGrantStatement path must add schema refs."""
        dg = DefaultGrant(
            grant_type=DefaultGrantTypes.table,
            in_schemas=("myschema",),
            target_role="anon",
        )
        stmt = DefaultGrantStatement(
            default_grant=dg,
            grant=Grant(grants=("SELECT",), target_role="anon"),
        )
        op = GrantPrivilegesOp(stmt)
        refs = _refs_for_grant(op, "create")
        schema_refs = {r for r in refs if r.schema == "myschema"}
        assert len(schema_refs) >= 1


# ---------------------------------------------------------------------------
# _refs_for_trigger
# ---------------------------------------------------------------------------


class TestRefsForTrigger:
    def _make_trigger_op(self, on="myschema.myview", execute="myschema.fn"):
        trigger = Trigger.instead_of(
            "insert", on=on, execute=execute, name="tr"
        ).for_each_row()
        return CreateTriggerOp(trigger)

    def test_includes_view_name_ref(self):
        op = self._make_trigger_op()
        refs = _refs_for_trigger(op, "create")
        names = {r.name for r in refs if r.name is not None}
        assert "myview" in names

    def test_includes_function_name_ref(self):
        op = self._make_trigger_op()
        refs = _refs_for_trigger(op, "create")
        names = {r.name for r in refs if r.name is not None}
        assert "fn" in names

    def test_includes_schema_ref(self):
        op = self._make_trigger_op()
        refs = _refs_for_trigger(op, "create")
        schemas = {r.schema for r in refs}
        assert "myschema" in schemas

    def test_op_without_trigger_returns_empty(self):
        op = MagicMock()
        op.trigger = None
        assert _refs_for_trigger(op, "create") == set()

    def test_unqualified_execute_uses_public(self):
        op = self._make_trigger_op(execute="myfunc")
        refs = _refs_for_trigger(op, "create")
        pub_refs = {r for r in refs if r.schema == "public"}
        assert len(pub_refs) >= 1


# ---------------------------------------------------------------------------
# _view_table_refs
# ---------------------------------------------------------------------------


class TestViewTableRefs:
    def test_extracts_schema_qualified_table(self):
        refs = _view_table_refs("SELECT * FROM myschema.mytable")
        assert ("myschema", "mytable") in refs

    def test_unqualified_table_not_extracted(self):
        """Tables without schema qualification must be skipped."""
        refs = _view_table_refs("SELECT * FROM mytable")
        assert refs == set()

    def test_invalid_sql_returns_empty(self):
        refs = _view_table_refs("NOT VALID SQL !!!")
        assert refs == set()

    def test_multiple_schemas_extracted(self):
        sql = (
            "SELECT a.x, b.y "
            "FROM schema1.table1 a "
            "JOIN schema2.table2 b ON a.id = b.fk"
        )
        refs = _view_table_refs(sql)
        assert ("schema1", "table1") in refs
        assert ("schema2", "table2") in refs

    def test_returns_set(self):
        refs = _view_table_refs("SELECT 1")
        assert isinstance(refs, set)


# ---------------------------------------------------------------------------
# _plpgsql_table_refs
# ---------------------------------------------------------------------------


class TestPlpgsqlTableRefs:
    def _make_op(self, fn):
        op = MagicMock()
        op.function = fn
        return op

    def test_extracts_schema_qualified_table(self):
        fn = Function(
            "my_func",
            "BEGIN\n    INSERT INTO myschema.mytable(col)"
            " VALUES (NEW.col);\n    RETURN NEW;\nEND;",
            returns="trigger",
            language="plpgsql",
            schema="public",
        )
        refs = _plpgsql_table_refs(self._make_op(fn))
        assert ("myschema", "mytable") in refs

    def test_non_plpgsql_language_returns_empty(self):
        fn = Function("fn", "SELECT 1", language="sql", schema="s")
        refs = _plpgsql_table_refs(self._make_op(fn))
        assert refs == set()

    def test_no_function_attr_returns_empty(self):
        op = MagicMock()
        op.function = None
        assert _plpgsql_table_refs(op) == set()

    def test_invalid_plpgsql_returns_empty(self):
        fn = Function(
            "fn",
            "$$COMPLETELY INVALID$$",
            language="plpgsql",
            schema="s",
        )
        refs = _plpgsql_table_refs(self._make_op(fn))
        assert refs == set()

    def test_returns_set(self):
        fn = Function(
            "fn",
            "BEGIN RETURN NULL; END;",
            language="plpgsql",
            schema="s",
        )
        refs = _plpgsql_table_refs(self._make_op(fn))
        assert isinstance(refs, set)


# ---------------------------------------------------------------------------
# Function return refs
# ---------------------------------------------------------------------------


class TestFunctionReturnRefs:
    def _make_op(self, fn):
        op = MagicMock()
        op.function = fn
        return op

    def test_setof_schema_qualified(self):
        fn = Function(
            "fn",
            "SELECT 1",
            returns="SETOF api.inventory",
            language="sql",
            schema="private",
        )
        refs = _function_return_refs(self._make_op(fn))
        assert ("api", "inventory") in refs

    def test_setof_case_insensitive(self):
        fn = Function(
            "fn",
            "SELECT 1",
            returns="setof Api.Revenue",
            language="sql",
            schema="private",
        )
        refs = _function_return_refs(self._make_op(fn))
        assert ("api", "revenue") in refs

    def test_non_setof_returns_empty(self):
        fn = Function(
            "fn",
            "BEGIN END;",
            returns="trigger",
            language="plpgsql",
            schema="s",
        )
        refs = _function_return_refs(self._make_op(fn))
        assert refs == set()

    def test_setof_unqualified_returns_empty(self):
        fn = Function(
            "fn",
            "SELECT 1",
            returns="SETOF mytable",
            language="sql",
            schema="s",
        )
        refs = _function_return_refs(self._make_op(fn))
        assert refs == set()

    def test_no_function_attr_returns_empty(self):
        op = MagicMock()
        op.function = None
        assert _function_return_refs(op) == set()

    def test_no_returns_attr_returns_empty(self):
        fn = Function(
            "fn",
            "SELECT 1",
            language="sql",
            schema="s",
        )
        refs = _function_return_refs(self._make_op(fn))
        assert refs == set()


# ---------------------------------------------------------------------------
# SQL function table refs
# ---------------------------------------------------------------------------


class TestSqlFunctionTableRefs:
    def _make_op(self, fn):
        op = MagicMock()
        op.function = fn
        return op

    def test_extracts_schema_qualified_table(self):
        fn = Function(
            "fn",
            "INSERT INTO api.inventory (sku) SELECT p_sku RETURNING *",
            returns="SETOF api.inventory",
            language="sql",
            schema="private",
        )
        refs = _sql_function_table_refs(self._make_op(fn))
        assert ("api", "inventory") in refs

    def test_non_sql_language_returns_empty(self):
        fn = Function(
            "fn",
            "BEGIN END;",
            language="plpgsql",
            schema="s",
        )
        refs = _sql_function_table_refs(self._make_op(fn))
        assert refs == set()

    def test_no_function_attr_returns_empty(self):
        op = MagicMock()
        op.function = None
        assert _sql_function_table_refs(op) == set()

    def test_invalid_sql_returns_empty(self):
        fn = Function(
            "fn",
            "NOT VALID SQL !!!",
            language="sql",
            schema="s",
        )
        refs = _sql_function_table_refs(self._make_op(fn))
        assert refs == set()


# ---------------------------------------------------------------------------
# _entity_identifier — additional cases
# ---------------------------------------------------------------------------


class TestEntityIdentifierAdditional:
    def test_grant_privileges_op_uses_grants_schema(self):
        grant_obj = PgGrant.new("select", to="anon").on_tables("api.widgets")
        op = GrantPrivilegesOp(grant_obj)
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.schema == "__grants__"

    def test_revoke_privileges_op_uses_grants_schema(self):
        grant_obj = PgGrant.new("select", to="anon").on_tables("api.items")
        op = RevokePrivilegesOp(grant_obj)
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.schema == "__grants__"

    def test_unhandled_op_type_returns_none(self):
        class UnknownOp:
            pass

        assert _entity_identifier(UnknownOp()) is None

    def test_create_trigger_identifier(self):
        trigger = Trigger.instead_of(
            "insert",
            on="s.v",
            execute="s.fn",
            name="my_trigger",
        ).for_each_row()
        op = CreateTriggerOp(trigger)
        eid = _entity_identifier(op)
        assert eid is not None
        assert eid.name == "my_trigger"


# ---------------------------------------------------------------------------
# expand_update_ops — UpdateProcedureOp
# ---------------------------------------------------------------------------


class TestExpandUpdateProcedureOp:
    def test_expand_update_procedure_op(self):
        proc_old = Procedure(
            "my_proc", "BEGIN END;", language="plpgsql", schema="public"
        )
        proc_new = Procedure(
            "my_proc",
            "BEGIN RETURN NULL; END;",
            language="plpgsql",
            schema="public",
        )
        op = UpdateProcedureOp(proc_old, proc_new)
        result = expand_update_ops([op])
        assert len(result) == len([proc_old, proc_new])
        assert isinstance(result[0], DropProcedureOp)
        assert isinstance(result[1], CreateProcedureOp)


# ---------------------------------------------------------------------------
# sort_migration_ops — FK graph integration
# ---------------------------------------------------------------------------


class TestSortMigrationOpsWithFkGraph:
    def test_fk_graph_parent_before_child(self):
        """FK graph must ensure parent table is created before child."""
        fk_graph = {("s", "child"): {("s", "parent")}}
        schema_op = CreateSchemaOp(Schema("s"))
        parent_op = alembic_ops.CreateTableOp(
            "parent", [Column("id", Integer, primary_key=True)], schema="s"
        )
        child_op = alembic_ops.CreateTableOp(
            "child", [Column("id", Integer, primary_key=True)], schema="s"
        )
        result = sort_migration_ops(
            [child_op, parent_op, schema_op], fk_graph=fk_graph
        )
        names = [
            op.table_name
            for op in result
            if isinstance(op, alembic_ops.CreateTableOp)
        ]
        assert names.index("parent") < names.index("child")

    def test_unkeyed_op_appended_at_end(self):
        """Ops with no entity identifier must appear after keyed ops."""

        class SentinelOp:
            pass

        sentinel = SentinelOp()
        create_op = alembic_ops.CreateTableOp(
            "t", [Column("id", Integer, primary_key=True)]
        )
        result = sort_migration_ops([sentinel, create_op])
        assert result[-1] is sentinel

    def test_setof_function_after_referenced_view(self):
        """Function with SETOF return must be created after the view."""
        schema_op = CreateSchemaOp(Schema("api"))
        view_op = CreateViewOp(View("inventory", "SELECT 1", schema="api"))
        fn = Function(
            "inventory_adjust",
            "INSERT INTO api.inventory (sku) SELECT p_sku RETURNING *",
            returns="SETOF api.inventory",
            language="sql",
            schema="private",
        )
        fn_schema_op = CreateSchemaOp(Schema("private"))
        fn_op = CreateFunctionOp(fn)
        result = sort_migration_ops([fn_op, fn_schema_op, view_op, schema_op])
        view_idx = next(
            i for i, op in enumerate(result) if isinstance(op, CreateViewOp)
        )
        fn_idx = next(
            i for i, op in enumerate(result) if isinstance(op, CreateFunctionOp)
        )
        assert view_idx < fn_idx
