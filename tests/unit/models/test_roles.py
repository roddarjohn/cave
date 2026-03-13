"""Unit tests for pgcraft.models.roles."""

from sqlalchemy import MetaData

from pgcraft.models.roles import anon, authenticator, register_roles
from pgcraft.resource import APIResource, register_api_resource


class TestModuleLevelRoles:
    def test_authenticator_exists(self):
        assert authenticator is not None
        assert authenticator.name == "authenticator"

    def test_anon_exists(self):
        assert anon is not None
        assert anon.name == "anon"

    def test_authenticator_has_login(self):
        assert authenticator.login is True

    def test_authenticator_in_anon_role(self):
        assert "anon" in authenticator.in_roles

    def test_anon_no_login(self):
        assert not anon.login


class TestRegisterRolesNoResources:
    def test_roles_registered(self):
        """Both authenticator and anon must appear after the call."""
        metadata = MetaData()
        register_roles(metadata)
        roles = metadata.info["roles"]
        names = {r.name for r in roles.roles}
        assert "authenticator" in names
        assert "anon" in names

    def test_no_usage_grant_when_no_resources(self):
        """With no API resources there should be no SCHEMA USAGE grant."""
        metadata = MetaData()
        register_roles(metadata)
        grants = metadata.info["grants"]
        schema_grants = [g for g in grants.grants if "USAGE" in str(g).upper()]
        assert schema_grants == []

    def test_grants_registered_even_without_resources(self):
        metadata = MetaData()
        register_roles(metadata)
        assert "grants" in metadata.info

    def test_roles_ignore_unspecified_true(self):
        metadata = MetaData()
        register_roles(metadata)
        assert metadata.info["roles"].ignore_unspecified is True

    def test_grants_ignore_unspecified_true(self):
        metadata = MetaData()
        register_roles(metadata)
        assert metadata.info["grants"].ignore_unspecified is True


class TestRegisterRolesWithResources:
    def test_schema_usage_grant_added(self):
        """A USAGE grant on the resource schema should be registered."""
        metadata = MetaData()
        register_api_resource(
            metadata,
            APIResource("products", schema="api", grants=["select"]),
        )
        register_roles(metadata)
        grants = metadata.info["grants"]
        # There should be at least one USAGE grant on 'api'
        usage_grants = [
            g
            for g in grants.grants
            if "api" in str(g).lower() and "USAGE" in str(g).upper()
        ]
        assert len(usage_grants) >= 1

    def test_table_grant_added(self):
        """A SELECT grant on the table should be registered."""
        metadata = MetaData()
        register_api_resource(
            metadata,
            APIResource("products", schema="api", grants=["select"]),
        )
        register_roles(metadata)
        grants = metadata.info["grants"]
        table_grants = [
            g for g in grants.grants if "products" in str(g).lower()
        ]
        assert len(table_grants) >= 1

    def test_multiple_resources_distinct_schema_grants(self):
        """Multiple resources in different schemas each get a USAGE grant."""
        metadata = MetaData()
        register_api_resource(
            metadata, APIResource("a", schema="s1", grants=["select"])
        )
        register_api_resource(
            metadata, APIResource("b", schema="s2", grants=["select"])
        )
        register_roles(metadata)
        grants_text = str(metadata.info["grants"])
        assert "s1" in grants_text
        assert "s2" in grants_text

    def test_multiple_resources_same_schema_single_usage_grant(self):
        """Multiple resources in the same schema share one USAGE grant."""
        metadata = MetaData()
        register_api_resource(
            metadata, APIResource("a", schema="api", grants=["select"])
        )
        register_api_resource(
            metadata, APIResource("b", schema="api", grants=["select"])
        )
        register_roles(metadata)
        grants = metadata.info["grants"]
        usage_grants = [
            g
            for g in grants.grants
            if "USAGE" in str(g).upper() and "api" in str(g).lower()
        ]
        # Exactly one USAGE grant for 'api' (not one per resource)
        assert len(usage_grants) == 1

    def test_roles_still_registered_with_resources(self):
        metadata = MetaData()
        register_api_resource(
            metadata, APIResource("x", schema="api", grants=["select"])
        )
        register_roles(metadata)
        names = {r.name for r in metadata.info["roles"].roles}
        assert "authenticator" in names
        assert "anon" in names

    def test_insert_grant_included(self):
        """When grant includes 'insert', the resulting grant should list it."""
        metadata = MetaData()
        register_api_resource(
            metadata,
            APIResource("orders", schema="api", grants=["select", "insert"]),
        )
        register_roles(metadata)
        grants_text = str(metadata.info["grants"])
        assert "INSERT" in grants_text.upper() or "insert" in grants_text
