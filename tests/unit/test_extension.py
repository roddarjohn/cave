"""Unit tests for pgcraft extension infrastructure."""

from dataclasses import dataclass
from typing import ClassVar
from unittest.mock import patch

import pytest
from sqlalchemy import MetaData

from pgcraft.config import PGCraftConfig
from pgcraft.errors import PGCraftValidationError
from pgcraft.extension import (
    PGCraftExtension,
    discover_extensions,
    validate_extension_deps,
)
from pgcraft.plugin import Plugin

# -- Stub extensions ------------------------------------------------


@dataclass
class _NoopExtension(PGCraftExtension):
    name: str = "noop"


@dataclass
class _PluginExtension(PGCraftExtension):
    name: str = "with-plugins"

    def plugins(self) -> list[Plugin]:
        return [Plugin()]


@dataclass
class _MetadataExtension(PGCraftExtension):
    name: str = "meta"

    def configure_metadata(self, metadata: MetaData) -> None:
        metadata.info["meta_ext_called"] = True


@dataclass
class _DependentExtension(PGCraftExtension):
    name: str = "dependent"
    depends_on: ClassVar[list[str]] = ["noop"]


@dataclass
class _MissingDepExtension(PGCraftExtension):
    name: str = "orphan"
    depends_on: ClassVar[list[str]] = ["nonexistent"]


@dataclass
class _ValidatingExtension(PGCraftExtension):
    name: str = "validating"

    def validate(self, registered_names: frozenset[str]) -> None:
        if "required-peer" not in registered_names:
            msg = "required-peer not found"
            raise PGCraftValidationError(msg)


# -- Extension base -------------------------------------------------


class TestExtensionBase:
    def test_default_plugins_empty(self):
        ext = _NoopExtension()
        assert ext.plugins() == []

    def test_default_configure_metadata_is_noop(self):
        ext = _NoopExtension()
        metadata = MetaData()
        ext.configure_metadata(metadata)
        assert "meta_ext_called" not in metadata.info

    def test_default_configure_alembic_is_noop(self):
        ext = _NoopExtension()
        ext.configure_alembic()

    def test_default_register_cli_is_noop(self):
        ext = _NoopExtension()
        ext.register_cli(object())

    def test_default_validate_is_noop(self):
        ext = _NoopExtension()
        ext.validate(frozenset({"noop"}))

    def test_plugins_override(self):
        ext = _PluginExtension()
        assert len(ext.plugins()) == 1
        assert isinstance(ext.plugins()[0], Plugin)

    def test_configure_metadata_override(self):
        ext = _MetadataExtension()
        metadata = MetaData()
        ext.configure_metadata(metadata)
        assert metadata.info["meta_ext_called"] is True


# -- Dependency validation -------------------------------------------


class TestExtensionDeps:
    def test_satisfied_deps_pass(self):
        validate_extension_deps([_NoopExtension(), _DependentExtension()])

    def test_missing_dep_raises(self):
        with pytest.raises(PGCraftValidationError, match="nonexistent"):
            validate_extension_deps([_MissingDepExtension()])

    def test_validate_hook_called(self):
        with pytest.raises(PGCraftValidationError, match="required-peer"):
            validate_extension_deps([_ValidatingExtension()])

    def test_validate_hook_passes_when_peer_present(self):
        @dataclass
        class _Peer(PGCraftExtension):
            name: str = "required-peer"

        validate_extension_deps([_ValidatingExtension(), _Peer()])


# -- Discovery -------------------------------------------------------


class TestDiscoverExtensions:
    def test_returns_dict(self):
        result = discover_extensions()
        assert isinstance(result, dict)

    def test_mock_entry_point(self):
        class _FakeEP:
            name = "fake"

            def load(self):
                return _NoopExtension

        with patch(
            "pgcraft.extension.entry_points",
            return_value=[_FakeEP()],
        ):
            result = discover_extensions()
            assert "fake" in result
            assert result["fake"] is _NoopExtension


# -- PGCraftConfig integration --------------------------------------


class TestConfigExtensions:
    def test_use_registers_extension(self):
        config = PGCraftConfig(auto_discover=False)
        ext = _NoopExtension()
        result = config.use(ext)
        assert result is config
        assert ext in config.extensions

    def test_use_chaining(self):
        config = PGCraftConfig(auto_discover=False)
        config.use(_NoopExtension()).use(_PluginExtension())
        assert len(config.extensions) == 2

    def test_resolved_extensions_returns_manual(self):
        config = PGCraftConfig(auto_discover=False)
        ext = _NoopExtension()
        config.use(ext)
        resolved = config._resolved_extensions()
        assert len(resolved) == 1
        assert resolved[0] is ext

    def test_resolved_extensions_dedupes_by_name(self):
        config = PGCraftConfig(auto_discover=False)
        config.use(_NoopExtension())
        config.use(_NoopExtension())
        resolved = config._resolved_extensions()
        assert len(resolved) == 1

    def test_manual_takes_precedence_over_discovered(self):
        manual = _NoopExtension()

        class _FakeEP:
            name = "noop"

            def load(self):
                return _NoopExtension

        config = PGCraftConfig(auto_discover=True)
        config.use(manual)
        with patch(
            "pgcraft.extension.entry_points",
            return_value=[_FakeEP()],
        ):
            resolved = config._resolved_extensions()
        assert len(resolved) == 1
        assert resolved[0] is manual

    def test_all_plugins_includes_extension_plugins(self):
        config = PGCraftConfig(auto_discover=False)
        config.use(_PluginExtension())
        all_plugins = config.all_plugins
        assert len(all_plugins) == 1
        assert isinstance(all_plugins[0], Plugin)

    def test_all_plugins_extension_before_direct(self):
        config = PGCraftConfig(auto_discover=False)
        config.use(_PluginExtension())
        direct = Plugin()
        config.register(direct)
        all_plugins = config.all_plugins
        assert len(all_plugins) == 2
        assert all_plugins[1] is direct

    def test_resolved_extensions_validates_deps(self):
        config = PGCraftConfig(auto_discover=False)
        config.use(_MissingDepExtension())
        with pytest.raises(PGCraftValidationError, match="nonexistent"):
            config._resolved_extensions()
