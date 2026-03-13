Plugin architecture
===================

pgcraft's resource factories are built entirely on a plugin system.  A factory
is a thin runner that topologically sorts a list of plugins by their declared
dependencies and calls each one in turn.  All behaviour — primary keys, table
layout, views, triggers, API exposure — is provided by plugins, so every part
of it can be replaced, extended, or recomposed without touching the core.

Core concepts
-------------

Plugin
~~~~~~

A plugin is any instance of a class that inherits from
:class:`~pgcraft.plugin.Plugin`.  The :meth:`~pgcraft.plugin.Plugin.run` method is
a no-op by default, so a plugin only needs to implement the hooks it cares
about.  Plugins communicate by writing to and reading from a shared
:class:`~pgcraft.factory.context.FactoryContext`.

FactoryContext
~~~~~~~~~~~~~~

:class:`~pgcraft.factory.context.FactoryContext` carries the inputs supplied to
the factory (table name, schema, metadata, schema items) together with a flat
key/value store that plugins use to pass objects between themselves.

.. code-block:: python

   # Writing a value
   ctx["my_table"] = table

   # Reading a value
   table = ctx["my_table"]

   # Checking existence (safe; never raises)
   if "my_table" in ctx:
       ...

   # Intentional override (force=True required to prevent accidents)
   ctx.set("my_table", replacement, force=True)

Writing to a key that already exists raises :class:`KeyError` immediately.
Reading a key that has not yet been written raises :class:`KeyError` with a
hint naming the missing key.

ResourceFactory
~~~~~~~~~~~~~~~

:class:`~pgcraft.factory.base.ResourceFactory` is the plugin runner.  It takes
a list of plugins, validates it for singleton conflicts, topologically sorts
the plugins by their :func:`~pgcraft.plugin.produces` /
:func:`~pgcraft.plugin.requires` declarations, and calls
:meth:`~pgcraft.plugin.Plugin.run` on each in the resolved order.

Subclasses declare ``DEFAULT_PLUGINS`` to establish their standard behaviour.
The three built-in dimension factories are thin wrappers:

.. code-block:: python

   class SimpleDimensionResourceFactory(ResourceFactory):
       DEFAULT_PLUGINS = [
           SerialPKPlugin(),
           SimpleTablePlugin(),
           APIPlugin(),
           SimpleTriggerPlugin(),
       ]


Plugin execution order
----------------------

Execution order is determined by sorting plugins topologically using two class
decorators: :func:`~pgcraft.plugin.produces` and :func:`~pgcraft.plugin.requires`.

:func:`~pgcraft.plugin.produces`
    Declare the ``ctx`` keys this plugin's :meth:`~pgcraft.plugin.Plugin.run`
    method will write.

:func:`~pgcraft.plugin.requires`
    Declare the ``ctx`` keys this plugin's :meth:`~pgcraft.plugin.Plugin.run`
    method needs to already be set before it runs.

The factory builds a dependency graph from these declarations and calls
:meth:`~pgcraft.plugin.Plugin.run` in a valid topological order.  Plugins with
no relationship to each other preserve their original list order.

.. code-block:: python

   from pgcraft.plugin import Dynamic, Plugin, produces, requires

   @produces(Dynamic("out_key"))
   @requires("primary")
   class MyTransformPlugin(Plugin):
       """Derive a summary table from the primary table."""

       def __init__(self, out_key: str = "summary") -> None:
           self.out_key = out_key

       def run(self, ctx: FactoryContext) -> None:
           primary = ctx["primary"]
           # ... build summary from primary ...
           ctx[self.out_key] = summary

``MyTransformPlugin`` will always run after the plugin that produces
``"primary"``, regardless of the order they appear in the plugin list.

Injected columns
~~~~~~~~~~~~~~~~~

Some plugins need to contribute columns to a table without knowing
which table plugin will consume them.  For this, plugins append
:class:`~sqlalchemy.Column` objects to ``ctx.injected_columns`` — a
shared list on :class:`~pgcraft.factory.context.FactoryContext`.  Table
plugins that support this pattern (currently
:class:`~pgcraft.plugins.ledger.LedgerTablePlugin`) spread the list
into the table definition:

.. code-block:: python

   # Column-providing plugin
   class MyColumnPlugin(Plugin):
       def run(self, ctx: FactoryContext) -> None:
           ctx.injected_columns.append(
               Column("tenant_id", Integer, nullable=False)
           )

   # Table plugin spreads injected columns
   table = Table(
       ctx.tablename, ctx.metadata,
       *pk_columns,
       *ctx.injected_columns,   # entry_id, created_at, etc.
       Column("value", Integer(), nullable=False),
       *ctx.table_items,
       schema=ctx.schemaname,
   )

Built-in plugins that inject columns:

- :class:`~pgcraft.plugins.created_at.CreatedAtPlugin` — injects a
  ``created_at`` timestamp column.
- :class:`~pgcraft.plugins.entry_id.UUIDEntryIDPlugin` — injects an
  ``entry_id`` UUID column.
- :class:`~pgcraft.plugins.ledger.DoubleEntryPlugin` — injects a
  ``direction`` column for debit/credit semantics.

These plugins also write their standard ctx store keys (e.g.
``"created_at_column"``, ``"entry_id_column"``, ``"double_entry_columns"``)
so that downstream trigger plugins can look up column metadata.


Before any :meth:`~pgcraft.plugin.Plugin.run` call the factory collects two
special inputs:

``pk_columns``
    The first non-``None`` result across all plugins is stored in
    ``ctx.pk_columns``.

``extra_columns``
    Results from all plugins are concatenated and stored in
    ``ctx.extra_columns``.

Both are available to every plugin's :meth:`~pgcraft.plugin.Plugin.run` via the
typed fields ``ctx.pk_columns`` and ``ctx.extra_columns``.

Dynamic key references
~~~~~~~~~~~~~~~~~~~~~~

When a ctx key name is a constructor parameter rather than a fixed string, use
:class:`~pgcraft.plugin.Dynamic` inside the decorator:

.. code-block:: python

   @produces(Dynamic("table_key"))
   class SimpleTablePlugin(Plugin):
       def __init__(self, table_key: str = "primary") -> None:
           self.table_key = table_key

       def run(self, ctx: FactoryContext) -> None:
           ctx[self.table_key] = Table(...)

The decorator validates at class definition time that the ``Dynamic`` attribute
name is a real ``__init__`` parameter, so typos are caught immediately.


Plugin resolution
-----------------

Each factory call resolves its plugin list from three sources, concatenated
in this order:

1. **Global plugins** — from :class:`~pgcraft.config.PGCraftConfig`, if passed via
   the ``config=`` argument.  Prepended before everything else.
2. **Factory plugins** — ``plugins`` kwarg if supplied, otherwise
   ``DEFAULT_PLUGINS``.
3. **Extra plugins** — always appended via ``extra_plugins``.

.. code-block:: python

   cave_cfg = PGCraftConfig()
   cave_cfg.register(AuditPlugin())         # prepended to every factory

   SimpleDimensionResourceFactory(
       "users", "app", metadata, schema_items,
       config=pgcraft_cfg,                        # global plugins first
       extra_plugins=[TenantPlugin()],       # appended after defaults
   )

To replace the default plugin list entirely:

.. code-block:: python

   SimpleDimensionResourceFactory(
       "events", "app", metadata, schema_items,
       plugins=[                             # replaces DEFAULT_PLUGINS
           SerialPKPlugin(column_name="event_id"),
           SimpleTablePlugin(),
           APIPlugin(schema="reporting"),
           SimpleTriggerPlugin(),
       ],
   )


Singleton groups
----------------

Some plugins must appear at most once in a resolved list (e.g. you cannot
have two PK plugins).  The :func:`~pgcraft.plugin.singleton` decorator
declares a *group name*; the factory raises
:class:`~pgcraft.errors.PGCraftValidationError` at construction time if two
plugins share the same group.

.. code-block:: python

   from pgcraft.plugin import Plugin, singleton

   @singleton("__pk__")
   class MyPKPlugin(Plugin):
       ...

The built-in groups are ``"__pk__"`` (one PK plugin), ``"__table__"``
(one table-layout plugin), ``"__entry_id__"`` (one entry ID plugin),
and ``"__double_entry__"`` (one double-entry plugin).  You can define
your own group names for custom plugins.


Context keys
------------

Plugins read and write objects in ``ctx`` using string keys.  Every built-in
plugin accepts its key names as constructor arguments with sensible defaults,
so two independent pipelines can coexist in one factory without colliding.

``SerialPKPlugin``
    Sets ``ctx.pk_columns`` (typed field, not a store key).

``SimpleTablePlugin``
    Writes ``"primary"`` (the backing table).

``APIPlugin``
    Reads ``"primary"`` (via ``table_key``).  Writes ``"api"`` (via
    ``view_key``).

``SimpleTriggerPlugin``
    Reads ``"primary"`` (via ``table_key``) and ``"api"`` (via ``view_key``).

``AppendOnlyTablePlugin``
    Writes ``"root_table"`` and ``"attributes"``.

``AppendOnlyViewPlugin``
    Reads ``"root_table"`` and ``"attributes"``.  Writes ``"primary"``.

``AppendOnlyTriggerPlugin``
    Reads ``"root_table"``, ``"attributes"``, and ``"api"`` (optional;
    skipped if absent from ``ctx``).

``EAVTablePlugin``
    Writes ``"entity"``, ``"attribute"``, and ``"eav_mappings"``.

``EAVViewPlugin``
    Reads ``"entity"``, ``"attribute"``, and ``"eav_mappings"``.  Writes
    ``"primary"``.

``EAVTriggerPlugin``
    Reads ``"entity"``, ``"attribute"``, ``"eav_mappings"``, and ``"api"``
    (optional; skipped if absent from ``ctx``).

``UUIDEntryIDPlugin``
    Writes ``"entry_id_column"`` (a ``Column`` object).  Also appends
    the column to ``ctx.injected_columns``.

``CreatedAtPlugin``
    Writes ``"created_at_column"`` (the column name string).  Also
    appends a ``DateTime`` column to ``ctx.injected_columns``.

``LedgerTablePlugin``
    Reads ``"pk_columns"`` and ``ctx.injected_columns``.  Requires
    ``"entry_id_column"`` and ``"created_at_column"`` for ordering.
    Writes ``"primary"`` (the table) and ``"__root__"``.

``LedgerTriggerPlugin``
    Reads ``"primary"`` (via ``table_key``), ``"api"`` (via
    ``view_key``), and ``"entry_id_column"``.

``LedgerLatestViewPlugin``
    Reads ``"primary"`` (via ``table_key``) and
    ``"created_at_column"``.  Writes ``"latest_view"``
    (via ``latest_view_key``).

``LedgerBalanceViewPlugin``
    Reads ``"primary"`` (via ``table_key``).  Writes
    ``"balance_view"`` (via ``balance_view_key``).

``LedgerBalanceCheckPlugin``
    Reads ``"primary"`` (via ``table_key``).  Registers an
    AFTER INSERT trigger enforcing ``SUM(value) >= min_balance``
    per dimension group.

``DoubleEntryPlugin``
    Writes ``"double_entry_columns"`` (the direction column name).
    Appends a ``direction`` column to ``ctx.injected_columns``.

``DoubleEntryTriggerPlugin``
    Reads ``"primary"`` (via ``table_key``),
    ``"double_entry_columns"``, and ``"entry_id_column"``.

All key names are overridable via constructor arguments, which means you can
wire plugins together in non-standard ways or run multiple pipelines within
a single factory.


Writing a custom plugin
-----------------------

Implement :meth:`~pgcraft.plugin.Plugin.run` and declare your dependencies with
:func:`~pgcraft.plugin.produces` and :func:`~pgcraft.plugin.requires`.  Use
``ctx`` to pass objects to downstream plugins.

A simple plugin that only contributes extra columns needs no dependency
declarations at all — it implements :meth:`~pgcraft.plugin.Plugin.extra_columns`
instead of :meth:`~pgcraft.plugin.Plugin.run`:

.. code-block:: python

   from __future__ import annotations

   from sqlalchemy import Column, DateTime, func
   from pgcraft.plugin import Plugin
   from pgcraft.factory.context import FactoryContext


   class TimestampPlugin(Plugin):
       """Add ``created_at`` / ``updated_at`` columns to every table."""

       def extra_columns(self, _ctx: FactoryContext) -> list[Column]:
           return [
               Column("created_at", DateTime(timezone=True),
                      server_default=func.now()),
               Column("updated_at", DateTime(timezone=True),
                      server_default=func.now(), onupdate=func.now()),
           ]

Register it globally so it applies to every factory in the project:

.. code-block:: python

   from pgcraft.config import PGCraftConfig
   from pgcraft.factory.dimension.simple import SimpleDimensionResourceFactory
   from pgcraft.factory.dimension.append_only import AppendOnlyDimensionResourceFactory

   cave_cfg = PGCraftConfig()
   cave_cfg.register(TimestampPlugin())

   SimpleDimensionResourceFactory(
       "products", "app", metadata, schema_items, config=pgcraft_cfg
   )
   AppendOnlyDimensionResourceFactory(
       "orders", "app", metadata, schema_items, config=pgcraft_cfg
   )

Or apply it to a single factory only:

.. code-block:: python

   SimpleDimensionResourceFactory(
       "products", "app", metadata, schema_items,
       extra_plugins=[TimestampPlugin()],
   )

Custom plugin with ctx communication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is a more involved example: a plugin that creates a shadow audit table
and makes it available to a downstream trigger plugin via a ctx key.

.. code-block:: python

   from sqlalchemy import Column, DateTime, ForeignKey, Integer, Table
   from pgcraft.plugin import Dynamic, Plugin, produces, requires, singleton
   from pgcraft.factory.context import FactoryContext


   @produces(Dynamic("shadow_key"))
   @singleton("__shadow__")
   class ShadowTablePlugin(Plugin):
       """Create a shadow audit table alongside the main table."""

       def __init__(self, shadow_key: str = "shadow") -> None:
           self.shadow_key = shadow_key

       def run(self, ctx: FactoryContext) -> None:
           shadow = Table(
               f"{ctx.tablename}_shadow",
               ctx.metadata,
               Column("id", Integer, primary_key=True),
               Column("ref_id", Integer,
                      ForeignKey(f"{ctx.schemaname}.{ctx.tablename}.id")),
               Column("changed_at", DateTime(timezone=True)),
               schema=ctx.schemaname,
           )
           ctx[self.shadow_key] = shadow


   @requires(Dynamic("shadow_key"))
   class ShadowTriggerPlugin(Plugin):
       """Register a trigger that writes to the shadow table on every change."""

       def __init__(self, shadow_key: str = "shadow") -> None:
           self.shadow_key = shadow_key

       def run(self, ctx: FactoryContext) -> None:
           shadow = ctx[self.shadow_key]   # guaranteed by @requires
           # ... register trigger using shadow.name, etc.


   # Use them together — order in the list doesn't matter because the
   # dependency declarations ensure ShadowTablePlugin runs first.
   SimpleDimensionResourceFactory(
       "products", "app", metadata, schema_items,
       extra_plugins=[ShadowTriggerPlugin(), ShadowTablePlugin()],
   )


Global configuration
--------------------

:class:`~pgcraft.config.PGCraftConfig` holds the global plugin list that is
prepended to every factory that references it.

.. code-block:: python

   from pgcraft.config import PGCraftConfig

   cave_cfg = PGCraftConfig()
   cave_cfg.register(TimestampPlugin(), TenantPlugin())

   # -- or equivalently --
   cave_cfg = PGCraftConfig(plugins=[TimestampPlugin(), TenantPlugin()])

Pass it to each factory via the ``config=`` argument.  A common pattern is to
create one ``PGCraftConfig`` per project and import it wherever factories are
defined:

.. code-block:: python

   # cave_setup.py
   from pgcraft.config import PGCraftConfig
   from myapp.plugins import TimestampPlugin, TenantPlugin

   cave_cfg = PGCraftConfig()
   cave_cfg.register(TimestampPlugin(), TenantPlugin())

   # models.py
   from myapp.cave_setup import cave_cfg
   from pgcraft.factory.dimension.simple import SimpleDimensionResourceFactory

   SimpleDimensionResourceFactory(
       "users", "app", metadata, schema_items, config=pgcraft_cfg
   )


Built-in plugins reference
--------------------------

See the :doc:`api` reference for full autodoc on all built-in plugins.
