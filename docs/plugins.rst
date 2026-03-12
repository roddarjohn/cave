Plugin architecture
===================

cave's dimension factories are built entirely on a plugin system.  A factory
is a thin runner that calls a fixed sequence of hooks on each plugin in a
list.  All behaviour — primary keys, table layout, views, triggers, API
exposure — is provided by plugins, so every part of it can be replaced,
extended, or recomposed without touching the core.

Core concepts
-------------

Plugin
~~~~~~

A plugin is any instance of a class that inherits from
:class:`~cave.plugin.Plugin`.  Every hook has a sensible no-op default, so a
plugin only needs to implement the hooks it cares about.  Plugins communicate
by writing to and reading from a shared :class:`~cave.factory.context.FactoryContext`.

FactoryContext
~~~~~~~~~~~~~~

:class:`~cave.factory.context.FactoryContext` carries the inputs supplied to the
factory (table name, schema, metadata, dimensions) together with a flat
key/value store that plugins use to pass objects between phases.

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

Writing to a key that already exists raises :class:`KeyError` immediately —
two plugins producing the same output key is almost always a mistake.
Reading a key that has not yet been written raises :class:`KeyError` with a
hint that the plugin responsible for writing that key must appear *earlier*
in the plugin list.

DimensionFactory
~~~~~~~~~~~~~~~~

:class:`~cave.factory.base.DimensionFactory` is the plugin runner.  It takes
a list of plugins, validates it for singleton conflicts, builds the context,
and executes each lifecycle phase across all plugins in order.

Subclasses declare ``DEFAULT_PLUGINS`` to establish their standard behaviour.  All three built-in factory types are
thin wrappers:

.. code-block:: python

   class SimpleDimensionFactory(DimensionFactory):
       DEFAULT_PLUGINS = [
           SerialPKPlugin(),
           SimpleTablePlugin(),
           APIPlugin(),
           SimpleTriggerPlugin(),
       ]


Lifecycle phases
----------------

All plugins are called in list order within each phase.  Phases run
sequentially, so every plugin finishes phase N before any plugin starts
phase N+1.  This is what makes inter-plugin dependencies safe: a trigger
plugin can always read a view that a view plugin created, regardless of
their relative order in the list, because *all* view creation finishes
before *any* trigger creation begins.

``pk_columns``
    Return the primary key column(s).  The first non-``None`` result across
    all plugins is used; remaining plugins are skipped for this phase.

``extra_columns``
    Return additional columns to prepend to the user-supplied dimension list.
    Results from all plugins are concatenated.

``create_tables``
    Create backing tables and store them in ``ctx``.

``create_views``
    Create views on top of the tables.  Read tables from ``ctx``, write
    views back.

``create_triggers``
    Register INSTEAD OF trigger functions and triggers on the views.

``post_create``
    Final hook.  Used for API resource registration, custom metadata,
    anything that depends on all earlier objects existing.

Why separate phases?
~~~~~~~~~~~~~~~~~~~~

PostgreSQL requires objects to be created in dependency order: tables before
views, views before triggers.  Without phases, a plugin would have to create
its table *and* its view in one step — but another plugin's trigger might
depend on that view, and it might not have been created yet.  The phase model
guarantees the global ordering regardless of which plugins appear together.


Plugin resolution
-----------------

Each factory call resolves its plugin list from three sources, concatenated
in this order:

1. **Global plugins** — from :class:`~cave.config.CaveConfig`, if passed via
   the ``cave=`` argument.  Run before everything else.
2. **Factory plugins** — ``plugins`` kwarg if supplied, otherwise
   ``DEFAULT_PLUGINS``.
3. **Extra plugins** — always appended via ``extra_plugins``.

.. code-block:: python

   cave_cfg = CaveConfig()
   cave_cfg.register(AuditPlugin())         # prepended to every factory

   SimpleDimensionFactory(
       "users", "app", metadata, dimensions,
       cave=cave_cfg,                        # global plugins first
       extra_plugins=[TenantPlugin()],       # appended after defaults
   )

To replace the default plugin list entirely:

.. code-block:: python

   SimpleDimensionFactory(
       "events", "app", metadata, dimensions,
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
have two PK plugins).  The :func:`~cave.plugin.singleton` decorator
declares a *group name*; the factory raises
:class:`~cave.errors.CaveValidationError` at construction time if two
plugins share the same group.

.. code-block:: python

   from cave.plugin import Plugin, singleton

   @singleton("__pk__")
   class MyPKPlugin(Plugin):
       ...

The built-in groups are ``"__pk__"`` (one PK plugin) and ``"__table__"``
(one table-layout plugin).  You can define your own group names for custom
plugins.


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
    skipped if absent).

``EAVTablePlugin``
    Writes ``"entity"``, ``"attribute"``, and ``"eav_mappings"``.

``EAVViewPlugin``
    Reads ``"entity"``, ``"attribute"``, and ``"eav_mappings"``.  Writes
    ``"primary"``.

``EAVTriggerPlugin``
    Reads ``"entity"``, ``"attribute"``, ``"eav_mappings"``, and ``"api"``
    (optional; skipped if absent).

All key names are overridable via constructor arguments, which means you can
wire plugins together in non-standard ways or run multiple pipelines within
a single factory.


Writing a custom plugin
-----------------------

Implement any subset of the six lifecycle hooks.  Use ``ctx`` to pass objects
to downstream plugins.

.. code-block:: python

   from __future__ import annotations

   from sqlalchemy import Column, DateTime, func
   from cave.plugin import Plugin
   from cave.factory.context import FactoryContext


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

   from cave.config import CaveConfig
   from cave.factory.dimension.simple import SimpleDimensionFactory
   from cave.factory.dimension.append_only import AppendOnlyDimensionFactory

   cave_cfg = CaveConfig()
   cave_cfg.register(TimestampPlugin())

   SimpleDimensionFactory(
       "products", "app", metadata, dimensions, cave=cave_cfg
   )
   AppendOnlyDimensionFactory(
       "orders", "app", metadata, dimensions, cave=cave_cfg
   )

Or apply it to a single factory only:

.. code-block:: python

   SimpleDimensionFactory(
       "products", "app", metadata, dimensions,
       extra_plugins=[TimestampPlugin()],
   )

Custom table plugin with ctx communication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is a more involved example: a plugin that creates a shadow audit table
and makes it available to a downstream trigger plugin via a ctx key.

.. code-block:: python

   from sqlalchemy import Column, DateTime, ForeignKey, Integer, Table
   from cave.plugin import Plugin, singleton
   from cave.factory.context import FactoryContext


   @singleton("__shadow__")
   class ShadowTablePlugin(Plugin):
       """Create a shadow audit table alongside the main table."""

       def __init__(self, shadow_key: str = "shadow") -> None:
           self.shadow_key = shadow_key

       def create_tables(self, ctx: FactoryContext) -> None:
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


   class ShadowTriggerPlugin(Plugin):
       """Register a trigger that writes to the shadow table on every change."""

       def __init__(self, shadow_key: str = "shadow") -> None:
           self.shadow_key = shadow_key

       def create_triggers(self, ctx: FactoryContext) -> None:
           shadow = ctx[self.shadow_key]   # written by ShadowTablePlugin
           # ... register trigger using shadow.name, etc.


   # Use them together
   SimpleDimensionFactory(
       "products", "app", metadata, dimensions,
       extra_plugins=[ShadowTablePlugin(), ShadowTriggerPlugin()],
   )

Because ``ShadowTablePlugin`` appears before ``ShadowTriggerPlugin`` in the
list and both operate in the same phase, the context key ``"shadow"`` is
always available when ``ShadowTriggerPlugin.create_triggers`` is called.


Global configuration
--------------------

:class:`~cave.config.CaveConfig` holds the global plugin list that is
prepended to every factory that references it.

.. code-block:: python

   from cave.config import CaveConfig

   cave_cfg = CaveConfig()
   cave_cfg.register(TimestampPlugin(), TenantPlugin())

   # -- or equivalently --
   cave_cfg = CaveConfig(plugins=[TimestampPlugin(), TenantPlugin()])

Pass it to each factory via the ``cave=`` argument.  A common pattern is to
create one ``CaveConfig`` per project and import it wherever factories are
defined:

.. code-block:: python

   # cave_setup.py
   from cave.config import CaveConfig
   from myapp.plugins import TimestampPlugin, TenantPlugin

   cave_cfg = CaveConfig()
   cave_cfg.register(TimestampPlugin(), TenantPlugin())

   # models.py
   from myapp.cave_setup import cave_cfg
   from cave.factory.dimension.simple import SimpleDimensionFactory

   SimpleDimensionFactory(
       "users", "app", metadata, dimensions, cave=cave_cfg
   )


Built-in plugins reference
--------------------------

See the :doc:`api` reference for full autodoc on all built-in plugins.
