Extension system
================

While :doc:`plugins` compose behaviour *within* a single factory,
**extensions** sit one level above: they bundle plugins, metadata hooks,
Alembic hooks, and CLI commands into a single installable unit.

Extensions make it possible for third-party packages to extend pgcraft,
and for pgcraft's own opt-in subsystems (PostgREST, future auth/RLS) to
be cleanly separated from the core.


Quick start
-----------

Register an extension on your :class:`~pgcraft.config.PGCraftConfig`:

.. code-block:: python

   from pgcraft.config import PGCraftConfig
   from pgcraft.extensions.postgrest import PostgRESTExtension

   config = PGCraftConfig()
   config.use(PostgRESTExtension())

Pass this config to your factories and to
:func:`~pgcraft.alembic.register.pgcraft_configure_metadata`:

.. code-block:: python

   from pgcraft.alembic.register import pgcraft_configure_metadata

   PGCraftSimple("users", "public", metadata, ..., config=config)
   pgcraft_configure_metadata(metadata, config)


Extension hooks
---------------

:class:`~pgcraft.extension.PGCraftExtension` provides five hooks.  Override
only the ones you need — every hook is a no-op by default.

``plugins()``
    Return a list of :class:`~pgcraft.plugin.Plugin` instances that are
    prepended to every factory's plugin list.

``configure_metadata(metadata)``
    Register roles, grants, schemas, or other metadata-level objects.
    Called by :func:`~pgcraft.alembic.register.pgcraft_configure_metadata`.

``configure_alembic()``
    Register custom Alembic renderers or rewriters.  Called by
    :func:`~pgcraft.alembic.register.pgcraft_alembic_hook`.

``register_cli(app)``
    Add subcommands to the ``pgcraft`` CLI.

``validate(registered_names)``
    Check that required peer extensions are present.  Called after all
    extensions are loaded.


Inter-extension dependencies
----------------------------

Declare dependencies using the ``depends_on`` class variable:

.. code-block:: python

   from dataclasses import dataclass
   from typing import ClassVar

   from pgcraft.extension import PGCraftExtension

   @dataclass
   class MyExtension(PGCraftExtension):
       name: str = "my-ext"
       depends_on: ClassVar[list[str]] = ["postgrest"]

pgcraft validates that all declared dependencies are present when
extensions are resolved.  A
:class:`~pgcraft.errors.PGCraftValidationError` is raised if any are
missing.


Entry point discovery
---------------------

Third-party packages can register extensions via the
``pgcraft.extensions`` entry point group in ``pyproject.toml``:

.. code-block:: toml

   [project.entry-points."pgcraft.extensions"]
   nanoid = "pgcraft_nanoid:NanoIDExtension"

Discovered extensions are automatically loaded unless
``auto_discover=False`` is set on the config.  Manually registered
extensions take precedence over discovered ones with the same name.


Writing an extension
--------------------

Here are three example tiers, from simple to complex.


Column-level extension (NanoID PK)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An extension that contributes a single plugin to replace the default
serial PK with a NanoID:

.. code-block:: python

   from dataclasses import dataclass

   from pgcraft.extension import PGCraftExtension
   from pgcraft.plugin import Plugin


   class NanoIDPKPlugin(Plugin):
       """Replace serial PK with a NanoID column."""
       # ... plugin implementation ...


   @dataclass
   class NanoIDExtension(PGCraftExtension):
       name: str = "nanoid"

       def plugins(self) -> list[Plugin]:
           return [NanoIDPKPlugin()]


Composite extension (audit trail)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An extension that bundles multiple plugins — a shadow table and
a trigger that writes to it:

.. code-block:: python

   from dataclasses import dataclass

   from pgcraft.extension import PGCraftExtension
   from pgcraft.plugin import Plugin


   @dataclass
   class AuditExtension(PGCraftExtension):
       name: str = "audit"

       def plugins(self) -> list[Plugin]:
           return [
               ShadowTablePlugin(),
               ShadowTriggerPlugin(),
           ]


Full subsystem extension (PostgREST)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The built-in PostgREST extension demonstrates a full subsystem:
metadata hooks for roles/grants.

.. code-block:: python

   from dataclasses import dataclass
   from typing import TYPE_CHECKING

   from pgcraft.extension import PGCraftExtension

   if TYPE_CHECKING:
       from sqlalchemy import MetaData

   @dataclass
   class PostgRESTExtension(PGCraftExtension):
       name: str = "postgrest"
       schema: str = "api"

       def configure_metadata(self, metadata: MetaData) -> None:
           from pgcraft.models.roles import register_roles
           register_roles(metadata)


Built-in extensions
-------------------

``postgrest``
    Registers PostgREST roles (``authenticator``, ``anon``) and
    per-resource grants on metadata.  Without this extension,
    no roles or grants are generated.

    The module also re-exports the API view and plugin classes
    as ``PostgRESTView`` and ``PostgRESTPlugin``:

    .. code-block:: python

       from pgcraft.extensions.postgrest import (
           PostgRESTExtension,
           PostgRESTPlugin,
           PostgRESTView,
       )

       config = PGCraftConfig()
       config.use(PostgRESTExtension())

       # PostgRESTView is the primary API view class
       PostgRESTView(source=factory, grants=["select"])
