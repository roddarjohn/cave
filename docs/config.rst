Configuration
=============

:class:`~pgcraft.config.PGCraftConfig` is the central object for
project-wide pgcraft settings.  Create one per project and pass it to
every factory and to :func:`~pgcraft.alembic.register.pgcraft_configure_metadata`.

Creating a config
-----------------

.. code-block:: python

   from pgcraft.config import PGCraftConfig

   config = PGCraftConfig()

Fields
------

``plugins``
~~~~~~~~~~~

A list of :class:`~pgcraft.plugin.Plugin` instances that are prepended
to every factory's plugin list.  Use :meth:`~pgcraft.config.PGCraftConfig.register`
to add plugins after construction:

.. code-block:: python

   config.register(TimestampPlugin(), TenantPlugin())

   # or at construction time
   config = PGCraftConfig(plugins=[TimestampPlugin(), TenantPlugin()])

See :doc:`plugins` for details on plugin resolution order.

``extensions``
~~~~~~~~~~~~~~

A list of :class:`~pgcraft.extension.PGCraftExtension` instances.
Use :meth:`~pgcraft.config.PGCraftConfig.use` to add extensions:

.. code-block:: python

   from pgcraft.extensions.postgrest import PostgRESTExtension

   config.use(PostgRESTExtension())

See :doc:`extensions` for the full extension guide.

``auto_discover``
~~~~~~~~~~~~~~~~~

When ``True`` (the default), pgcraft discovers extensions registered
under the ``pgcraft.extensions`` entry point group.  Manually
registered extensions take precedence over discovered ones with the
same name.

Set to ``False`` to disable automatic discovery:

.. code-block:: python

   config = PGCraftConfig(auto_discover=False)

``utility_schema``
~~~~~~~~~~~~~~~~~~

The PostgreSQL schema where pgcraft creates utility functions (e.g.
``ledger_apply_state``).  Defaults to ``"pgcraft"``.

Override this only if your project already uses a schema named
``pgcraft``:

.. code-block:: python

   config = PGCraftConfig(utility_schema="my_utils")

Methods
-------

.. method:: PGCraftConfig.register(*plugins) -> PGCraftConfig

   Add one or more plugins to the global plugin list.  Returns
   ``self`` for chaining.

.. method:: PGCraftConfig.use(*extensions) -> PGCraftConfig

   Register one or more extensions.  Returns ``self`` for chaining.

.. attribute:: PGCraftConfig.all_plugins

   Combined list of extension plugins followed by direct plugins.
   Extension plugins are prepended so they run first.

Typical project setup
---------------------

A common pattern is to create one config module and import it
wherever factories are defined:

.. code-block:: python

   # pgcraft_setup.py
   from pgcraft.config import PGCraftConfig
   from pgcraft.extensions.postgrest import PostgRESTExtension
   from myapp.plugins import TimestampPlugin, TenantPlugin

   pgcraft_cfg = PGCraftConfig()
   pgcraft_cfg.use(PostgRESTExtension())
   pgcraft_cfg.register(TimestampPlugin(), TenantPlugin())

.. code-block:: python

   # models.py
   from myapp.pgcraft_setup import pgcraft_cfg
   from pgcraft.factory import PGCraftSimple

   PGCraftSimple(
       "users", "app", metadata, schema_items,
       config=pgcraft_cfg,
   )

.. code-block:: python

   # migrations/env.py
   from pgcraft.alembic.register import pgcraft_configure_metadata
   from myapp.pgcraft_setup import pgcraft_cfg

   pgcraft_configure_metadata(
       target_metadata, config=pgcraft_cfg,
   )
