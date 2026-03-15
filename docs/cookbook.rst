Cookbook
=======

Practical recipes for common pgcraft use cases.

.. _cookbook-migrations-only:

Migrations only
---------------

Use pgcraft purely as a migration generator — define your schema with
pgcraft factories, produce Alembic migrations, and export them as raw
SQL. No pgcraft code runs at application time.

API views are created separately via
:class:`~pgcraft.views.api.PostgRESTView`.  Since this recipe does not
use PostgREST, simply omit the ``PostgRESTView`` call.

**Project layout**::

    myproject/
    ├── alembic.ini
    ├── models.py
    └── migrations/
        ├── env.py
        └── versions/

**1. Define your schema**

.. code-block:: python

   # models.py
   from sqlalchemy import Column, MetaData, Numeric, String

   from pgcraft.factory import PGCraftSimple
   from pgcraft import pgcraft_build_naming_conventions

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )

   PGCraftSimple(
       tablename="products",
       schemaname="inventory",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("sku", String(32), nullable=False),
           Column("price", Numeric(10, 2), nullable=False),
       ],
   )

**2. Wire up Alembic**

Follow the standard :doc:`setup` instructions: call
``pgcraft_alembic_hook()`` early in ``env.py``, call
``pgcraft_configure_metadata()`` after loading your metadata, and pass
``pgcraft_process_revision_directives`` to ``context.configure()``.

**3. Generate a migration**

.. code-block:: bash

   alembic revision --autogenerate -m "add products table"

Review the generated Python file in ``migrations/versions/``.

**4. Export as raw SQL**

Alembic's ``--sql`` flag renders migrations as plain SQL instead of
executing them against a database. This is useful when you hand
migrations off to a DBA, run them via CI, or apply them with ``psql``
directly.

Generate the upgrade SQL for all pending migrations:

.. code-block:: bash

   # Upgrade from nothing to head (full schema)
   alembic upgrade head --sql > upgrade.sql

Generate SQL for a specific revision range:

.. code-block:: bash

   # From one revision to another
   alembic upgrade abc123:def456 --sql > upgrade.sql

   # Downgrade SQL
   alembic downgrade def456:abc123 --sql > downgrade.sql

The resulting ``.sql`` files are standalone — they have no dependency on
pgcraft or Python. You can commit them to your repo, review them in a
PR, or hand them to your ops team.

**5. Apply with psql**

.. code-block:: bash

   psql -d mydb -f upgrade.sql

At this point pgcraft has done its job. Your application code never
imports pgcraft — it only consumes the database schema that the
migrations created.


.. _cookbook-models-in-app:

Using pgcraft models in your application
----------------------------------------

pgcraft factories produce real SQLAlchemy tables registered on a shared
``MetaData`` instance. This means you can query and insert data using
SQLAlchemy Core or ORM directly — in Flask, FastAPI, or any other
framework.

This recipe uses the :func:`~pgcraft.declarative.register` decorator
to define models. The decorator registers the table on ``metadata`` at
import time — access it via
``metadata.tables["<schema>.<tablename>"]`` to get a standard
:class:`sqlalchemy.schema.Table` object.

Since this recipe does not use PostgREST, simply omit the
``PostgRESTView`` call.

Shared models module
~~~~~~~~~~~~~~~~~~~~

Put your pgcraft definitions in a module that both Alembic and your
application import:

.. code-block:: python

   # myapp/models.py
   from sqlalchemy import Column, Integer, MetaData, String

   from pgcraft.declarative import register
   from pgcraft import pgcraft_build_naming_conventions

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )


   @register(metadata=metadata)
   class Users:
       __tablename__ = "users"
       __table_args__ = {"schema": "public"}

       name = Column(String, nullable=False)
       email = Column(String, nullable=False)
       age = Column(Integer)

   # Grab the generated table for use in queries
   users = metadata.tables["public.users"]

FastAPI example
~~~~~~~~~~~~~~~

.. code-block:: python

   # app.py
   from fastapi import FastAPI, HTTPException
   from pydantic import BaseModel
   from sqlalchemy import create_engine, select

   from myapp.models import users

   engine = create_engine("postgresql+psycopg://localhost/mydb")
   app = FastAPI()


   class UserCreate(BaseModel):
       name: str
       email: str
       age: int | None = None


   @app.get("/users")
   def list_users():
       with engine.connect() as conn:
           rows = conn.execute(select(users)).mappings().all()
           return [dict(r) for r in rows]


   @app.get("/users/{user_id}")
   def get_user(user_id: int):
       with engine.connect() as conn:
           row = conn.execute(
               select(users).where(users.c.id == user_id)
           ).mappings().first()
           if not row:
               raise HTTPException(status_code=404)
           return dict(row)


   @app.post("/users", status_code=201)
   def create_user(body: UserCreate):
       with engine.begin() as conn:
           result = conn.execute(
               users.insert()
               .values(**body.model_dump(exclude_none=True))
               .returning(users)
           )
           return dict(result.mappings().first())

Flask example
~~~~~~~~~~~~~

.. code-block:: python

   # app.py
   from flask import Flask, jsonify, request
   from sqlalchemy import create_engine, select

   from myapp.models import users

   engine = create_engine("postgresql+psycopg://localhost/mydb")
   app = Flask(__name__)


   @app.get("/users")
   def list_users():
       with engine.connect() as conn:
           rows = conn.execute(select(users)).mappings().all()
           return jsonify([dict(r) for r in rows])


   @app.post("/users")
   def create_user():
       data = request.get_json()
       with engine.begin() as conn:
           result = conn.execute(
               users.insert().values(**data).returning(users)
           )
           return jsonify(dict(result.mappings().first())), 201


.. _cookbook-postgrest:

Adding PostgREST API views
--------------------------

pgcraft can generate PostgREST-compatible API views, INSTEAD OF
triggers for write operations, and the role/grant declarations that
PostgREST expects.  Create a factory, then call
:class:`~pgcraft.views.api.PostgRESTView` to expose it.

How it works
~~~~~~~~~~~~

When :class:`~pgcraft.views.api.PostgRESTView` is called it:

1. Creates a view in the ``api`` schema (configurable) that
   ``SELECT *`` s from the backing table.
2. Registers an :class:`~pgcraft.resource.APIResource` on the
   metadata so that pgcraft can generate role and grant statements.
3. Creates INSTEAD OF ``INSERT`` / ``UPDATE`` / ``DELETE`` triggers
   on the view so PostgREST clients can write through it.

Minimal example
~~~~~~~~~~~~~~~

.. code-block:: python

   # models.py
   from sqlalchemy import Column, Integer, MetaData, Numeric, String

   from pgcraft.factory import PGCraftSimple
   from pgcraft.views import PostgRESTView
   from pgcraft import pgcraft_build_naming_conventions

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )

   products = PGCraftSimple(
       tablename="products",
       schemaname="inventory",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("sku", String(32), nullable=False),
           Column("price", Numeric(10, 2), nullable=False),
       ],
   )

   PostgRESTView(source=products)

This creates:

* ``inventory.products`` — the backing table with a serial PK.
* ``api.products`` — a view for PostgREST to expose.
* INSTEAD OF triggers so ``INSERT`` / ``UPDATE`` / ``DELETE`` through
  the view are forwarded to the backing table.
* An ``anon`` role with ``SELECT`` grants on ``api.products``.

Customising grants
~~~~~~~~~~~~~~~~~~

By default the ``anon`` role gets only ``SELECT``. Pass a
``grants`` list to :class:`~pgcraft.views.api.PostgRESTView` to allow
writes:

.. code-block:: python

   PostgRESTView(
       source=products,
       grants=["select", "insert", "update", "delete"],
   )

Grants drive triggers: INSTEAD OF triggers are only created for the
DML operations listed in ``grants``.  A ``["select"]``-only view
has no triggers and is read-only.  A view with
``["select", "insert"]`` gets only an INSERT trigger — no UPDATE or
DELETE.

Changing the API schema
~~~~~~~~~~~~~~~~~~~~~~~

The default API schema is ``api``. Override it with the ``schema``
parameter:

.. code-block:: python

   PostgRESTView(source=products, schema="reporting")

PostgREST setup
~~~~~~~~~~~~~~~

After generating and applying migrations, point PostgREST at your
database. A minimal ``postgrest.conf``:

.. code-block:: ini

   db-uri = "postgresql://authenticator:changeme@localhost/mydb"
   db-schemas = "api"
   db-anon-role = "anon"
   db-extra-search-path = "public, inventory"

Start the server and query the API:

.. code-block:: bash

   postgrest postgrest.conf

   # List all products
   curl -s http://localhost:3000/products | python3 -m json.tool

   # Filter
   curl -s "http://localhost:3000/products?name=eq.Widget"

   # Insert (requires insert grant)
   curl -s http://localhost:3000/products \
       -H "Content-Type: application/json" \
       -d '{"name": "Widget", "sku": "W-001", "price": 9.99}'

See the `PostgREST documentation <https://docs.postgrest.org>`_ for
the full query syntax and configuration reference.


.. _cookbook-column-selection:

Exposing a subset of columns
-----------------------------

By default :class:`~pgcraft.views.api.PostgRESTView` creates a
``SELECT *`` view.  Pass a ``columns`` list to expose only
specific columns through the API — useful when a table has
internal columns that should not be visible to API consumers.

.. code-block:: python

   from sqlalchemy import Column, MetaData, Numeric, String, Text

   from pgcraft.factory import PGCraftSimple
   from pgcraft.views import PostgRESTView
   from pgcraft import pgcraft_build_naming_conventions

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )

   products = PGCraftSimple(
       tablename="products",
       schemaname="inventory",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("sku", String(32), nullable=False),
           Column("price", Numeric(10, 2), nullable=False),
           Column("internal_notes", Text),  # hidden from API
       ],
   )

   PostgRESTView(
       source=products,
       columns=["id", "name", "sku", "price"],
   )

The generated view selects only the listed columns:

.. code-block:: sql

   CREATE VIEW api.products AS
   SELECT p.id, p.name, p.sku, p.price
   FROM inventory.products AS p

The ``internal_notes`` column exists in the backing table but is
invisible through the PostgREST API.  Any column name not found on
the table raises ``ValueError`` at factory construction time.

Alternatively, use ``exclude_columns`` to hide specific columns
while including everything else — often more convenient for large
tables:

.. code-block:: python

   PostgRESTView(
       source=products,
       exclude_columns=["internal_notes"],
   )


.. _cookbook-statistics-views:

Joining aggregate views into the API
-------------------------------------

Create standalone aggregate views with
:class:`~pgcraft.views.view.PGCraftView`, then join them into an
API view using the ``query=`` parameter on
:class:`~pgcraft.views.api.PostgRESTView`.

Each ``PGCraftView`` exposes a ``.table`` property — a joinable
SQLAlchemy selectable — so you can compose joins using standard
SQLAlchemy syntax.

Multiple statistics on one dimension
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A realistic customer dimension with both order and invoice
statistics:

.. code-block:: python

   from sqlalchemy import (
       Column,
       Integer,
       MetaData,
       Numeric,
       String,
       func,
       select,
   )

   from pgcraft.factory import PGCraftSimple
   from pgcraft.views import PostgRESTView
   from pgcraft.views.view import PGCraftView
   from pgcraft import (
       pgcraft_build_naming_conventions,
   )

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )

   # -- Table factories ----------------------------------------

   Orders = PGCraftSimple(
       "orders", "public", metadata,
       schema_items=[
           Column("customer_id", Integer, nullable=False),
           Column("total", Numeric(10, 2), nullable=False),
       ],
   )

   customers = PGCraftSimple(
       "customers", "public", metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("email", String),
       ],
   )

   # -- Standalone aggregate views ----------------------------

   _orders_t = Orders.table
   order_stats = PGCraftView(
       "customer_order_stats", "public", metadata,
       query=select(
           _orders_t.c.customer_id,
           func.count().label("order_count"),
           func.sum(_orders_t.c.total).label("order_total"),
       ).group_by(_orders_t.c.customer_id),
   )

   # -- API view with joined statistics -----------------------
   # PGCraftView.table is a joinable SQLAlchemy Table.
   # Triggers still work — they operate on the base table
   # columns; joined columns are read-only.

   _os = order_stats.table

   PostgRESTView(
       source=customers,
       grants=["select", "insert", "update", "delete"],
       query=lambda q, t: (
           select(
               t.c.id,
               t.c.name,
               t.c.email,
               _os.c.order_count,
               _os.c.order_total,
           )
           .select_from(t)
           .outerjoin(
               _os, t.c.id == _os.c.customer_id
           )
       ),
   )

This creates:

* ``public.customers`` — the backing table (``id``, ``name``,
  ``email``).
* ``public.customer_order_stats`` — a standalone aggregate view.
* ``api.customers`` — the API view with a LEFT JOIN to the
  statistics view.

The ``query=`` lambda receives the base ``SELECT *`` query and the
source table. Return any valid SQLAlchemy ``Select`` — add joins,
filter columns, or transform freely. INSTEAD OF triggers are still
created for the base table columns; joined columns are read-only.

How it works
~~~~~~~~~~~~

1. ``PGCraftView`` creates a standalone view from any SQLAlchemy
   ``select()`` expression and exposes ``.table`` for use in
   further joins.
2. ``PostgRESTView`` with ``query=`` uses the lambda to customise the
   view definition. Grants drive which INSTEAD OF triggers are
   created.
3. Writable columns are automatically restricted to the base
   table's dimension columns — joined columns cannot be written
   through the API view.


.. _cookbook-computed-columns:

Computed columns
----------------

PostgreSQL computed columns (``Computed``) are derived from other
columns in the same row.  PostgreSQL evaluates them automatically
— they appear in the API view like any other column but cannot be
written to.

.. code-block:: python

   from sqlalchemy import (
       Column,
       Computed,
       Integer,
       MetaData,
       String,
   )

   from pgcraft.factory import PGCraftSimple
   from pgcraft.views import PostgRESTView
   from pgcraft import pgcraft_build_naming_conventions

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )

   products = PGCraftSimple(
       "products", "inventory", metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("price", Integer, nullable=False),
           Column("qty", Integer, nullable=False),
           Column(
               "total", Integer,
               Computed("price * qty"),
           ),
       ],
   )

   PostgRESTView(
       source=products,
       grants=["select", "insert", "update", "delete"],
   )

The ``total`` column is a generated column — it appears in the
API but is computed by PostgreSQL, not writable through the API.
