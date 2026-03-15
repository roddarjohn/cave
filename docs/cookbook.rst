Cookbook
========

Practical recipes for common pgcraft use cases.

.. _cookbook-migrations-only:

Migrations only
---------------

Use pgcraft purely as a migration generator — define your schema with
pgcraft factories, produce Alembic migrations, and export them as raw
SQL. No pgcraft code runs at application time.

To add PostgREST API views, see :ref:`ext-postgrest`.

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

To add PostgREST API views, see :ref:`ext-postgrest`.

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


For PostgREST API views, computed columns, column filtering,
and aggregate view joins, see :ref:`ext-postgrest`.


.. _cookbook-indices-and-fks:

Indices and foreign keys
------------------------

pgcraft supports declarative index and foreign key definitions
using ``{column_name}`` markers — the same syntax used by
:class:`~pgcraft.check.PGCraftCheck`.

Adding indices
~~~~~~~~~~~~~~

Use :class:`~pgcraft.index.PGCraftIndex` in ``schema_items``.
The constructor mirrors ``sqlalchemy.Index``: name first, then
expressions, then keyword arguments passed through to the
underlying index.

.. code-block:: python

   from sqlalchemy import Column, Integer, MetaData, String

   from pgcraft.factory import PGCraftSimple
   from pgcraft.index import PGCraftIndex
   from pgcraft import pgcraft_build_naming_conventions

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )

   products = PGCraftSimple(
       "products", "inventory", metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("sku", String(32), nullable=False),
           Column("price", Integer, nullable=False),
           # Simple index
           PGCraftIndex("idx_products_sku", "{sku}"),
           # Unique index
           PGCraftIndex(
               "uq_products_name", "{name}", unique=True
           ),
           # Functional index with dialect kwargs
           PGCraftIndex(
               "idx_products_lower_name",
               "lower({name})",
               postgresql_using="btree",
           ),
           # Multi-column index
           PGCraftIndex(
               "idx_products_name_price",
               "{name}", "{price}",
           ),
       ],
   )

Adding foreign keys
~~~~~~~~~~~~~~~~~~~

Use :class:`~pgcraft.fk.PGCraftFK` in ``schema_items``.
Exactly one of ``references`` or ``raw_references`` must be
provided:

- ``references`` — ``"dimension.column"`` strings resolved via the
  dimension registry.  pgcraft finds the correct physical table
  regardless of dimension type (simple vs append-only).
- ``raw_references`` — ``"schema.table.column"`` strings passed
  through to SQLAlchemy directly, bypassing resolution.

**Resolved references** (dimension registry):

.. code-block:: python

   from sqlalchemy import Column, Integer, MetaData, String

   from pgcraft.factory import PGCraftSimple
   from pgcraft.fk import PGCraftFK
   from pgcraft import pgcraft_build_naming_conventions

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )

   customers = PGCraftSimple(
       "customers", "public", metadata,
       schema_items=[
           Column("name", String, nullable=False),
       ],
   )

   orders = PGCraftSimple(
       "orders", "public", metadata,
       schema_items=[
           Column("customer_id", Integer, nullable=False),
           Column("total", Integer, nullable=False),
           PGCraftFK(
               references={
                   "{customer_id}": "customers.id"
               },
               name="fk_orders_customer",
               ondelete="CASCADE",
           ),
       ],
   )

The ``"customers.id"`` reference is resolved to the physical table
via the dimension registry.  If ``customers`` were an append-only
dimension, the FK would point to the root table automatically.

**Raw references** (bypass resolution):

.. code-block:: python

   PGCraftFK(
       raw_references={
           "{org_id}": "public.organizations.id"
       },
       name="fk_orders_org",
   )

Use ``raw_references`` when referencing tables outside pgcraft
or when you want full control over the target.

See :doc:`constraints_and_indices` for a walkthrough of the
generated SQL.
