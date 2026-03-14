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
:class:`~pgcraft.views.api.APIView`.  Since this recipe does not
use PostgREST, simply omit the ``APIView`` call.

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
``APIView`` call.

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
:class:`~pgcraft.views.api.APIView` to expose it.

How it works
~~~~~~~~~~~~

When :class:`~pgcraft.views.api.APIView` is called it:

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
   from pgcraft.views import APIView
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

   APIView(source=products)

This creates:

* ``inventory.products`` — the backing table with a serial PK.
* ``api.products`` — a view for PostgREST to expose.
* INSTEAD OF triggers so ``INSERT`` / ``UPDATE`` / ``DELETE`` through
  the view are forwarded to the backing table.
* An ``anon`` role with ``SELECT`` grants on ``api.products``.

Customising grants
~~~~~~~~~~~~~~~~~~

By default the ``anon`` role gets only ``SELECT``. Pass a
``grants`` list to :class:`~pgcraft.views.api.APIView` to allow
writes:

.. code-block:: python

   APIView(
       source=products,
       grants=["select", "insert", "update", "delete"],
   )

Changing the API schema
~~~~~~~~~~~~~~~~~~~~~~~

The default API schema is ``api``. Override it with the ``schema``
parameter:

.. code-block:: python

   APIView(source=products, schema="reporting")

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

By default :class:`~pgcraft.views.api.APIView` creates a
``SELECT *`` view.  Pass a ``columns`` list to expose only
specific columns through the API — useful when a table has
internal columns that should not be visible to API consumers.

.. code-block:: python

   from sqlalchemy import Column, MetaData, Numeric, String, Text

   from pgcraft.factory import PGCraftSimple
   from pgcraft.views import APIView
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

   APIView(
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

   APIView(
       source=products,
       exclude_columns=["internal_notes"],
   )


.. _cookbook-statistics-views:

Joining statistics views into the API
-------------------------------------

Use :class:`~pgcraft.statistics.PGCraftStatisticsView` to expose
aggregate or derived data as read-only columns in the API view.

:class:`~pgcraft.plugins.statistics.StatisticsViewPlugin` is
included in every dimension factory's default plugins and is a
no-op when no statistics items are present.

Statistics queries are defined using SQLAlchemy ``select()``
expressions — column names are derived automatically from the
query, so there is nothing to keep in sync.

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
       Table,
       func,
       select,
   )

   from pgcraft.factory import PGCraftSimple
   from pgcraft.statistics import PGCraftStatisticsView
   from pgcraft.views import APIView
   from pgcraft import (
       pgcraft_build_naming_conventions,
   )

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )

   # -- Reference tables (already exist in the database) ------

   orders = Table(
       "orders",
       metadata,
       Column("id", Integer, primary_key=True),
       Column("customer_id", Integer),
       Column("total", Numeric(10, 2)),
       schema="public",
   )

   invoices = Table(
       "invoices",
       metadata,
       Column("id", Integer, primary_key=True),
       Column("customer_id", Integer),
       Column("amount", Numeric(10, 2)),
       Column("paid", Integer),
       schema="public",
   )

   # -- Statistics queries ------------------------------------

   order_stats = select(
       orders.c.customer_id,
       func.count().label("order_count"),
       func.sum(orders.c.total).label("order_total"),
   ).group_by(orders.c.customer_id)

   invoice_stats = select(
       invoices.c.customer_id,
       func.count().label("invoice_count"),
       func.sum(invoices.c.amount).label("invoiced_total"),
       func.sum(invoices.c.paid).label("paid_total"),
   ).group_by(invoices.c.customer_id)

   # -- Dimension with statistics -----------------------------

   customer = PGCraftSimple(
       tablename="customer",
       schemaname="dim",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("email", String),
           PGCraftStatisticsView(
               name="orders",
               query=order_stats,
               join_key="customer_id",
           ),
           PGCraftStatisticsView(
               name="invoices",
               query=invoice_stats,
               join_key="customer_id",
           ),
       ],
   )

   APIView(source=customer)

This creates:

* ``dim.customer`` — the backing table (``id``, ``name``,
  ``email``).
* ``dim.customer_orders_statistics`` — a view aggregating orders.
* ``dim.customer_invoices_statistics`` — a view aggregating
  invoices.
* ``api.customer`` — the API view with LEFT JOINs to both
  statistics views.

The generated API view looks like:

.. code-block:: sql

   SELECT p.id, p.name, p.email,
          s.order_count, s.order_total,
          s1.invoice_count, s1.invoiced_total, s1.paid_total
   FROM dim.customer AS p
   LEFT OUTER JOIN dim.customer_orders_statistics AS s
     ON p.id = s.customer_id
   LEFT OUTER JOIN dim.customer_invoices_statistics AS s1
     ON p.id = s1.customer_id

The join key column (``customer_id``) is automatically excluded
from the API select list — only the aggregate columns appear.

How it works
~~~~~~~~~~~~

1. ``PGCraftStatisticsView`` items are filtered out of
   ``schema_items`` before table creation — they do not add
   columns to the backing table.
2. ``StatisticsViewPlugin`` (an internal plugin) compiles each
   query to SQL and creates a view (or materialized view) named
   ``{tablename}_{name}_statistics``.
3. ``APIView`` reads the view info and generates LEFT JOINs
   into the API view automatically.

Column names are derived from the ``select()`` expression
automatically.  The ``join_key`` column is included in the view
(for the JOIN) but excluded from the API select list.

Custom schema for statistics views
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default statistics views are created in the same schema as the
dimension.  Pass a ``schema`` to place them elsewhere:

.. code-block:: python

   PGCraftStatisticsView(
       name="orders",
       query=order_stats,
       join_key="customer_id",
       schema="analytics",
   )

Materialized statistics
~~~~~~~~~~~~~~~~~~~~~~~

For expensive aggregations, set ``materialized=True`` to create a
materialized view that must be refreshed manually:

.. code-block:: python

   PGCraftStatisticsView(
       name="lifetime",
       query=select(
           orders.c.customer_id,
           func.sum(orders.c.total).label("lifetime_value"),
       ).group_by(orders.c.customer_id),
       join_key="customer_id",
       materialized=True,
   )

Refresh it on a schedule:

.. code-block:: sql

   REFRESH MATERIALIZED VIEW dim.customer_lifetime_statistics;

Join key
~~~~~~~~

The ``join_key`` parameter tells the plugin which column in the
statistics query corresponds to the primary table's PK.  This is
required when the foreign key column name differs from the PK
column name (which is the common case — e.g. ``customer_id`` in
orders vs. ``id`` in customers).

When ``join_key`` is omitted it defaults to the PK column name,
which works when the statistics query uses the same name:

.. code-block:: python

   # Works because the query selects "id", matching the PK
   PGCraftStatisticsView(
       name="lines",
       query=select(
           orders.c.id,
           func.count().label("line_count"),
       ).group_by(orders.c.id),
   )

Combining column selection with statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``columns`` on ``APIView`` to control which table columns
appear, while statistics columns are always appended:

.. code-block:: python

   from pgcraft.views import APIView

   APIView(source=customer, columns=["id", "name"])


.. _cookbook-computed-and-statistics:

Computed columns with statistics
--------------------------------

Combine PostgreSQL computed columns (``Computed``) with statistics
views for a dimension that has both derived table columns and
aggregated read-only data from related tables.

``Computed`` columns live in the backing table — PostgreSQL
evaluates them automatically from other columns.  Statistics views
are separate views that get LEFT JOINed into the API.

.. code-block:: python

   from sqlalchemy import (
       Column,
       Computed,
       Integer,
       MetaData,
       Numeric,
       String,
       Table,
       func,
       select,
   )

   from pgcraft.factory import PGCraftSimple
   from pgcraft.statistics import PGCraftStatisticsView
   from pgcraft.views import APIView
   from pgcraft import (
       pgcraft_build_naming_conventions,
   )

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )

   # Reference table
   orders = Table(
       "orders",
       metadata,
       Column("id", Integer, primary_key=True),
       Column("customer_id", Integer),
       Column("total", Numeric(10, 2)),
       schema="public",
   )

   order_stats = select(
       orders.c.customer_id,
       func.count().label("order_count"),
       func.sum(orders.c.total).label("order_total"),
   ).group_by(orders.c.customer_id)

   products = PGCraftSimple(
       tablename="products",
       schemaname="inventory",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("price", Integer, nullable=False),
           Column("qty", Integer, nullable=False),
           # Computed: Postgres evaluates this automatically
           Column(
               "total",
               Integer,
               Computed("price * qty"),
           ),
           # Statistics: aggregated from a related table
           PGCraftStatisticsView(
               name="orders",
               query=order_stats,
               join_key="customer_id",
           ),
       ],
   )

   APIView(source=products)

This creates:

* ``inventory.products`` — backing table with ``id``, ``name``,
  ``price``, ``qty``, and ``total`` (a generated column).
* ``inventory.products_orders_statistics`` — order aggregation
  view.
* ``api.products`` — API view with the computed ``total`` column
  from the table and the ``order_count`` / ``order_total``
  columns LEFT JOINed from the statistics view.

The key distinction: ``Computed`` is a native PostgreSQL feature
for deriving a column from others in the same row, while
``PGCraftStatisticsView`` creates a separate view that aggregates
data from other tables and joins it into the API.
