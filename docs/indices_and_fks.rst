Indices and foreign keys
========================

pgcraft supports declarative indices and foreign keys using the
same ``{column_name}`` marker syntax as
:class:`~pgcraft.check.PGCraftCheck`.  This page walks through
the generated SQL for each feature.


Indices
-------

:class:`~pgcraft.index.PGCraftIndex` mirrors the
``sqlalchemy.Index`` constructor: name first, then column
expressions, then keyword arguments passed through to the
underlying index.

Configuration
^^^^^^^^^^^^^

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
           PGCraftIndex("idx_products_sku", "{sku}"),
           PGCraftIndex(
               "uq_products_name", "{name}", unique=True
           ),
           PGCraftIndex(
               "idx_products_lower_name",
               "lower({name})",
               postgresql_using="btree",
           ),
       ],
   )

Generated SQL
^^^^^^^^^^^^^

.. code-block:: sql

   CREATE TABLE inventory.products (
       id   SERIAL PRIMARY KEY,
       name VARCHAR NOT NULL,
       sku  VARCHAR(32) NOT NULL,
       price INTEGER NOT NULL
   );

   -- Simple index on a column
   CREATE INDEX idx_products_sku
       ON inventory.products (sku);

   -- Unique index
   CREATE UNIQUE INDEX uq_products_name
       ON inventory.products (name);

   -- Functional index with dialect kwargs
   CREATE INDEX idx_products_lower_name
       ON inventory.products
       USING btree (lower(name));

Resulting schema
^^^^^^^^^^^^^^^^

.. code-block:: text

   =# \d inventory.products
                                      Table "inventory.products"
    Column |         Type          | Collation | Nullable |                Default
   --------+-----------------------+-----------+----------+---------------------------------------
    id     | integer               |           | not null | nextval('products_id_seq'::regclass)
    name   | character varying     |           | not null |
    sku    | character varying(32) |           | not null |
    price  | integer               |           | not null |
   Indexes:
       "pk__products__id" PRIMARY KEY, btree (id)
       "uq_products_name" UNIQUE, btree (name)
       "idx_products_sku" btree (sku)
       "idx_products_lower_name" btree (lower(name::text))

Index types
^^^^^^^^^^^

``PGCraftIndex`` supports the same keyword arguments as
``sqlalchemy.Index``.  Common options:

.. list-table::
   :header-rows: 1

   * - Keyword
     - Effect
   * - ``unique=True``
     - Creates a ``UNIQUE`` index
   * - ``postgresql_using="gin"``
     - Uses the GIN index method
   * - ``postgresql_where=text("active")``
     - Partial index (``WHERE active``)
   * - ``postgresql_ops={"data": "jsonb_path_ops"}``
     - Operator class for a column


Foreign keys
------------

:class:`~pgcraft.fk.PGCraftFK` defines foreign key constraints
with ``{column_name}`` markers for local columns.  Two reference
modes control how the target table is identified.

``references`` vs ``raw_references``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Exactly one must be provided:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Parameter
     - Format
     - When to use
   * - ``references``
     - ``["dimension.column"]``
     - Target is a pgcraft dimension.  Resolved via the dimension
       registry at factory time.
   * - ``raw_references``
     - ``["schema.table.column"]``
     - Target is outside pgcraft, or you want full control.
       Passed through to SQLAlchemy as-is.

Providing both raises ``PGCraftValidationError``.  Providing
neither also raises.


Example: resolved references
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When two pgcraft dimensions reference each other, use
``references``.  pgcraft resolves the dimension name to the
correct physical table automatically.

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
               columns=["{customer_id}"],
               references=["customers.id"],
               name="fk_orders_customer",
               ondelete="CASCADE",
           ),
       ],
   )

Generated SQL:

.. code-block:: sql

   CREATE TABLE public.customers (
       id   SERIAL PRIMARY KEY,
       name VARCHAR NOT NULL
   );

   CREATE TABLE public.orders (
       id          SERIAL PRIMARY KEY,
       customer_id INTEGER NOT NULL,
       total       INTEGER NOT NULL,
       CONSTRAINT fk_orders_customer
           FOREIGN KEY (customer_id)
           REFERENCES public.customers (id)
           ON DELETE CASCADE
   );

``"customers.id"`` was resolved to ``"public.customers.id"``
via the dimension registry.  The ``customers`` dimension
registered itself when its factory ran.


Example: raw references
^^^^^^^^^^^^^^^^^^^^^^^

When referencing a table that pgcraft does not manage, use
``raw_references`` with the full ``schema.table.column`` path:

.. code-block:: python

   PGCraftFK(
       columns=["{org_id}"],
       raw_references=["tenant.organizations.id"],
       name="fk_orders_org",
   )

Generated SQL:

.. code-block:: sql

   ALTER TABLE public.orders
       ADD CONSTRAINT fk_orders_org
       FOREIGN KEY (org_id)
       REFERENCES tenant.organizations (id);

No dimension lookup occurs — the string is used as-is.


Dimension resolution for append-only tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When the target dimension is append-only (SCD Type 2), the FK
resolves to the **root table**, not the attributes table or the
join view.  This is the correct target because the root table
holds the stable primary key.

.. code-block:: python

   from pgcraft.factory import PGCraftAppendOnly

   # Append-only: creates students (root) + students_log (attrs)
   students = PGCraftAppendOnly(
       "students", "public", metadata,
       schema_items=[
           Column("name", String, nullable=False),
       ],
   )

   enrollments = PGCraftSimple(
       "enrollments", "public", metadata,
       schema_items=[
           Column("student_id", Integer, nullable=False),
           Column("course", String, nullable=False),
           PGCraftFK(
               columns=["{student_id}"],
               references=["students.id"],
               name="fk_enrollment_student",
           ),
       ],
   )

Generated SQL:

.. code-block:: sql

   -- Root table (stable PK)
   CREATE TABLE public.students (
       id SERIAL PRIMARY KEY
   );

   -- Attributes table (append-only log)
   CREATE TABLE public.students_log (
       id         SERIAL PRIMARY KEY,
       created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
       students_id INTEGER NOT NULL
           REFERENCES public.students (id),
       name VARCHAR NOT NULL
   );

   -- FK points to the root table, not the log
   CREATE TABLE public.enrollments (
       id         SERIAL PRIMARY KEY,
       student_id INTEGER NOT NULL,
       course     VARCHAR NOT NULL,
       CONSTRAINT fk_enrollment_student
           FOREIGN KEY (student_id)
           REFERENCES public.students (id)
   );

The ``"students.id"`` reference resolved to
``public.students.id`` (the root table) because that is where the
stable primary key lives.  The dimension registry handles this
mapping — you do not need to know the internal table structure.


Multi-column foreign keys
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   PGCraftFK(
       columns=["{tenant_id}", "{org_id}"],
       raw_references=[
           "shared.orgs.tenant_id",
           "shared.orgs.org_id",
       ],
       name="fk_composite",
   )

Generated SQL:

.. code-block:: sql

   ALTER TABLE public.my_table
       ADD CONSTRAINT fk_composite
       FOREIGN KEY (tenant_id, org_id)
       REFERENCES shared.orgs (tenant_id, org_id);

Columns are matched positionally — the first local column maps to
the first reference column, and so on.


Cascade options
^^^^^^^^^^^^^^^

Both ``ondelete`` and ``onupdate`` accept any PostgreSQL action
string:

.. code-block:: python

   PGCraftFK(
       columns=["{customer_id}"],
       references=["customers.id"],
       name="fk_orders_customer",
       ondelete="CASCADE",
       onupdate="SET NULL",
   )

.. code-block:: sql

   CONSTRAINT fk_orders_customer
       FOREIGN KEY (customer_id)
       REFERENCES public.customers (id)
       ON DELETE CASCADE
       ON UPDATE SET NULL

Valid actions: ``CASCADE``, ``SET NULL``, ``SET DEFAULT``,
``RESTRICT``, ``NO ACTION`` (the default when omitted).
