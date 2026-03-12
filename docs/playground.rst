Playground
==========

The ``playground/`` directory is a local development scratchpad for manually
testing cave against a real PostgreSQL database. It is not part of the
distributed package — it exists purely for iterative development and
experimentation.

It includes:

* A SQLAlchemy model (``models.py``) you can extend freely
* An Alembic setup for managing the local database schema
* A ``Justfile`` with commands wrapping common database operations

Prerequisites
-------------

You need a running PostgreSQL server. The easiest options are:

**Docker (recommended)**

.. code-block:: bash

    docker run -d \
        --name cave-db \
        -p 5432:5432 \
        -e POSTGRES_DB=cave \
        -e POSTGRES_HOST_AUTH_METHOD=trust \
        postgres:17

**System install (Linux — Ubuntu/Debian)**

See the `PostgreSQL downloads page <https://www.postgresql.org/download/>`_
for the most up-to-date instructions for your distribution. On Ubuntu/Debian:

.. code-block:: bash

    sudo apt install -y postgresql
    sudo systemctl start postgresql
    sudo systemctl enable postgresql   # start automatically on boot

Then create a local superuser so you can connect without switching to the
``postgres`` system user:

.. code-block:: bash

    sudo -u postgres createuser --superuser $USER

By default, PostgreSQL requires a password for TCP connections. To avoid
this, connect via a Unix socket instead — peer auth is passwordless for your
local user with no extra configuration:

.. warning::

    Passwordless authentication is only appropriate for local development.
    Production databases should always require credentials.

Configuration
-------------

Copy the example env and set your database URL:

.. code-block:: bash

    cp playground/.env.example playground/.env

Edit ``playground/.env`` with your connection details:

.. code-block:: bash

    # System postgres via Unix socket (passwordless, recommended):
    DATABASE_URL=postgresql+psycopg:///cave

    # Docker or system postgres via TCP with no password:
    DATABASE_URL=postgresql+psycopg://localhost/cave

    # System postgres with a user/password:
    DATABASE_URL=postgresql+psycopg://user:password@localhost/cave

``playground/.env`` is gitignored and will never be committed.

Setup
-----

From the ``playground/`` directory, run::

    just init

This creates the database (safe to re-run if it already exists) and applies
all pending migrations.

Commands
--------

All commands are run from the ``playground/`` directory.

.. include:: _generated/playground_just_commands.rst

Typical workflow
----------------

1. Edit ``models.py`` to add or change a model
2. Generate a migration::

    just revision "add email_verified to users"

3. Review the generated file in ``migrations/versions/``
4. Apply it::

    just migrate

5. Write a script or use ``db-shell`` to verify the result::

    just db-shell

6. If something is wrong, roll back and adjust::

    just rollback


Adding models
-------------

Define new models in ``playground/models.py`` using the cave dimension
factory classes::

    SimpleDimensionFactory(
        tablename="users",
        schemaname="public",
        metadata=metadata,
        dimensions=[
            Column("name", Integer),
        ],
    )

Then generate and apply a migration::

    just revision "add users table"
    just migrate

Alembic autogenerate compares your models against the live database schema, so
it will pick up additions, removals, and column changes automatically.

PostgREST
---------

The playground includes a `PostgREST <https://postgrest.org>`_ configuration
for testing the generated API. Cave automatically creates API views and grants
for each factory-created table.

**Prerequisites**

Install PostgREST from the `official releases
<https://github.com/PostgREST/postgrest/releases>`_ and ensure it is on
your ``PATH``.

**Configuration**

The playground includes a ``postgrest.conf`` that connects as the
``authenticator`` role. The password is read from ``PGRST_DB_PASSWORD`` in
``.env``.

**Starting the server**

.. code-block:: bash

    just serve

**Querying the API**

.. code-block:: bash

    # List all resources
    just api

    # Get all students
    just api students

    # Filter students
    just api "students?name=eq.Alice"

    # Embed related resources (e.g. user for each student)
    just api "students?select=*,users(*)"

    # Or with curl directly
    curl -s "http://localhost:3000/students?select=*,users(*)" | python3 -m json.tool

**Resource embedding**

PostgREST automatically detects foreign key relationships through views.
The ``db-extra-search-path`` setting in ``postgrest.conf`` includes the
``public`` and ``private`` schemas so that PostgREST can trace FK
relationships from the API views back to their base tables.

The embedding name matches the **view/table name**, not the column name.
Use an alias to rename the embedded resource::

    # Column is "user_id", but the resource is called "users"
    just api "students?select=*,users(*)"

    # Rename to "user" with an alias
    just api "students?select=*,user:users(*)"

See the `PostgREST resource embedding docs
<https://docs.postgrest.org/en/stable/references/api/resource_embedding.html>`_
for the full query syntax.
