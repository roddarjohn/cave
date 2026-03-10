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

Configuration
-------------

Copy the example env and set your database URL:

.. code-block:: bash

    cp playground/.env.example playground/.env

Edit ``playground/.env`` with your connection details:

.. code-block:: bash

    # Docker or system postgres with no password:
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

``just init``
    Create the database and apply all migrations. Safe to re-run.

``just migrate``
    Apply all pending migrations.

``just rollback``
    Roll back the most recent migration.

``just revision "describe your change"``
    Auto-generate a new migration from the current state of ``models.py``.
    Always review the generated file before applying it.

``just history``
    Show the full migration history.

``just current``
    Show which migration the database is currently at.

``just downgrade <revision>``
    Downgrade to a specific revision ID, e.g. ``just downgrade abc123``.

``just db-shell``
    Open a ``psql`` shell connected to the database using ``DATABASE_URL``.

Typical Workflow
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

Adding Models
-------------

Define new models in ``playground/models.py`` by extending ``Base``::

    class Post(Base):
        __tablename__ = "posts"

        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        title: Mapped[str] = mapped_column(String, nullable=False)

Then generate and apply a migration::

    just revision "add posts table"
    just migrate

Alembic autogenerate compares your models against the live database schema, so
it will pick up additions, removals, and column changes automatically.
