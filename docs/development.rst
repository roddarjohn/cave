Development
===========

This guide explains how to set up a local development environment for **pgcraft**,
run tests, lint code, and build the documentation.

Prerequisites
-------------

You will need the following tools installed:

* `Python 3.10+ <https://www.python.org/downloads/>`_
* `uv <https://docs.astral.sh/uv/>`_ — dependency management and virtual environments
* `just <https://just.systems>`_ — command runner

Install ``uv`` by following the
`uv installation instructions <https://docs.astral.sh/uv/getting-started/installation/>`_
— prefer whatever method is listed as current there.

Install ``just`` by following the
`just installation instructions <https://just.systems/man/en/packages.html>`_
— prefer whatever method is listed as current there.

Fork and clone
--------------

`Fork the repository <https://github.com/roddarjohn/pgcraft/fork>`_ on GitHub, then clone your fork::

    git clone https://github.com/<your-username>/pgcraft
    cd pgcraft

Install all dependency groups and activate the virtual environment::

    uv sync --all-groups

Install the pre-commit hooks (runs ruff automatically on every commit)::

    just setup

That's it. You're ready to develop.

Running tests
-------------

pgcraft uses `pytest <https://docs.pytest.org>`_ for testing.

Tests are organised into three directories that mirror the source tree under
``src/pgcraft/``:

``tests/unit/``
    Pure Python tests with no database dependency.  These run instantly and
    cover things like factory configuration logic and template rendering.
    Fixtures that require a live database are intentionally unavailable here —
    pytest will error if a unit test accidentally references one.

``tests/integration/``
    Tests that exercise real PL/pgSQL behaviour against a live PostgreSQL
    instance.  Each test runs inside a transaction that is rolled back on
    teardown, so **nothing is left in the database** after the suite finishes.
    These tests require ``DATABASE_URL`` to be set; they skip automatically
    when it is absent.

``tests/migrations/``
    `pytest-alembic <https://pytest-alembic.readthedocs.io>`_ tests that
    verify the migration history is consistent and round-trips cleanly.

For fast feedback during development, run pytest directly::

    just dev-test

To run the full test suite with tox (installs the package into a clean environment,
matching what CI does)::

    just test

Both commands pass arguments through to pytest::

    just dev-test tests/unit
    just dev-test tests/integration
    just dev-test -k test_shoot
    just test tests/unit

.. note::

   Integration tests require a PostgreSQL instance.  Set ``DATABASE_URL``
   before running them, for example::

       DATABASE_URL=postgresql+psycopg://postgres@localhost/pgcraft just dev-test tests/integration

Coverage
--------

pgcraft uses `slipcover <https://github.com/plasma-umass/slipcover>`_ for
coverage reporting::

    just coverage

This runs the full pytest suite under slipcover and prints a per-file
coverage table to the terminal.  Pass any pytest arguments to narrow the
scope::

    just coverage tests/unit
    just coverage tests/integration

Benchmarks
----------

pgcraft ships with a performance benchmark suite built on
`pytest-benchmark <https://pytest-benchmark.readthedocs.io/>`_.
Benchmarks exercise the trigger-based API views for each dimension type
against a real PostgreSQL instance.

Benchmarks are **excluded** from normal test runs (``just dev-test`` and
``just test``) via the ``addopts = "--ignore=tests/benchmarks"`` setting in
``pyproject.toml``.  Run them explicitly with::

    just bench

You can pass any ``pytest-benchmark`` flags through::

    # Save results for later comparison
    just bench --benchmark-save=baseline

    # Compare against a saved baseline
    just bench --benchmark-compare=0001_baseline

    # Only run ledger benchmarks
    just bench -k ledger

Each benchmark row in the output table shows:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Column
     - Meaning
   * - **Min**
     - Fastest observed round
   * - **Max**
     - Slowest observed round
   * - **Mean**
     - Arithmetic mean across all rounds
   * - **StdDev**
     - Standard deviation (lower is more consistent)
   * - **Median**
     - Middle value (robust to outliers)
   * - **Rounds**
     - Number of times the function was called

See :doc:`benchmarks` for the full list of benchmarks and representative
results.

Linting and formatting
-----------------------

pgcraft uses `ruff <https://docs.astral.sh/ruff/>`_ for linting and formatting.
It runs automatically as a pre-commit hook, but you can also run it manually::

    just lint

Ruff will check for style issues and verify formatting. To auto-fix and auto-format::

    uv run --group lint ruff check --fix
    uv run --group lint ruff format

Type checking
-------------

pgcraft uses `ty <https://github.com/astral-sh/ty>`_ for type checking::

    just type-check

Documentation
-------------

The docs are built with `Sphinx <https://www.sphinx-doc.org>`_ using the
`Furo <https://pradyunsg.me/furo/>`_ theme.

The doc build generates schema diagrams and query examples from a live
PostgreSQL database, so ``DATABASE_URL`` must be set. If it is not set,
the build defaults to ``postgresql+psycopg:///pgcraft``.

To build the docs::

    just docs

To serve the docs locally with live reload at ``http://localhost:8000``::

    just serve-docs

Or with an explicit database URL::

    DATABASE_URL=postgresql+psycopg://localhost/pgcraft just serve-docs

The docs will automatically rebuild whenever you save a file.

Commands reference
------------------

.. include:: _generated/just_commands.rst

Contributing
------------

Contributions are welcome. Fork the repository, make your changes with tests
where applicable, verify the test suite and linter pass (see the sections
above), then open a pull request against ``main``.

Type annotations are required on all public API.

To report a bug, open a GitHub issue with a minimal reproduction case, what
you expected, what happened, and your Python version and OS.

For security vulnerabilities, see
`SECURITY.md <https://github.com/roddarjohn/pgcraft/blob/main/SECURITY.md>`_
rather than opening a public issue.
