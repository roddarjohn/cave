Development
===========

This guide explains how to set up a local development environment for **cave**,
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

Fork and Clone
--------------

`Fork the repository <https://github.com/roddajohn/cave/fork>`_ on GitHub, then clone your fork::

    git clone https://github.com/<your-username>/cave
    cd cave

Setting Up
----------

Install all dependency groups and activate the virtual environment::

    uv sync --all-groups

Install the pre-commit hooks (runs ruff automatically on every commit)::

    just setup

That's it. You're ready to develop.

Running Tests
-------------

cave uses `pytest <https://docs.pytest.org>`_ for testing.

For fast feedback during development, run pytest directly::

    just dev-test

To run the full test suite with tox (installs the package into a clean environment,
matching what CI does)::

    just test

Both commands pass arguments through to pytest::

    just dev-test tests/test_cli.py
    just dev-test -k test_shoot
    just test tests/test_cli.py

Linting and Formatting
-----------------------

cave uses `ruff <https://docs.astral.sh/ruff/>`_ for linting and formatting.
It runs automatically as a pre-commit hook, but you can also run it manually::

    just lint

Ruff will check for style issues and verify formatting. To auto-fix and auto-format::

    uv run --group lint ruff check --fix
    uv run --group lint ruff format

Type Checking
-------------

cave uses `ty <https://github.com/astral-sh/ty>`_ for type checking::

    just type-check

Documentation
-------------

The docs are built with `Sphinx <https://www.sphinx-doc.org>`_ using the
`Furo <https://pradyunsg.me/furo/>`_ theme.

To build the docs::

    just docs

To serve the docs locally with live reload at ``http://127.0.0.1:8000``::

    just serve-docs

The docs will automatically rebuild whenever you save a file.

Commands Reference
------------------

.. include:: _generated/just_commands.rst
