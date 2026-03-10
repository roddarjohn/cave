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

Fork and clone
--------------

`Fork the repository <https://github.com/roddajohn/cave/fork>`_ on GitHub, then clone your fork::

    git clone https://github.com/<your-username>/cave
    cd cave

Install all dependency groups and activate the virtual environment::

    uv sync --all-groups

Install the pre-commit hooks (runs ruff automatically on every commit)::

    just setup

That's it. You're ready to develop.

Running tests
-------------

cave uses `pytest <https://docs.pytest.org>`_ for testing.

For fast feedback during development, run pytest directly::

    just dev-test

To run the full test suite with tox (installs the package into a clean environment,
matching what CI does)::

    just test

Both commands pass arguments through to pytest::
------------------------------------------------

    just dev-test tests/test_cli.py
    just dev-test -k test_shoot
    just test tests/test_cli.py

Linting and formatting
-----------------------

cave uses `ruff <https://docs.astral.sh/ruff/>`_ for linting and formatting.
It runs automatically as a pre-commit hook, but you can also run it manually::

    just lint

Ruff will check for style issues and verify formatting. To auto-fix and auto-format::

    uv run --group lint ruff check --fix
    uv run --group lint ruff format

Type checking
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
`SECURITY.md <https://github.com/roddajohn/cave/blob/main/SECURITY.md>`_
rather than opening a public issue.
