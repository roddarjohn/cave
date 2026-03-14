# Install git hooks (run once after cloning)
setup:
    uvx pre-commit install

# Run ruff linter and formatter check
lint:
    uv run --group lint ruff check && uv run --group lint ruff format --check

# Run ty type checker
type-check:
    uv run --group lint ty check src/

# Run tests via tox (full isolation, builds package as sdist)
test *args:
    uv run tox -- {{args}}

# Run tests directly via uv (faster, for local development)
dev-test *args:
    uv run pytest {{args}}

# Run tests with slipcover coverage report (branch + line coverage)
coverage *args:
    uv run python -m slipcover --branch --source src/pgcraft -m pytest {{args}}

# Run coverage and write XML output (used by CI)
coverage-ci *args:
    uv run python -m slipcover --branch --xml --out coverage.xml \
        --source src/pgcraft -m pytest {{args}}

# Build HTML docs for all versions (output in docs/_build/html)
docs: _docs-setup
    uv run python scripts/build_versioned_docs.py

# Serve docs with live reload for editing (http://127.0.0.1:8000)
serve-docs-autoreload: _docs-setup
    uv run --group docs sphinx-autobuild docs docs/_build/html

# Serve the full versioned docs build (http://localhost:8000)
serve-docs-static: docs
    python -m http.server -d docs/_build/html 8000

[private]
_docs-setup:
    #!/usr/bin/env bash
    export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg:///pgcraft}"
    mkdir -p docs/_generated
    just --list | uv run python scripts/just_to_rst.py > docs/_generated/just_commands.rst
    just --justfile playground/Justfile --list | uv run python scripts/just_to_rst.py > docs/_generated/playground_just_commands.rst
    uv run python scripts/generate_dimension_docs.py
