# Install git hooks (run once after cloning)
setup:
    uvx pre-commit install

# Run ruff linter and formatter check
lint:
    uv run --group lint ruff check
    uv run --group lint ruff format --check

# Run ty type checker
type-check:
    uv run --group lint ty check

# Run tests via tox (full isolation, builds package as sdist)
test *args:
    uvx --with tox-uv tox -- {{args}}

# Run tests directly via uv (faster, for local development)
dev-test *args:
    uv run pytest {{args}}

# Build HTML docs (output in docs/_build/html)
docs:
    just --list > docs/_generated/just_commands.txt
    uv run --group docs sphinx-build -b html docs docs/_build/html

# Serve docs locally with live reload (http://127.0.0.1:8000)
serve-docs:
    just --list > docs/_generated/just_commands.txt
    uv run --group docs sphinx-autobuild docs docs/_build/html
