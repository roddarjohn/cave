# Run tests via tox (full isolation, builds package as sdist)
test *args:
    uvx --with tox-uv tox -- {{args}}

# Run tests directly via uv (faster, for local development)
dev-test *args:
    uv run pytest {{args}}
