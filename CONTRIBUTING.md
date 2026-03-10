# Contributing

Thank you for your interest in contributing to cave!

## Getting started

See the [development guide](docs/development.rst) for instructions on setting up a local development environment, running tests, and building documentation.

The short version:

```bash
uv sync --all-groups
just setup
```

## Submitting changes

1. Fork the repository and create a branch for your change.
2. Make your changes, add tests if applicable.
3. Run the test suite and linter:

   ```bash
   just dev-test
   just lint
   just type-check
   ```

4. Open a pull request against `main`. Describe what you changed and why.

## Reporting bugs

Open an issue on GitHub. Include:

- A minimal reproduction case
- What you expected to happen
- What actually happened
- Python version and OS

## Code style

cave uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting. It runs automatically as a pre-commit hook after `just setup`. CI will fail if linting or formatting checks do not pass.

Type annotations are required for all public API. cave uses [ty](https://github.com/astral-sh/ty) for type checking.

## Questions

Open an issue if you have a question about contributing or the codebase.
