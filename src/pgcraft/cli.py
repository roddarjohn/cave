from importlib.metadata import version
from typing import Annotated

import typer

app = typer.Typer()


def _version_callback(value: bool) -> None:  # noqa: FBT001
    """Print version and exit."""
    if value:
        typer.echo(f"pgcraft {version('pgcraft')}")
        raise typer.Exit


@app.callback()
def callback(
    _version: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            "--version",
            "-V",
            help="Show version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Configuration-driven PostgreSQL framework."""


@app.command()
def shoot() -> None:
    """Shoot the portal gun."""
    typer.echo("Shooting portal gun")


@app.command()
def load() -> None:
    """Load the portal gun."""
    typer.echo("Loading portal gun")
