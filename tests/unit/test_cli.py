"""Unit tests for the pgcraft CLI."""

from typer.testing import CliRunner

from pgcraft.cli import app

runner = CliRunner()


def test_generate_schema_missing_args():
    result = runner.invoke(app, ["generate-schema"])
    assert result.exit_code != 0


def test_generate_schema_bad_config(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{not valid json}")
    result = runner.invoke(
        app,
        [
            "generate-schema",
            str(bad),
            "--schema",
            "s",
            "--database-url",
            "postgresql+psycopg://localhost/x",
        ],
    )
    assert result.exit_code != 0
