from typer.testing import CliRunner

from cave.cli import app

runner = CliRunner()


def test_shoot():
    result = runner.invoke(app, ["shoot"])
    assert result.exit_code == 0
    assert "Shooting portal gun" in result.output


def test_load():
    result = runner.invoke(app, ["load"])
    assert result.exit_code == 0
    assert "Loading portal gun" in result.output
