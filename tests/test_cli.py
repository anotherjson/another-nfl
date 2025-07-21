import pytest
from click.testing import CliRunner
from cli import cli

def test_list_datasets():
    runner = CliRunner()
    result = runner.invoke(cli, ['list-datasets'])
    assert result.exit_code == 0
    assert "Available dataset types:" in result.output

def test_get_pbp():
    runner = CliRunner()
    result = runner.invoke(cli, ['get-pbp', '2023'])
    assert result.exit_code == 0
    assert "game_id" in result.output

def test_read_parquet():
    runner = CliRunner()
    result = runner.invoke(cli, ['read-parquet', 'dummy.parquet'])
    assert result.exit_code == 0
    assert "a" in result.output
