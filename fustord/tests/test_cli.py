import pytest
from click.testing import CliRunner
from fustord.cli import cli
from unittest.mock import patch, MagicMock

def test_fustord_cli_list_pipes():
    runner = CliRunner()
    mock_pipes = {"pipe1": MagicMock(views=["view1"], receiver="recv1")}
    with patch("fustord.config.unified.fustord_config.get_all_pipes", return_value=mock_pipes):
        with patch("fustord.config.unified.fustord_config.get_view", return_value=MagicMock(disabled=False)):
            with patch("fustord.config.unified.fustord_config.get_receiver", return_value=MagicMock(disabled=False)):
                result = runner.invoke(cli, ["list"])
                assert result.exit_code == 0
                assert "pipe1" in result.output
                assert "ENABLED" in result.output

def test_fustord_cli_status_stopped():
    runner = CliRunner()
    with patch("fustord.cli._is_running", return_value=False):
        result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "STOPPED" in result.output

def test_fustord_cli_status_running():
    runner = CliRunner()
    with patch("fustord.cli._is_running", return_value=5678):
        result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "RUNNING (PID: 5678)" in result.output

def test_fustord_cli_reload_not_running():
    runner = CliRunner()
    with patch("os.path.exists", return_value=False):
        result = runner.invoke(cli, ["reload"])
        assert "fustord is not running" in result.output
