import pytest
import asyncio
import os
import shutil
from unittest.mock import MagicMock, AsyncMock, patch
from sensord.app import App
from sensord.config.unified import sensord_config
import socket

@pytest.fixture
def mock_config_dir(tmp_path):
    config_dir = tmp_path / "sensord-config"
    config_dir.mkdir()
    
    with open(config_dir / "default.yaml", "w") as f:
        f.write("""
sources:
  src1:
    driver: fs
    uri: "/tmp"
senders:
  snd1:
    driver: fustord
    uri: "http://localhost"
pipes:
  pipe1:
    source: src1
    sender: snd1
""")
    
    with open(config_dir / "other.yaml", "w") as f:
        f.write("""
pipes:
  pipe2:
    source: src1
    sender: snd1
""")
    
    return config_dir

@pytest.mark.asyncio
async def test_app_resolve_target_pipes(mock_config_dir, tmp_path):
    with patch("sensord.app.get_fustor_home_dir", return_value=tmp_path):
        with patch.object(sensord_config, "dir", mock_config_dir):
            sensord_config.reload()
            # Default: only from default.yaml
            app = App()
            assert app._target_pipe_ids == ["pipe1"]
            
            # Explicit pipe ID
            app2 = App(config_list=["pipe2"])
            assert "pipe2" in app2._target_pipe_ids
            
            # Explicit file
            app3 = App(config_list=["other.yaml"])
            assert "pipe2" in app3._target_pipe_ids

@pytest.mark.asyncio
async def test_app_startup_shutdown(mock_config_dir, tmp_path):
    with patch("sensord.app.get_fustor_home_dir", return_value=tmp_path):
        with patch.object(sensord_config, "dir", mock_config_dir):
            sensord_config.reload()
            app = App()
            
            # Mock drivers and instances to avoid actual IO
            app.source_driver_service = MagicMock()
            app.sender_driver_service = MagicMock()
            
            mock_bus = MagicMock()
            mock_bus.source_driver_instance = MagicMock()
            app.event_bus_service = AsyncMock()
            app.event_bus_service.get_or_create_bus_for_subscriber.return_value = (mock_bus, 0)
            
            mock_pipe = AsyncMock()
            mock_pipe.id = "pipe1" # Mock attribute access
            mock_pipe.state = MagicMock() # Mock state
            mock_pipe.info = "" # Mock info
            mock_pipe.task_id = "test-sensord:pipe1" # Mock task_id
            mock_pipe.bus = MagicMock(id="mock-bus-id") # Mock bus attribute

            with patch("sensord.app.SourceHandlerAdapter"), patch("sensord.app.SenderHandlerAdapter"):
                with patch("sensord.runtime.sensord_pipe.SensordPipe", return_value=mock_pipe):
                    await app.startup()
                    assert "pipe1" in app.pipe_runtime
                    mock_pipe.start.assert_called_once()
                    
                    await app.shutdown()
                    assert "pipe1" not in app.pipe_runtime
                    mock_pipe.stop.assert_called_once()

@pytest.mark.asyncio
async def test_app_reload_config(mock_config_dir, tmp_path):
    with patch("sensord.app.get_fustor_home_dir", return_value=tmp_path):
        with patch.object(sensord_config, "dir", mock_config_dir):
            sensord_config.reload()
            app = App()
            app.source_driver_service = MagicMock()
            app.sender_driver_service = MagicMock()
            
            mock_bus = MagicMock()
            mock_bus.source_driver_instance = MagicMock()
            app.event_bus_service = AsyncMock()
            app.event_bus_service.get_or_create_bus_for_subscriber.return_value = (mock_bus, 0)
            
            mock_pipe1 = AsyncMock()
            mock_pipe1.id = "pipe1"
            mock_pipe1.state = MagicMock()
            mock_pipe1.info = ""
            mock_pipe1.task_id = "test-sensord:pipe1"
            mock_pipe1.bus = MagicMock(id="mock-bus-id-1")

            mock_pipe2 = AsyncMock()
            mock_pipe2.id = "pipe2"
            mock_pipe2.state = MagicMock()
            mock_pipe2.info = ""
            mock_pipe2.task_id = "test-sensord:pipe2"
            mock_pipe2.bus = MagicMock(id="mock-bus-id-2")
            
            def pipe_side_effect(pipe_id, **kwargs):
                if pipe_id == "pipe1": return mock_pipe1
                if pipe_id == "pipe2": return mock_pipe2
                return AsyncMock()

            with patch("sensord.app.SourceHandlerAdapter"), patch("sensord.app.SenderHandlerAdapter"):
                with patch("sensord.runtime.sensord_pipe.SensordPipe", side_effect=pipe_side_effect):
                    await app.startup()
                    assert "pipe1" in app.pipe_runtime
                    
                    with patch.object(sensord_config, "get_diff", return_value={"added": {"pipe2"}, "removed": {"pipe1"}}):
                        # Force signature change to trigger reload
                        with patch.object(sensord_config, "get_config_signature", return_value="new_signature"):
                            await app.reload_config()
                        
                        assert "pipe1" not in app.pipe_runtime
                        assert "pipe2" in app.pipe_runtime
                        mock_pipe1.stop.assert_called_once()
                        mock_pipe2.start.assert_called_once()
