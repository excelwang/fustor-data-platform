import asyncio
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock

from sensord.stability.pipe_manager import PipeManager
from sensord.domain.configs.pipe import PipeConfigService
from sensord.domain.configs.source import SourceConfigService
from sensord.domain.configs.sender import SenderConfigService
from sensord.domain.event_bus import EventBusManager
from sensord.domain.drivers.source_driver import SourceDriverService
from sensord.domain.drivers.sender_driver import SenderDriverService
from sensord_core.models.states import PipeState
from sensord_core.models.config import AppConfig, PipeConfig, SourceConfig, SenderConfig, PasswdCredential
from sensord.config.unified import SensordPipeConfig

@pytest.fixture
def mock_services():
    app_config = AppConfig()
    source_cfg = SourceConfig(driver="fs", uri="/tmp", credential=PasswdCredential(user="u"))
    sender_cfg = SenderConfig(driver="echo", uri="http://loc", credential=PasswdCredential(user="u"))
    pipe_cfg = PipeConfig(source="s1", sender="se1", fields_mapping=[])
    
    app_config.add_source("s1", source_cfg)
    app_config.add_sender("se1", sender_cfg)
    app_config.add_pipe("p1", pipe_cfg)

    source_cfg_svc = SourceConfigService(app_config)
    sender_cfg_svc = SenderConfigService(app_config)
    pipe_cfg_svc = PipeConfigService(app_config, source_cfg_svc, sender_cfg_svc)
    
    source_dr_svc = SourceDriverService()
    sender_dr_svc = SenderDriverService()
    
    # Create a mock for EventBusInstanceRuntime
    mock_bus_instance_runtime = MagicMock()
    mock_bus_instance_runtime.id = "mock-bus-id"

    bus_mgr = EventBusManager(source_configs={"s1": source_cfg}, source_driver_service=source_dr_svc)
    bus_mgr.get_or_create_bus_for_subscriber = AsyncMock(return_value=(mock_bus_instance_runtime, False))
    
    # Patch discovery before creating services
    with patch.object(SourceDriverService, "_discover_installed_drivers", return_value={}), \
         patch.object(SenderDriverService, "_discover_installed_drivers", return_value={}):
        
        with patch("sensord.stability.pipe_manager.SensordPipe") as mock_pipe_cls:
            mock_pipe_instance = AsyncMock(id="p1", state=PipeState.STARTING)
            mock_pipe_cls.return_value = mock_pipe_instance
            
            # Mock get_config on instances because SourceConfigService uses global sensord_config
            # and we want to bypass that for this integration test of PipeManager
            source_cfg_svc.get_config = MagicMock(side_effect=lambda id: source_cfg if id == "s1" else None)
            sender_cfg_svc.get_config = MagicMock(side_effect=lambda id: sender_cfg if id == "se1" else None)

            service = PipeManager(
                pipe_config_service=pipe_cfg_svc,
                source_config_service=source_cfg_svc,
                sender_config_service=sender_cfg_svc,
                bus_manager=bus_mgr,
                sender_driver_service=MagicMock(),
                source_driver_service=MagicMock()
            )
            # Inject bus manager dependency
            bus_mgr.set_dependencies(service)
            service.source_driver_service._get_driver_by_type = MagicMock(return_value=MagicMock())
            service.sender_driver_service._get_driver_by_type = MagicMock(return_value=MagicMock())
    return service, pipe_cfg_svc

@pytest.mark.asyncio
async def test_pipe_service_restart_outdated_pipes(mock_services):
    """Verify that restart_outdated_pipes stops existing and starts new instance."""
    service, pipe_cfg_svc = mock_services
    
    mock_pipe_instance_1 = AsyncMock()
    mock_pipe_instance_1.id = "p1"
    mock_pipe_instance_1.task_id = "test-sensord:p1"
    mock_pipe_instance_1.state = MagicMock()
    mock_pipe_instance_1.info = ""
    mock_pipe_instance_1.bus = MagicMock(id="mock-bus-id-1")


    mock_pipe_instance_2 = AsyncMock()
    mock_pipe_instance_2.id = "p1"
    mock_pipe_instance_2.task_id = "test-sensord:p1"
    mock_pipe_instance_2.state = MagicMock()
    mock_pipe_instance_2.info = ""
    mock_pipe_instance_2.bus = MagicMock(id="mock-bus-id-2")


    with patch("sensord.domain.configs.pipe.sensord_config") as mock_sensord_config, \
         patch("sensord.stability.pipe_manager.SensordPipe", side_effect=[mock_pipe_instance_1, mock_pipe_instance_2]) as MockSensordPipeClass:
        
        sensord_pipe_config = SensordPipeConfig(source="s1", sender="se1")
        mock_sensord_config.get_pipe.return_value = sensord_pipe_config
        
        # 1. Start initially
        await service.start_one("p1")
        assert "p1" in service.pool
        first_instance = service.pool["p1"]
        
        # 2. Mark as outdated
        await service.mark_dependent_pipes_outdated("source", "s1", "config changed")
        
        # 3. Restart outdated
        count = await service.restart_outdated_pipes()
        assert count == 1
        
        # Verify first was stopped
        first_instance.stop.assert_called_once()
        
        # Verify "p1" is still in pool (new one started)
        assert "p1" in service.pool
        assert service.pool["p1"] is not first_instance
        assert MockSensordPipeClass.call_count == 2

@pytest.mark.asyncio
async def test_pipe_service_stop_all_cleans_up(mock_services):
    """Stop all should close bus service and stop all pipes."""
    service, _ = mock_services
    # The attribute is bus_manager, not bus_service
    # But we want to verify calls on the existing bus_manager mock from fixture, or replace it.
    # The fixture sets bus_mgr. Let's spy on it or mock methods on it.
    
    # We can just use the existing bus_manager from the service
    service.bus_manager.get_or_create_bus_for_subscriber = AsyncMock(return_value=(MagicMock(id="mock-bus-for-stop-all"), False))
    service.bus_manager.release_all_unused_buses = AsyncMock() 

    mock_pipe_instance = AsyncMock() # Single mock instance
    mock_pipe_instance.id = "p1"
    mock_pipe_instance.bus = MagicMock(id="mock-bus-id-1") # Ensure bus attribute exists

    with patch("sensord.domain.configs.pipe.sensord_config") as mock_sensord_config, \
         patch("sensord.stability.pipe_manager.SensordPipe", return_value=mock_pipe_instance) as MockSensordPipeClass:
        
        sensord_pipe_config = SensordPipeConfig(source="s1", sender="se1")
        mock_sensord_config.get_pipe.return_value = sensord_pipe_config

        await service.start_one("p1")
        
        # Capture the instance before stop_all removes it from the pool
        captured_instance = service.pool["p1"]

        await service.stop_all()
            
        captured_instance.stop.assert_called_once()
        service.bus_manager.release_all_unused_buses.assert_called_once()
        assert len(service.pool) == 0
