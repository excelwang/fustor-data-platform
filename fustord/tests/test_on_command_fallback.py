import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from fustord.api.views import FallbackDriverWrapper
from fustord import runtime_objects

@pytest.mark.asyncio
async def test_fallback_driver_wrapper_success():
    """Test that wrapper passes through successful calls."""
    mock_driver = AsyncMock()
    mock_driver.get_data_view.return_value = {"status": "ok"}
    
    wrapper = FallbackDriverWrapper(mock_driver, "test-view")
    
    # Mock readiness check to pass
    with patch("fustord.runtime.readiness.check_view_readiness", new_callable=AsyncMock):
        result = await wrapper.get_data_view(path="/")
    
    assert result == {"status": "ok"}
    mock_driver.get_data_view.assert_called_once()


@pytest.mark.asyncio
async def test_fallback_driver_wrapper_triggers_fallback():
    """Test that wrapper checks triggering fallback on exception."""
    mock_driver = AsyncMock()
    # Simulate a failure that IS NOT ViewNotReadyError (generic failure)
    mock_driver.get_data_view.side_effect = Exception("Generic failure")
    
    wrapper = FallbackDriverWrapper(mock_driver, "test-view")
    
    # Mock the registry in runtime_objects
    mock_fallback = AsyncMock(return_value={"fallback": "data"})
    mock_pm = MagicMock()
    
    # Patch readiness check (assume it passed, but driver failed later)
    with patch("fustord.runtime.readiness.check_view_readiness", new_callable=AsyncMock), \
         patch.object(runtime_objects, "on_command_fallback", mock_fallback), \
         patch.object(runtime_objects, "pipe_manager", mock_pm):
         
        result = await wrapper.get_data_view(path="/")
        
        assert result == {"fallback": "data"}
        # Expect call with pipe_manager
        mock_fallback.assert_called_once_with("test-view", {"path": "/"}, mock_pm)


@pytest.mark.asyncio
async def test_fallback_driver_wrapper_no_fallback_configured():
    """Test that wrapper re-raises original error if no fallback is registered."""
    mock_driver = AsyncMock()
    mock_driver.get_data_view.side_effect = ValueError("Original error")
    
    wrapper = FallbackDriverWrapper(mock_driver, "test-view")
    
    # Ensure no fallback is registered
    # Also patch readiness check
    with patch("fustord.runtime.readiness.check_view_readiness", new_callable=AsyncMock), \
         patch.object(runtime_objects, "on_command_fallback", None):
        with pytest.raises(ValueError, match="Original error"):
            await wrapper.get_data_view(path="/")
