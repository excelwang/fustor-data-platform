
import pytest
import asyncio
import time
from unittest.mock import MagicMock, AsyncMock, patch

from fustord_mgmt.on_command import on_command_fallback

@pytest.mark.asyncio
async def test_on_command_fallback_concurrency_limit():
    """
    Test that on_command_fallback respects the concurrency semaphore.
    Simulate 50 pipes, semaphore limit is 10.
    Each task takes 0.1s.
    Total time should be approx (50 / 10) * 0.1s = 0.5s.
    If unlimited, it would be 0.1s.
    """
    
    # Mock pipe manager
    mock_pm = MagicMock()
    pipe_ids = [f"pipe-{i}" for i in range(50)]
    mock_pm.resolve_pipes_for_view.return_value = pipe_ids
    
    # Mock get_pipe and get_bridge to return valid objects
    mock_pipe = MagicMock()
    mock_pipe.leader_session = "sess-1"
    mock_pm.get_pipe.return_value = mock_pipe
    
    mock_bridge = MagicMock()
    mock_pm.get_bridge.return_value = mock_bridge
    
    # Track concurrency
    active_tasks = 0
    max_active = 0
    
    async def slow_command(*args, **kwargs):
        nonlocal active_tasks, max_active
        active_tasks += 1
        max_active = max(max_active, active_tasks)
        await asyncio.sleep(0.1)
        active_tasks -= 1
        return {"success": True}
        
    mock_bridge.send_command_and_wait = slow_command
    
    # Reset semaphore to ensure we use the new limit
    import fustord_mgmt.on_command as oc
    oc._SEMAPHORE = None
    
    # Mock config to return limit 10
    with patch("fustord.config.unified.fustord_config") as mock_config:
        mock_config.fustord.on_command_concurrency_limit = 10
        
        start_time = time.time()
        
        # Execute fallback
        await on_command_fallback("view-1", {}, mock_pm)
        
        duration = time.time() - start_time
        
        # Assertions
        print(f"Max concurrent tasks: {max_active}")
        print(f"Total duration: {duration:.4f}s")
        
        # The semaphore is 10. So max_active should be <= 10.
        # Allow small buffer for loop overhead, but strict equality is usually fine with semaphore.
        assert max_active <= 10, f"Concurrency limit exceeded: {max_active} > 10"
        
        # Check duration to ensure it actually ran in parallel (not serial which would be 5s)
        # and not all at once (which would be 0.1s)
        # 5 runs of 0.1s = 0.5s.
        assert duration >= 0.45, "Ran too fast (concurrency limit likely ignored)"
        assert duration < 2.0, "Ran too slow (likely serial execution)"

from fastapi import HTTPException

@pytest.mark.asyncio
async def test_pipe_manager_not_provided():
    """Test error when pipe_manager is missing."""
    with pytest.raises(HTTPException) as exc:
        await on_command_fallback("view-1", {}, None)
    assert exc.value.status_code == 502
    assert "Pipe manager not provided" in exc.value.detail
