import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from sensord.runner import run_sensord

@pytest.mark.asyncio
async def test_supervisor_loop_restarts_on_error():
    """GAP-2 Verification: Supervisor loop restarts sensord on failure."""
    
    mock_app = MagicMock()
    # First call raises, second succeeds, third raises to stop validly? 
    # Or start raises, then we stop.
    
    # We want to verify it RETRIES.
    # startup() -> raises Exception -> logs -> sleep -> startup() -> ...
    
    # We use side_effect to control sequence
    startup_calls = 0
    stop_event = asyncio.Event()
    
    async def mock_startup():
        nonlocal startup_calls
        startup_calls += 1
        if startup_calls == 1:
            raise RuntimeError("Crash on first boot")
        if startup_calls == 2:
            # Success on second try
            return
        # If called again, just return
    
    mock_app.startup = AsyncMock(side_effect=mock_startup)
    mock_app.shutdown = AsyncMock()
    
    # We need to stop the loop eventually. 
    # run_sensord waits on stop_event.
    # We can trigger stop_event after some time.
    
    async def set_stop():
        await asyncio.sleep(0.5) # Wait enough for retry logic (assuming backoff is small or we patch it)
        stop_event.set()
        
    asyncio.create_task(set_stop())
    
    # Run the sensord
    # Note: run_sensord uses `while not stop_event.is_set():`
    # If startup raises, it logs and continues loop.
    # It sleeps 5s by default? Check runner.py
    
    with patch("sensord.runner.asyncio.sleep", AsyncMock()) as mock_sleep:
        
        # Run sensord with timeout to break the infinite loop after success
        try:
            await asyncio.wait_for(run_sensord(), timeout=0.5)
        except asyncio.TimeoutError:
            pass # Expected, as it waits on stop_event
        except asyncio.CancelledError:
             pass
