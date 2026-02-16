"""
Test cases for session clear functionality.
"""
import asyncio
from unittest.mock import patch, Mock, MagicMock, AsyncMock
import pytest

from fustord.management.api.session import create_session
from fustord.stability.session_manager import session_manager
from fustord.domain.view_state_manager import view_state_manager
from fustord.stability.pipe import FustordPipe
from fustord.stability.session_bridge import create_session_bridge
from fustord import runtime_objects


class MockRequest:
    def __init__(self, client_host="127.0.0.1"):
        self.client = Mock()
        self.client.host = client_host


async def setup_dummy_pipe(view_id: str, allow_concurrent_push: bool = False):
    """Register a dummy pipe in the global manager for API tests."""
    if not runtime_objects.pipe_manager:
        from fustord.stability.pipe_manager import FustordPipeManager
        runtime_objects.pipe_manager = FustordPipeManager()
    
    mock_handler = Mock()
    mock_handler.id = "dummy-handler"
    mock_handler.view_id = view_id
    mock_handler.resolve_session_role = AsyncMock(return_value={"role": "leader"})
    
    pipe = FustordPipe(
        pipe_id=view_id,
        config={"view_ids": [view_id], "allow_concurrent_push": allow_concurrent_push},
        view_handlers=[mock_handler]
    )
    pipe._handlers_ready.set()
    runtime_objects.pipe_manager._pipes[view_id] = pipe
    
    bridge = create_session_bridge(pipe)
    runtime_objects.pipe_manager._bridges[view_id] = bridge
    return pipe


def make_session_config(allow_concurrent_push=False, session_timeout_seconds=30):
    """Create a mock session config dict."""
    return {
        "allow_concurrent_push": allow_concurrent_push,
        "session_timeout_seconds": session_timeout_seconds,
    }


@pytest.mark.asyncio
async def test_clear_all_sessions():
    """
    Test that clear_all_sessions properly removes all sessions and releases locks
    """
    # Use runtime_objects.pipe_manager instead of session_manager
    if not runtime_objects.pipe_manager:
        from fustord.stability.pipe_manager import FustordPipeManager
        runtime_objects.pipe_manager = FustordPipeManager()
    
    pm = runtime_objects.pipe_manager
    view_state_manager._states.clear()
    
    view_id = "7"
    pipe = await setup_dummy_pipe(view_id, allow_concurrent_push=False)
    
    config = make_session_config(allow_concurrent_push=False, session_timeout_seconds=30)
    
    with patch('fustord.management.api.session._get_session_config', return_value=config):
        # Create a session
        payload = type('CreateSessionPayload', (), {})()
        payload.task_id = "task_to_clear"
        payload.session_timeout_seconds = None
        payload.client_info = None
        request = MockRequest(client_host="192.168.1.21")
        
        result = await create_session(payload, request, view_id)
        session_id = result["session_id"]
        
        # Verify session exists in pipe
        sessions = await pipe.get_all_sessions()
        assert session_id in sessions
        assert await view_state_manager.is_locked_by_session(view_id, session_id)
        
        # Clear all sessions for this view via PipeManager
        await pm.clear_all_sessions(view_id)
        
        # Verify session is gone from pipe
        sessions = await pipe.get_all_sessions()
        assert session_id not in sessions
        
        # Verify lock is released (bridge.close_session should handle it)
        assert not await view_state_manager.is_locked_by_session(view_id, session_id)
