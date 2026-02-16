
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from fustord.stability.pipe import FustordPipe
from fustord.stability.session_bridge import PipeSessionBridge, create_session_bridge

@pytest.fixture
def mock_pipe():
    """Create a mock FustordPipe."""
    pipe = MagicMock(spec=FustordPipe)
    pipe.pipe_id = "test-pipe"
    pipe.view_id = "1"
    pipe.view_ids = ["1"]
    pipe._active_sessions = {}
    pipe._leader_session = None
    pipe.config = {"session_timeout_seconds": 30}
    
    # Mock async methods
    pipe.on_session_created = AsyncMock()
    pipe.on_session_closed = AsyncMock()
    
    # Mock get_session_role
    async def get_role(session_id):
        session = pipe._active_sessions.get(session_id)
        return session.get("role", "unknown") if session else "unknown"
    
    pipe.get_session_role = get_role
    
    # Mock get_session_info
    async def get_session_info(session_id):
        session = pipe._active_sessions.get(session_id)
        if session:
            return {**session, "session_id": session_id}
        return None
    
    pipe.get_session_info = get_session_info
    
    async def get_all_sessions():
        return {sid: {**s, "session_id": sid} for sid, s in pipe._active_sessions.items()}
    pipe.get_all_sessions = get_all_sessions
    
    return pipe

@pytest.fixture
def session_bridge(mock_pipe):
    """Create a PipeSessionBridge."""
    return PipeSessionBridge(mock_pipe)

class TestPipeSessionBridgeInit:
    """Test bridge initialization."""
    
    def test_init(self, mock_pipe):
        """Bridge should initialize with pipe."""
        bridge = PipeSessionBridge(mock_pipe)
        assert bridge._pipe is mock_pipe

    @pytest.mark.asyncio
    async def test_close_session(self, session_bridge, mock_pipe):
        """close_session should close the session on the pipe."""
        with patch("fustord.domain.view_state_manager.view_state_manager.unlock_for_session", AsyncMock()), \
             patch("fustord.domain.view_state_manager.view_state_manager.release_leader", AsyncMock()):
            
            mock_pipe.on_session_closed = AsyncMock()
            
            # Close session
            result = await session_bridge.close_session("sess-123")
            
            # Verify pipe was called
            mock_pipe.on_session_closed.assert_called_once_with("sess-123")
            assert result is True

    @pytest.mark.asyncio
    async def test_get_session_info_from_pipe(self, session_bridge, mock_pipe):
        """get_session_info should get info from pipe."""
        mock_pipe._active_sessions["sess-1"] = {
            "role": "leader",
            "task_id": "sensord:sync"
        }
        
        info = await session_bridge.get_session_info("sess-1")
        
        assert info["role"] == "leader"
        assert info["session_id"] == "sess-1"

    @pytest.mark.asyncio
    async def test_get_session_info_not_found(self, session_bridge):
        """get_session_info should return None if not found."""
        info = await session_bridge.get_session_info("nonexistent")
        assert info is None

class TestConvenienceFunction:
    """Test create_session_bridge convenience function."""
    
    def test_create_session_bridge(self, mock_pipe):
        """create_session_bridge should work."""
        bridge = create_session_bridge(mock_pipe)
        assert isinstance(bridge, PipeSessionBridge)
        assert bridge._pipe is mock_pipe
