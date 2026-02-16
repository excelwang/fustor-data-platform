# fustord/tests/runtime/test_fustord_pipe.py
"""
Tests for fustordPipe.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from typing import Any, Dict, List, Optional

from fustor_core.pipe import PipeState
from fustor_core.pipe.handler import ViewHandler
from fustord.runtime import fustordPipe


class MockViewHandler(ViewHandler):
    """Mock view handler for testing."""
    
    schema_name = "mock"
    
    def __init__(self, handler_id: str = "mock-view"):
        super().__init__(handler_id, {})
        self.events_processed: List[Any] = []
        self.session_starts = 0
        self.session_closes = 0
        self.initialized = False
        self._closed = False
        self.next_role = "leader"
    
    async def initialize(self) -> None:
        self.initialized = True
    
    async def close(self) -> None:
        self._closed = True
    
    async def process_event(self, event: Any) -> None:
        self.events_processed.append(event)
    
    async def on_session_start(self, **kwargs) -> None:
        self.session_starts += 1
    
    async def on_session_close(self, **kwargs) -> None:
        self.session_closes += 1
    
    async def resolve_session_role(self, session_id: str, **kwargs) -> Dict[str, Any]:
        from fustord.view_state_manager import view_state_manager
        view_id = self.id
        if self.next_role == "leader":
            is_leader = await view_state_manager.try_become_leader(view_id, session_id)
            return {"role": "leader" if is_leader else "follower"}
        return {"role": "follower"}

    def get_data_view(self, **kwargs) -> Dict[str, Any]:
        return {"events_count": len(self.events_processed)}
    
    def get_stats(self) -> Dict[str, Any]:
        return {"processed": len(self.events_processed)}


@pytest.fixture
def mock_view_handler():
    return MockViewHandler(handler_id="mock-view")


@pytest.fixture
def pipe_config():
    return {
        "view_ids": ["mock-view"],
        "allow_concurrent_push": True,
        "queue_batch_size": 100,
    }


import pytest_asyncio
@pytest_asyncio.fixture
async def fustord_pipe(mock_view_handler, pipe_config):
    from fustord.view_state_manager import view_state_manager
    from fustord.core.session_manager import session_manager
    
    # 清理旧状态
    await view_state_manager.clear_state("mock-view")
    await session_manager.clear_all_sessions("mock-view")
    
    p = fustordPipe(
        pipe_id="test-pipe",
        config=pipe_config,
        view_handlers=[mock_view_handler]
    )
    # Manual injection to adapt to business logic bug - REMOVED: V2 uses .id correctly
    # p.pipe_id = p.id
    return p


class TestfustordPipeInit:
    """Test fustordPipe initialization."""
    
    @pytest.mark.asyncio
    async def test_initial_state(self, fustord_pipe):
        assert fustord_pipe.id == "test-pipe"
        assert fustord_pipe.state == PipeState.STOPPED
        assert fustord_pipe.session_id is None
    
    @pytest.mark.asyncio
    async def test_view_handlers_registered(self, fustord_pipe, mock_view_handler):
        assert "mock-view" in fustord_pipe._view_handlers
        assert fustord_pipe._view_handlers["mock-view"] == mock_view_handler
    
    @pytest.mark.asyncio
    async def test_config_parsing(self, fustord_pipe):
        assert fustord_pipe.view_ids == ["mock-view"]
        assert fustord_pipe.allow_concurrent_push is True
        assert fustord_pipe.queue_batch_size == 100
    
    @pytest.mark.asyncio
    async def test_dto(self, fustord_pipe):
        dto = await fustord_pipe.get_dto() # get_dto is async
        assert dto["id"] == "test-pipe"
        assert dto["state"] == "STOPPED"
        assert dto["view_ids"] == ["mock-view"]


class TestfustordPipeLifecycle:
    """Test fustordPipe start/stop lifecycle."""
    
    @pytest.mark.asyncio
    async def test_start(self, fustord_pipe, mock_view_handler):
        await fustord_pipe.start()
        await fustord_pipe.wait_until_ready()
        assert fustord_pipe.state == PipeState.RUNNING
        assert mock_view_handler.initialized is True
        await fustord_pipe.stop()
    
    @pytest.mark.asyncio
    async def test_stop(self, fustord_pipe, mock_view_handler):
        await fustord_pipe.start()
        await fustord_pipe.wait_until_ready()
        await fustord_pipe.stop()
        assert fustord_pipe.state == PipeState.STOPPED
        assert mock_view_handler._closed is True


class TestfustordPipeSession:
    """Test session management."""
    
    @pytest.mark.asyncio
    async def test_session_created_first_is_leader(self, fustord_pipe, mock_view_handler):
        """First session should become leader."""
        from fustord.runtime.session_bridge import create_session_bridge
        bridge = create_session_bridge(fustord_pipe)
        
        await fustord_pipe.start()
        
        # Use bridge to create session (handles election and backing store)
        await bridge.create_session(task_id="sensord:pipe", session_id="sess-1")
        
        # Give background event loop time to notify handlers
        await asyncio.sleep(0.05)
        
        assert await fustord_pipe.get_session_role("sess-1") == "leader"
        assert mock_view_handler.session_starts == 1
        
        await fustord_pipe.stop()
    
    @pytest.mark.asyncio
    async def test_session_created_second_is_follower(self, fustord_pipe, mock_view_handler):
        """Second session should be follower."""
        from fustord.runtime.session_bridge import create_session_bridge
        bridge = create_session_bridge(fustord_pipe)
        
        await fustord_pipe.start()
        
        await bridge.create_session(task_id="sensord1:pipe", session_id="sess-1")
        
        # Make the next session a follower
        mock_view_handler.next_role = "follower"
        await bridge.create_session(task_id="sensord2:pipe", session_id="sess-2")
        
        # Give background event loop time
        await asyncio.sleep(0.05)
        
        assert await fustord_pipe.get_session_role("sess-1") == "leader"
        assert await fustord_pipe.get_session_role("sess-2") == "follower"
        
        await fustord_pipe.stop()
    
    @pytest.mark.asyncio
    async def test_leader_election_on_close(self, fustord_pipe):
        """New leader should be elected when leader leaves."""
        from fustord.runtime.session_bridge import create_session_bridge
        bridge = create_session_bridge(fustord_pipe)
        
        await fustord_pipe.start()
        
        await bridge.create_session(task_id="sensord1:pipe", session_id="sess-1")
        await bridge.create_session(task_id="sensord2:pipe", session_id="sess-2")
        
        # Close via bridge (which calls pipe.on_session_closed)
        await bridge.close_session("sess-1")
        
        # In V2, promotion happens during heartbeat/keep_alive
        mock_view_handler.next_role = "leader"
        # We must trigger enough heartbeats or set _LEADER_VERIFY_INTERVAL to 1 for test
        bridge._LEADER_VERIFY_INTERVAL = 1
        await bridge.keep_alive("sess-2")
        
        # sess-2 should now be leader
        assert await fustord_pipe.get_session_role("sess-2") == "leader"
        
        await fustord_pipe.stop()


class TestfustordPipeDto:
    """Test data transfer objects."""
    
    @pytest.mark.asyncio
    async def test_aggregated_stats(self, fustord_pipe, mock_view_handler):
        stats = await fustord_pipe.get_aggregated_stats()
        assert "pipe" in stats
        assert "views" in stats
        assert "mock-view" in stats["views"]
    
    @pytest.mark.asyncio
    async def test_str_representation(self, fustord_pipe):
        s = str(fustord_pipe)
        assert "fustordPipe" in s
        assert "test-pipe" in s


class TestfustordPipeViewHandler:
    """Test view handler management."""
    
    @pytest.mark.asyncio
    async def test_register_handler(self, fustord_pipe):
        h2 = MockViewHandler("h2")
        fustord_pipe.register_view_handler(h2)
        assert "h2" in fustord_pipe._view_handlers
    
    @pytest.mark.asyncio
    async def test_get_view(self, fustord_pipe, mock_view_handler):
        # get_view is synchronous
        view = fustord_pipe.get_view("mock-view")
        assert view["events_count"] == 0
    
    @pytest.mark.asyncio
    async def test_get_view_nonexistent(self, fustord_pipe):
        view = fustord_pipe.get_view("nonexistent")
        assert view is None
