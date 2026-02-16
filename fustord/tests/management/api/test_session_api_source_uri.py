
import pytest
from unittest.mock import patch, Mock, AsyncMock
from fustord.management.api.session import create_session, CreateSessionPayload
from fustord.management.api.views import list_view_sessions

class MockRequest:
    def __init__(self, client_host="127.0.0.1"):
        self.client = Mock()
        self.client.host = client_host

@pytest.mark.asyncio
async def test_session_source_uri_exposure():
    # Setup
    view_id = "test_view_uri"
    pipe_id = "test_pipe_uri"
    session_id = "test-session-id"
    
    # Mock config
    config = {
        "allow_concurrent_push": True,
        "session_timeout_seconds": 30,
    }
    
    # 1. Mock session info
    mock_session_info = {
        "session_id": session_id,
        "task_id": "task_with_uri",
        "client_ip": "127.0.0.1",
        "source_uri": "file:///tmp/test.txt",
        "created_at": 123456789.0,
        "last_activity": 123456790.0,
        "is_leader": True
    }
    
    # 2. Mock Pipe and PipeManager
    mock_pipe = Mock()
    mock_pipe.pipe_id = pipe_id
    mock_pipe.view_ids = [view_id]
    
    mock_pipe_manager = Mock()
    mock_pipe_manager.get_pipe = Mock(return_value=mock_pipe)
    mock_pipe_manager.list_sessions = AsyncMock(return_value=[mock_session_info])
    
    # Create a mock session info object for create_session return (SessionInfo model)
    from fustor_core.models.states import SessionInfo
    mock_si_obj = SessionInfo(
        session_id=session_id,
        task_id="task_with_uri",
        view_id=pipe_id,
        role="leader",
        created_at=123456789.0,
        last_heartbeat=123456790.0
    )
    mock_si_obj.source_uri = "file:///tmp/test.txt"
    mock_si_obj.audit_interval_sec = 30
    mock_si_obj.sentinel_interval_sec = 10
    
    mock_pipe_manager._on_session_created = AsyncMock(return_value=mock_si_obj)
    
    # Patch dependencies
    with patch('fustord.management.api.session._get_session_config', return_value=config):
        # We need to patch the pipe_manager instance in runtime_objects
        with patch('fustord.stability.runtime_objects.pipe_manager', mock_pipe_manager):
            # 1. Create Session with source_uri
            payload = CreateSessionPayload(
                task_id="task_with_uri",
                client_info={"source_uri": "file:///tmp/test.txt"},
                session_timeout_seconds=30
            )
            request = MockRequest()
            
            # Mock get_pipe_id_from_auth dependency and uuid.uuid4
            with patch('fustord.management.api.session.get_pipe_id_from_auth', return_value=pipe_id):
                with patch('fustord.management.api.session.uuid.uuid4', return_value=session_id):
                    result = await create_session(payload, request, pipe_id=pipe_id)
                    assert result["session_id"] == session_id
                    
                    # 2. List Sessions and verify source_uri
                    list_result = await list_view_sessions(view_id, authorized_view_id=view_id)
                    
                    assert list_result["count"] == 1
                    session_data = list_result["active_sessions"][0]
                    
                    assert session_data["session_id"] == session_id
                    assert session_data["source_uri"] == "file:///tmp/test.txt"
                    assert session_data["client_ip"] == "127.0.0.1"
                    assert session_data["role"] == "leader"
