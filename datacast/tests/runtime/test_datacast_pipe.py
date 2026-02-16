# datacast/tests/runtime/test_datacast_pipe.py
"""
Tests for DatacastPipe - Basic unit tests only.

Note: Full lifecycle tests are skipped because they require async task coordination.
Those tests should be done as integration tests with proper timeouts.
"""
import pytest
from datacast_core.pipe import PipeState
from datacast.stability.pipe import DatacastPipe

@pytest.fixture
def datacast_pipe(mock_source, mock_sender, pipe_config):
    return DatacastPipe(
        pipe_id="test-pipe",
        config=pipe_config,
        source_handler=mock_source,
        sender_handler=mock_sender
    )

class TestDatacastPipeInit:
    """Test DatacastPipe initialization."""
    
    def test_initial_state(self, datacast_pipe):
        """Pipe should start in STOPPED state."""
        assert datacast_pipe.state == PipeState.STOPPED
        assert datacast_pipe.session_id is None
        assert datacast_pipe.current_role is None
    
    def test_config_parsing(self, datacast_pipe, pipe_config):
        """Configuration should be parsed correctly."""
        assert datacast_pipe.batch_size == pipe_config["batch_size"]
    
    def test_dto(self, datacast_pipe):
        """get_dto should return pipe info."""
        dto = datacast_pipe.get_dto()
        assert dto.id == "test-pipe"
        assert dto.task_id is None # Not yet resolved
        assert "STOPPED" in str(dto.state)

        assert dto.statistics is not None
    
    def test_is_running_when_stopped(self, datacast_pipe):
        """is_running should return False when stopped."""
        assert not datacast_pipe.is_running()
    
    def test_is_outdated_when_fresh(self, datacast_pipe):
        """is_outdated should return False on fresh pipe."""
        assert not datacast_pipe.is_outdated()


class TestDatacastPipeStateManagement:
    """Test pipestate transitions."""
    
    def test_set_state(self, datacast_pipe):
        """_set_state should update state and info."""
        datacast_pipe._set_state(PipeState.RUNNING, "Test info")
        
        assert datacast_pipe.state == PipeState.RUNNING
        assert datacast_pipe.info == "Test info"
    
    def test_composite_state(self, datacast_pipe):
        """Pipe should support composite states."""
        datacast_pipe._set_state(
            PipeState.RUNNING | PipeState.SNAPSHOT_SYNC
        )
        
        assert datacast_pipe.is_running()
        assert PipeState.SNAPSHOT_SYNC in datacast_pipe.state
    
    def test_str_representation(self, datacast_pipe):
        """__str__ should return readable format."""
        s = str(datacast_pipe)
        assert "test-pipe" in s
        assert "STOPPED" in s
