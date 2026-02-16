# sensord/tests/runtime/test_sensord_pipe.py
"""
Tests for sensordPipe - Basic unit tests only.

Note: Full lifecycle tests are skipped because they require async task coordination.
Those tests should be done as integration tests with proper timeouts.
"""
import pytest
from fustor_core.pipe import PipeState
from sensord.runtime.sensord_pipe import sensordPipe

@pytest.fixture
def sensord_pipe(mock_source, mock_sender, pipe_config):
    return sensordPipe(
        pipe_id="test-pipe",
        config=pipe_config,
        source_handler=mock_source,
        sender_handler=mock_sender
    )

class TestsensordPipeInit:
    """Test sensordPipe initialization."""
    
    def test_initial_state(self, sensord_pipe):
        """Pipe should start in STOPPED state."""
        assert sensord_pipe.state == PipeState.STOPPED
        assert sensord_pipe.session_id is None
        assert sensord_pipe.current_role is None
    
    def test_config_parsing(self, sensord_pipe, pipe_config):
        """Configuration should be parsed correctly."""
        assert sensord_pipe.batch_size == pipe_config["batch_size"]
    
    def test_dto(self, sensord_pipe):
        """get_dto should return pipe info."""
        dto = sensord_pipe.get_dto()
        assert dto.id == "test-pipe"
        assert dto.task_id is None # Not yet resolved
        assert "STOPPED" in str(dto.state)

        assert dto.statistics is not None
    
    def test_is_running_when_stopped(self, sensord_pipe):
        """is_running should return False when stopped."""
        assert not sensord_pipe.is_running()
    
    def test_is_outdated_when_fresh(self, sensord_pipe):
        """is_outdated should return False on fresh pipe."""
        assert not sensord_pipe.is_outdated()


class TestsensordPipeStateManagement:
    """Test pipestate transitions."""
    
    def test_set_state(self, sensord_pipe):
        """_set_state should update state and info."""
        sensord_pipe._set_state(PipeState.RUNNING, "Test info")
        
        assert sensord_pipe.state == PipeState.RUNNING
        assert sensord_pipe.info == "Test info"
    
    def test_composite_state(self, sensord_pipe):
        """Pipe should support composite states."""
        sensord_pipe._set_state(
            PipeState.RUNNING | PipeState.SNAPSHOT_SYNC
        )
        
        assert sensord_pipe.is_running()
        assert PipeState.SNAPSHOT_SYNC in sensord_pipe.state
    
    def test_str_representation(self, sensord_pipe):
        """__str__ should return readable format."""
        s = str(sensord_pipe)
        assert "test-pipe" in s
        assert "STOPPED" in s
