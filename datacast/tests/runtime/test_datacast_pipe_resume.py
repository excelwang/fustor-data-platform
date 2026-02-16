
import pytest
import asyncio
from unittest.mock import AsyncMock
from datacast.stability.pipe import DatacastPipe
from .mocks import MockSourceHandler, MockSenderHandler

class TestDatacastPipeResume:
    
    @pytest.fixture
    def mock_sender_with_index(self):
        sender = MockSenderHandler()
        sender.get_latest_committed_index = AsyncMock(return_value=12345)
        return sender

    @pytest.fixture
    def datacast_pipe(self, mock_source, mock_sender_with_index, pipe_config):
        return DatacastPipe(
            pipe_id="test-resume-pipe",
            config=pipe_config,
            source_handler=mock_source,
            sender_handler=mock_sender_with_index
        )

    @pytest.mark.asyncio
    async def test_resume_from_committed_index(self, datacast_pipe, mock_sender_with_index):
        """
        Verify that the pipe initializes its statistics with the 
        committed index fetched from fustord on startup.
        """
        # Start pipe
        await datacast_pipe.start()
        
        # Wait for session creation (control loop iteration)
        await asyncio.sleep(0.1)
        
        # Verify call was made
        mock_sender_with_index.get_latest_committed_index.assert_called_once()
        
        # Verify stats updated
        assert datacast_pipe.statistics["last_pushed_event_id"] == 12345
        
        # Cleanup
        await datacast_pipe.stop()

    @pytest.mark.asyncio
    async def test_resume_does_not_overwrite_newer_local_stats(self, datacast_pipe, mock_sender_with_index):
        """
        Verify that if local stats are newer (e.g. from previous run in same process), 
        we don't overwrite with older server index.
        """
        # Simulate local progress
        datacast_pipe.statistics["last_pushed_event_id"] = 99999
        mock_sender_with_index.get_latest_committed_index.return_value = 100
        
        await datacast_pipe.start()
        await asyncio.sleep(0.1)
        
        # Should persist local value
        assert datacast_pipe.statistics["last_pushed_event_id"] == 99999
        
        await datacast_pipe.stop()
