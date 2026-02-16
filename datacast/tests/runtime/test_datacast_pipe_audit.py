
import pytest
import asyncio
from unittest.mock import MagicMock
from datacast.stability.pipe import DatacastPipe
from datacast.stability.mixins.phases import run_audit_sync
from .mocks import MockSourceHandler, MockSenderHandler

from datacast_core.pipe import PipeState

class TestDatacastPipeAudit:
    
    @pytest.fixture
    def mock_source_audit(self):
        ms = MockSourceHandler()
        # Mock audit iterator to return specific tuple sequence
        ms.get_audit_iterator = MagicMock(return_value=iter([
            (None, {"/path/silent": 100.0}),
            ({"event_type": "update", "path": "/path/active"}, {"/path/active": 200.0})
        ]))
        return ms

    @pytest.fixture
    def datacast_pipe(self, mock_source_audit, mock_sender, pipe_config):
        pipe = DatacastPipe(
            pipe_id="test-audit-pipe",
            config=pipe_config,
            source_handler=mock_source_audit,
            sender_handler=mock_sender
        )
        return pipe

    @pytest.mark.asyncio
    async def test_audit_sync_updates_context(self, datacast_pipe):
        """
        Verify that run_audit_sync updates pipe.audit_context from source iterator items,
        properly handling both Silent (None) entries and Active entries.
        """
        # Set state to RUNNING so loop continues
        datacast_pipe._set_state(PipeState.RUNNING, "Starting test")
        datacast_pipe.session_id = "sess-1"
        
        # Run audit sync directly
        await run_audit_sync(datacast_pipe)
        
        # Verify context updates
        assert "/path/silent" in datacast_pipe.audit_context
        assert datacast_pipe.audit_context["/path/silent"] == 100.0
        
        assert "/path/active" in datacast_pipe.audit_context
        assert datacast_pipe.audit_context["/path/active"] == 200.0
        
        # Verify sender called for active event but NOT for silent one (None)
        # We expect 1 batch of 1 event
        assert len(datacast_pipe.sender_handler.batches_sent) == 1 + 1 # 1 data batch + 1 final signal batch
        rows = datacast_pipe.sender_handler.batches_sent[0]
        assert len(rows) == 1
        assert rows[0]["event_type"] == "update"
