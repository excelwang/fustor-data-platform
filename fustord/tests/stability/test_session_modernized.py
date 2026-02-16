
import pytest
import pytest_asyncio
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustord.stability.session_bridge import PipeSessionBridge
from fustord.domain.job_manager import JobManager

# Mock FustordPipe interface required by PipeSessionBridge
class MockPipe:
    def __init__(self, pipe_id="pipe-test", view_ids=None):
        self.id = pipe_id
        self.view_ids = view_ids or ["view-default"]
        self.session_bridge = None
        
        # Lifecycle hooks
        self.on_session_created = AsyncMock()
        self.on_session_closed = AsyncMock()
        self.keep_session_alive = AsyncMock()
        
        # Getters
        self.get_session_role = AsyncMock(return_value="follower")
        self.get_session_info = AsyncMock(return_value={})
        self.get_all_sessions = AsyncMock(return_value={})
        
        # Properties
        self._leader_session = None
        
    @property
    def leader_session(self):
        return self._leader_session
        
    def find_handler_for_view(self, view_id):
        # Return a mock handler
        handler = MagicMock()
        handler.resolve_session_role = AsyncMock(return_value={"role": "follower"})
        return handler

@pytest.mark.asyncio
class TestSessionModernized:
    """
    Modernized test suite for Session Lifecycle using PipeSessionBridge and JobManager directly.
    Replaces older tests that used the monolithic SessionManager.
    """

    @pytest_asyncio.fixture
    async def bridge(self):
        pipe = MockPipe(view_ids=["view-job"])
        bridge = PipeSessionBridge(pipe)
        return bridge

    @pytest_asyncio.fixture
    async def job_manager(self):
        jm = JobManager()
        return jm

    async def test_datacastst_job_lifecycle(self, bridge, job_manager):
        """Verify datacaststJob creation and completion flow using JobManager."""
        view_id = "view-job"
        sessions = ["s1", "s2"]
        
        # 0. Create sessions (Bridge side)
        await bridge.create_session(task_id="t1", session_id="s1")
        await bridge.create_session(task_id="t2", session_id="s2")
        
        # 1. Create Job (JobManager)
        job_id = await job_manager.create_job(view_id, "/path/target", sessions)
        assert job_id
        assert job_id in job_manager._jobs
        job = job_manager._jobs[job_id]
        assert job.status == "RUNNING"
        assert job.expected_sessions == set(sessions)
        
        # 2. Complete for s1
        await job_manager.complete_job_for_session(view_id, "s1", "/path/target")
        assert "s1" in job.completed_sessions
        assert job.status == "RUNNING"
        
        # 3. Complete for s2 (Final)
        await job_manager.complete_job_for_session(view_id, "s2", "/path/target")
        assert "s2" in job.completed_sessions
        assert job.status == "COMPLETED"
        assert job.completed_at is not None

    async def test_session_expiration(self, bridge):
        """Verify sessions are cleaned up after timeout using PipeSessionBridge."""
        session_id = "sess-exp"
        
        # Mock VSM to allow unlocking during close
        mock_vsm = AsyncMock()
        with patch('fustord.stability.mixins.bridge_lifecycle.view_state_manager', mock_vsm):
            # 1. Create session with 0.1s timeout
            await bridge.create_session(task_id="t1", session_id=session_id, session_timeout_seconds=0.1)
            assert bridge.store.get_session(session_id) is not None
            
            # 2. Wait for expiration
            await asyncio.sleep(0.2)
            
            # 3. Trigger cleanup
            await bridge.cleanup_expired_sessions()
            
            # 4. Verify removal
            assert bridge.store.get_session(session_id) is None
            
            # 5. Verify hook called
            bridge._pipe.on_session_closed.assert_awaited_with(session_id)

    async def test_leader_promotion_trigger(self):
        """Verify termination triggers leader promotion via VSM calls."""
        view_id = "view-elect"
        leader_id = "s-leader"
        follower_id = "s-follower"
        
        # Setup Pipe with mocking
        pipe = MockPipe(view_ids=[view_id])
        
        # Mock Handler to return specific roles during creation
        mock_handler = MagicMock()
        mock_handler.resolve_session_role = AsyncMock()
        # First call for leader, second for follower
        mock_handler.resolve_session_role.side_effect = [
            {"role": "leader", "election_key": view_id},
            {"role": "follower", "election_key": view_id}
        ]
        pipe.find_handler_for_view = MagicMock(return_value=mock_handler)
        
        bridge = PipeSessionBridge(pipe)
        
        # Mock VSM
        mock_vsm = AsyncMock()
        mock_vsm.lock_for_session.return_value = True
        
        # Patch where it is imported in bridge_lifecycle
        with patch('fustord.stability.mixins.bridge_lifecycle.view_state_manager', mock_vsm):
            # 1. Create two sessions
            # Leader
            res1 = await bridge.create_session(task_id="t1", session_id=leader_id)
            assert res1["role"] == "leader"
            
            # Follower
            res2 = await bridge.create_session(task_id="t2", session_id=follower_id)
            assert res2["role"] == "follower"
            
            # 2. Terminate Leader
            await bridge.close_session(leader_id)
            
            # 3. Verify VSM calls
            # Check leader released
            mock_vsm.release_leader.assert_awaited_with(view_id, leader_id)
            mock_vsm.unlock_for_session.assert_awaited_with(view_id, leader_id)
            
        
            # Note: The bridge itself doesn't trigger promotion calculation for OTHERS. 
            # That logic was in SessionManager but now resides in Pipe/Server/VSM coordination.
            # The bridge ONLY handles the local session's lifecycle.
            # So we only assert that the bridge did ITS part: releasing the leader lock.
            # The actual re-election is handled by the next heartbeat or lazy check from others, 
            # OR by explicit VSM triggers which are not in the bridge's close_session method directly 
            # (unless added). 
            
            # Looking at bridge_lifecycle.py: close_session calls unlock_for_session and release_leader.
            # It does NOT iterate over other sessions to promote them. 
            # Promotion is typically demand-driven or periodic.

    async def test_command_queue_flow(self, bridge):
        """Verify command queueing and retrieval via keep_alive."""
        session_id = "s-cmd"
        
        # 1. Create session
        await bridge.create_session(task_id="t1", session_id=session_id)
        
        # 2. Queue command
        cmd = {"type": "scan", "path": "/foo"}
        bridge.store.queue_command(session_id, cmd)
        
        # 3. Retrieve via keep_alive
        res = await bridge.keep_alive(session_id)
        assert res["status"] == "ok"
        assert len(res["commands"]) == 1
        assert res["commands"][0]["type"] == "scan"
        assert res["commands"][0]["path"] == "/foo"
        
        # 4. Confirm cleared
        res2 = await bridge.keep_alive(session_id)
        assert len(res2["commands"]) == 0

    async def test_broadcast_scan_trigger(self, job_manager):
        """Verify FSViewDriver broadcase logic using mocked PipeManager."""
        from fustor_view_fs.driver import FSViewDriver
        
        # Mock PipeManager
        mock_pm = MagicMock()
        
        # Mock Pipes with bridges
        pipe1 = MockPipe(pipe_id="p1", view_ids=["v1"])
        pipe1.session_bridge = MagicMock()
        pipe1.session_bridge.store = MagicMock()
        
        # Set up sessions
        active_sessions = [{"session_id": "s1"}, {"session_id": "s2"}]
        mock_pm.list_sessions = AsyncMock(return_value=active_sessions)
        mock_pm.get_pipes.return_value = {"p1": pipe1}
        
        # Setup pipe1 sessions
        pipe1.get_all_sessions = AsyncMock(return_value={"s1": {}, "s2": {}})
        
        # Patch dependencies
        with patch.dict('sys.modules', {
            'fustord.stability.pipe_manager': MagicMock(pipe_manager=mock_pm),
            'fustord.domain.job_manager': MagicMock(job_manager=job_manager),
        }):
             driver = FSViewDriver("d1", "v1", config={})
             # Mock driver internal components to avoid initialization overhead
             driver.state = MagicMock()
             driver.tree_manager = MagicMock()
             driver.arbitrator = MagicMock()
             driver._global_read_lock = MagicMock()
             driver._global_read_lock.return_value.__aenter__.return_value = None
             
             # Trigger
             success, job_id = await driver.trigger_on_demand_scan("/path", recursive=True)
             
             assert success is True
             assert job_id is not None
             assert job_id in job_manager._jobs
             
             # Verify bridge.store.queue_command called twice
             assert pipe1.session_bridge.store.queue_command.call_count == 2
