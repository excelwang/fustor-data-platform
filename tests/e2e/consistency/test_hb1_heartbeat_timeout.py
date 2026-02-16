# tests/e2e/consistency/test_hb1_heartbeat_timeout.py
"""
Test HB1: Heartbeat Timeout - datacastst recovers after session expires naturally.

验证 datacastst 在长时间由于网络或其他原因无法发送心跳，导致会话在 fustord 端超时后，
能够检测到会话过期并自动重新创建会话。
"""
import time
import pytest
import logging

from ..utils import docker_manager
from ..fixtures.constants import (
    CONTAINER_CLIENT_A,
    SESSION_TIMEOUT,
    SESSION_VANISH_TIMEOUT,
    MEDIUM_TIMEOUT,
    POLL_INTERVAL
)

logger = logging.getLogger(__name__)

class TestHeartbeatTimeout:
    """Test datacastst's ability to recover from naturally expired sessions."""

    def test_datacastst_recovers_after_timeout(
        self,
        setup_datacaststs,
        fustord_client
    ):
        """
        Scenario:
          1. datacastst A is running with an active session.
          2. Pause datacastst A container to stop heartbeats.
          3. Wait for session timeout (3s + buffer).
          4. Unpause datacastst A container.
          5. Verify datacastst A detects session loss and creates a new one.
        """
        logger.info("Starting heartbeat timeout recovery test")
        
        # 1. Get current datacastst A session
        sessions = fustord_client.get_sessions()
        datacastst_a = next((s for s in sessions if "client-a" in s.getdatacastcast_id", "")), None)
        assert datacastst_a is not None,datacastcast A must have a session initially"
        
        old_session_id = datacastst_a["session_id"]
        logger.info(f"Initial session ID: {old_session_id}")
        
        # 2. Pause datacastst A to stop everything (including heartbeats)
        logger.info(f"Pausing container {CONTAINER_CLIENT_A}...")
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["sh", "-c", "kill -STOP $(cat /root/.fustor/datacastst.pid)"])
        
        # 3. Wait for session timeout in fustord
        # fustord timeout is SESSION_TIMEOUT. Replace hard sleep with polling.
        logger.info(f"Polling for session {old_session_id} to expire in fustord...")
        
        from ..utils.wait_helpers import wait_for_condition
        
        def is_session_expired():
            sessions = fustord_client.get_sessions()
            return old_session_id not in [s["session_id"] for s in sessions]
            
        wait_for_condition(
            is_session_expired, 
            timeout=SESSION_VANISH_TIMEOUT * 4, 
            fail_msg=f"Session {old_session_id} did not expire within timeout"
        )
        
        # 4. Resume datacastst A
        logger.info(f"Resuming container {CONTAINER_CLIENT_A}...")
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["sh", "-c", "kill -CONT $(cat /root/.fustor/datacastst.pid)"])
        
        # 5. Wait for datacastst A to detect error and recover
        logger.info("Waiting for datacastst A to detect timeout and recover...")
        
        start_wait = time.time()
        new_session_id = None
        
        while time.time() - start_wait < MEDIUM_TIMEOUT:
            # OPTIMIZATION: Check for early failure by reading log file directly
            logs_res = docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["cat", "/root/.fustor/logs/datacastst.log"])
            logs = logs_res.stdout + logs_res.stderr
            
            # Aggressive Fast-Fail (BUT skip known non-fatal exceptions)
            # Ref: Proposal B.1 - Avoid false positives on expected recovery exceptions
            fatal_patterns = ["SyntaxError", "AttributeError", "FATAL", "Unhandled exception", "Traceback (most recent call last)"]
            for pattern in fatal_patterns:
                if pattern in logs:
                    logger.error(f"datacastst A CRITICAL ERROR detected idatacastcast.log:\n{logs}")
                    pytest.fail(f"datacastst A failed with {pattern}")
            
            # Check if process is still alive
            ps_res = docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["ps", "aux"])
            if "datacastst" not in ps_res.stdout and "python" not in ps_res.stdout:
                logger.error(f"datacastst A process DIED during recovery. Logs:\n{logs}")
                pytest.fail("datacastst A process died during recovery")

            sessions = fustord_client.get_sessions()
            datacastst_a_sessions = [s for s in sessions if "client-a" in s.getdatacastcast_id", "")]
            if datacastst_a_sessions:
                new_session_id = datacastst_a_sessions[0]["session_id"]
                if new_session_id != old_session_id:
                    logger.info(f"datacastst A recovered with new session ID: {new_session_id}")
                    break
            time.sleep(POLL_INTERVAL)
            
        assert new_session_id is not None, "datacastst A did not create a new session after timeout"
        assert new_session_id != old_session_id, "datacastst A should have a DIFFERENT session ID"
        
        # 6. Verify Cluster Health (Proposal B.1)
        recovered_sessions = fustord_client.get_sessions()
        recovered_a = next((s for s in recovered_sessions if s["session_id"] == new_session_id), None)
        assert recovered_a is not None
        assert recovered_a.get("role") in ["leader", "follower"], f"Recovered session should have a valid role, got {recovered_a.get('role')}"
        
        leaders = [s for s in recovered_sessions if s.get("role") == "leader"]
        assert len(leaders) >= 1, f"Cluster must have at least one Leader after recovery. Sessions: {recovered_sessions}"
        
        logger.info("✅ Heartbeat timeout recovery verified successfully with strict health checks")
