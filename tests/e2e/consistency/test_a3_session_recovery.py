# tests/e2e/consistency/test_a3_session_recovery.py
"""
Test A3: Session Recovery - sensord recovers from SessionObsoletedError (HTTP 419).

验证 sensord 在会话过期或被强制终止后，能够检测到 419 错误并自动重新创建会话，恢复工作。
"""
import time
import pytest
import logging

from ..utils import docker_manager
from ..fixtures.constants import MEDIUM_TIMEOUT, POLL_INTERVAL

logger = logging.getLogger(__name__)

class TestSessionRecovery:
    """Test sensord's ability to recover from lost sessions."""

    def test_sensord_recovers_after_session_terminated(
        self,
        setup_sensords,
        fustord_client
    ):
        """
        Scenario:
          1. sensord A is running as leader with an active session.
          2. fustord manually terminates sensord A's session.
          3. sensord A's next heartbeat or ingestion should fail with 419.
          4. sensord A should automatically re-create session and continue.
        """
        logger.info("Starting session recovery test")
        
        # 1. Get current leader session
        sessions = fustord_client.get_sessions()
        leader = next((s for s in sessions if "client-a" in s.get("sensord_id", "")), None)
        assert leader is not None, "sensord A must be leader initially"
        
        old_session_id = leader["session_id"]
        logger.info(f"Initial session ID: {old_session_id}")
        
        # 2. Terminate the session in fustord
        logger.info(f"Force terminating session {old_session_id}...")
        fustord_client.terminate_session(old_session_id)
        
        # 3. Verify session is gone in fustord
        sessions_after = fustord_client.get_sessions()
        assert old_session_id not in [s["session_id"] for s in sessions_after]
        
        # 4. Wait for sensord A to detect error and recover
        # The heartbeat interval is usually 5s. Recovery should happen within reasonable time.
        logger.info("Waiting for sensord A to recover and create new session...")
        
        start_wait = time.time()
        new_session_id = None
        
        while time.time() - start_wait < MEDIUM_TIMEOUT:
            sessions = fustord_client.get_sessions()
            sensord_a_sessions = [s for s in sessions if "client-a" in s.get("sensord_id", "")]
            if sensord_a_sessions:
                # The sensord might briefly have the old session ID if it hasn't heartbeat yet
                current_session = sensord_a_sessions[0]
                if current_session["session_id"] != old_session_id:
                    new_session_id = current_session["session_id"]
                    logger.info(f"sensord A recovered with new session ID: {new_session_id}")
                    break
            time.sleep(POLL_INTERVAL)
            
        assert new_session_id is not None, "sensord A did not create a new session"
        assert new_session_id != old_session_id, "sensord A should have a DIFFERENT session ID"
        
        # 5. Verify it has a valid role and system availability (Proposal B.2)
        all_sessions = fustord_client.get_sessions()
        recovered_a = next((s for s in all_sessions if s["session_id"] == new_session_id), None)
        assert recovered_a is not None
        role = recovered_a.get("role")
        assert role in ["leader", "follower"], f"Recovered session should have a valid role, but got {role}"
        
        # Verify cluster health: at least one leader must exist
        leaders = [s for s in all_sessions if s.get("role") == "leader"]
        assert len(leaders) >= 1, f"Cluster must have at least one Leader after recovery. Sessions: {all_sessions}"

        logger.info("✅ Session recovery verified successfully with strict availability checks")
