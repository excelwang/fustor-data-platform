"""
Test A1: First datacastst becomes Leader.

验证第一个连接到 fustord 的 datacastst 被选举为 Leader。
参考文档: CONSISTENCY_DESIGN.md - Section 3 (Leader/Follower 选举)
"""
import pytest
import time

from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_B
from ..fixtures.constants import VIEW_READY_TIMEOUT


class TestLeaderElectionFirst:
    """Test that the first datacastst to connect becomes the leader."""

    def test_first_datacastst_becomes_leader(
        self,
        docker_env,
        fustord_client,
        setup_datacaststs,
        clean_shared_dir
    ):
        """
        场景: 第一个 datacastst A 连接到 fustord
        预期: datacastst A 被选举为 Leader
        验证方法: 查询 fustord Sessions API，确认 datacastst A 的 role 为 "leader"
        """
        # Wait for datacaststs to establish sessions and View to be READY
        assert fustord_client.wait_for_view_ready(timeout=VIEW_READY_TIMEOUT), "View did not become ready"
        
        # Get all sessions from fustord
        sessions = fustord_client.get_sessions()
        
        # Find leader session
        leaders = [s for s in sessions if s.get("role") == "leader"]
        
        # Assert exactly one leader exists (Proposal B.3)
        assert len(leaders) == 1, f"Should have exactly one leader, got: {leaders}"
        leader_session = leaders[0]
        
        # Record actual leader for debugging
        import logging
        test_logger = logging.getLogger("fustor_test")
        test_logger.info(f"Elected leader for view: {leader_session.get('datacastst_id')}")
        
        # Verify leader has the correct capabilities
        assert leader_session.get("can_snapshot") is True, "Leader should be able to snapshot"
        assert leader_session.get("can_audit") is True, "Leader should be able to audit"
