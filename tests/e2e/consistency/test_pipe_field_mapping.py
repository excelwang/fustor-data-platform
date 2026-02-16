# tests/e2e/consistency/test_pipe_field_mapping.py
"""
Integration test for Field Mapping in DatacastPipe.
"""
import time
import pytest
import logging
from ..fixtures.constants import MOUNT_POINT, FUSION_ENDPOINT, MEDIUM_TIMEOUT, POLL_INTERVAL

logger = logging.getLogger("fustor_test")

class TestPipeFieldMapping:
    """Test field mapping functionality in DatacastPipe."""
    
    def test_field_mapping_affects_data(
        self, 
        docker_env, 
        setup_datacasts, 
        fustord_client
    ):
        """
        Test that field mapping correctly transforms data.
        We will map 'size' to 'remapped_size' so that fustord sees default size (0).
        """
        logger.info("Running field mapping test")
        
        containers = setup_datacasts["containers"]
        leader = containers["leader"]
        view_id = setup_datacasts["view_id"]
        api_key = setup_datacasts["api_key"]
        
        # 1. Update datacast Config to include fields_mapping
        # Map: path -> path, modified_time -> modified_time, is_directory -> is_directory, size -> remapped_size
        pipe_config = f"""
pipes:
  integration-test-ds:
    source: shared-fs
    sender: fustord-main
    fields_mapping:
      - to: "path"
        source: ["path:string"]
      - to: "modified_time"
        source: ["modified_time:number"]
      - to: "is_directory"
        source: ["is_directory:boolean"]
      - to: "created_time"
        source: ["created_time:number"]
      # Test mapping: Map standard 'size' to a custom field 'remapped_size'
      # This verifies that the 'size' field in fustord becomes 0 (default) because its source was redirected.
      - to: "remapped_size"
        source: ["size:integer"]
"""
        docker_env.create_file_in_container(
            leader, 
            "/root/.fustor/datacast-config/pipe-task-1.yaml", 
            pipe_config
        )
        
        # 2. Restart datacast to apply config
        logger.info(f"Restarting datacast in {leader} to apply fields_mapping")
        
        # Reset fustord state to ensure a clean start for the new mapping
        fustord_client.reset()
        
        # Use the standard ensure_datacast_running function to restart
        # This handles PID cleanup and environment variable injection correctly
        setup_datacasts["ensure_datacast_running"](leader, api_key, view_id)
        
        # Wait for datacast to reconnect and become leader
        logger.info("Waiting for datacast A to become leader...")
        start_wait = time.time()
        while time.time() - start_wait < MEDIUM_TIMEOUT:
            sessions = fustord_client.get_sessions()
            # Ensure it's the leader and it's client-a
            leader_session = next((s for s in sessions if s.get("role") == "leader" and "client-a" in s.get("datacast_id", "")), None)
            if leader_session:
                logger.info(f"datacast A successfully became leader: {leader_session.get('session_id')}")
                break
            time.sleep(POLL_INTERVAL)
        else:
             pytest.fail(f"datacast A failed to become leader. Current sessions: {fustord_client.get_sessions()}")
        
        # 3. Create a file with specific size
        import os.path
        timestamp = int(time.time() * 1000)
        file_name = f"mapping_test_{timestamp}.txt"
        test_file = f"{MOUNT_POINT}/{file_name}"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        expected_size = 1234
        
        # Create file with 1234 bytes
        # Use dd for cleaner file creation
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"dd if=/dev/zero of={test_file} bs={expected_size} count=1"]
        )
        logger.info(f"Created test file with size {expected_size}: {test_file}")
        
        # 4. Wait for fustord to detect it
        # FSDriver should use absolute paths by default (matching schema 'Absolute file path')
        expected_path_in_tree = test_file_rel
        success = fustord_client.wait_for_file_in_tree(expected_path_in_tree, timeout=MEDIUM_TIMEOUT)
        assert success, f"File {expected_path_in_tree} not found in tree. Tree: {fustord_client.get_tree()}"
        
        # 5. Verify size in fustord
        # Because we mapped 'size' to 'remapped_size', and fustord expects 'size',
        # fustord should see the default value (0) instead of 1234.
        node = fustord_client.get_node(expected_path_in_tree)
        assert node is not None, f"Node {expected_path_in_tree} should exist but get_node returned None."
        
        logger.info(f"fustord node data: {node}")
        
        actual_size = node.get('size')
        
        assert actual_size in (0, None), f"Expected size 0 or None (due to mapping), but got {actual_size}"
        logger.info("✅ Field mapping confirmed: 'size' was correctly redirected to 'remapped_size', causing fustord to see 0.")
