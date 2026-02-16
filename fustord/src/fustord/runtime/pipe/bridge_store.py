# fustord/src/fustord/runtime/pipe/bridge_store.py
from typing import Dict, List, Set

class PipeSessionStore:
    """
    Dedicated store for session-related state of a FustordPipe.
    """
    
    def __init__(self, view_ids: List[str]):
        self.view_ids = view_ids
        # {session_id: {sensord_id, source_uri}}
        self.lineage_cache: Dict[str, Dict[str, str]] = {}
        # {election_key: set(session_id)}
        self.leader_cache: Dict[str, Set[str]] = {}
        # session_id -> heartbeat_count
        self.heartbeat_count: Dict[str, int] = {}
        
    def add_session(self, session_id: str, lineage: Dict[str, str]):
        """Synchronously add session metadata."""
        self.lineage_cache[session_id] = lineage
        self.heartbeat_count[session_id] = 0
        
    def remove_session(self, session_id: str):
        """Synchronously remove session metadata."""
        self.lineage_cache.pop(session_id, None)
        self.heartbeat_count.pop(session_id, None)
        # Remove from leader cache
        for key in list(self.leader_cache.keys()):
            self.leader_cache[key].discard(session_id)
            if not self.leader_cache[key]:
                del self.leader_cache[key]

    def get_lineage(self, session_id: str) -> Dict[str, str]:
        """Get lineage info for an active session."""
        return self.lineage_cache.get(session_id, {})

    def is_leader(self, session_id: str, election_key: str) -> bool:
        """Check if a session is currently recorded as leader for a key."""
        return session_id in self.leader_cache.get(election_key, set())

    def is_any_leader(self, session_id: str) -> bool:
        """Check if session is leader for ANY election key tracked for this pipe."""
        for sessions in self.leader_cache.values():
            if session_id in sessions:
                return True
        return False

    def record_leader(self, session_id: str, election_key: str):
        """Record leadership status."""
        self.leader_cache.setdefault(election_key, set()).add(session_id)

    def record_heartbeat(self, session_id: str) -> int:
        """Increment and return heartbeat count."""
        count = self.heartbeat_count.get(session_id, 0) + 1
        self.heartbeat_count[session_id] = count
        return count
