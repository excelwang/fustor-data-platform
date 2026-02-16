import time
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass, field

@dataclass
class SessionEntry:
    session_id: str
    task_id: str
    view_ids: List[str]
    created_at: float
    last_activity: float
    timeout: int
    client_ip: Optional[str] = None
    source_uri: Optional[str] = None
    can_realtime: bool = False
    datacast_status: Optional[Dict[str, Any]] = None
    pending_commands: List[Dict[str, Any]] = field(default_factory=list)

class PipeSessionStore:
    """
    Dedicated store for session-related state of a FustordPipe.
    Now acts as the source of truth for sessions on this pipe.
    """
    
    def __init__(self, view_ids: List[str], default_timeout: int = 30):
        self.view_ids = view_ids
        self.default_timeout = default_timeout
        # session_id -> SessionEntry
        self.sessions: Dict[str, SessionEntry] = {}
        # {election_key: set(session_id)}
        self.leader_cache: Dict[str, Set[str]] = {}
        # session_id -> heartbeat_count
        self.heartbeat_count: Dict[str, int] = {}
        
    def add_session(self, session_id: str, task_id: str, 
                    client_ip: Optional[str] = None, 
                    source_uri: Optional[str] = None,
                    timeout: Optional[int] = None):
        """Add a new session to the store."""
        now = time.monotonic()
        now_epoch = time.time()
        
        entry = SessionEntry(
            session_id=session_id,
            task_id=task_id,
            view_ids=self.view_ids,
            created_at=now_epoch,
            last_activity=now,
            timeout=timeout or self.default_timeout,
            client_ip=client_ip,
            source_uri=source_uri
        )
        self.sessions[session_id] = entry
        self.heartbeat_count[session_id] = 0
        
    def remove_session(self, session_id: str):
        """Remove mission metrics."""
        self.sessions.pop(session_id, None)
        self.heartbeat_count.pop(session_id, None)
        # Remove from leader cache
        for key in list(self.leader_cache.keys()):
            self.leader_cache[key].discard(session_id)
            if not self.leader_cache[key]:
                del self.leader_cache[key]

    def get_session(self, session_id: str) -> Optional[SessionEntry]:
        return self.sessions.get(session_id)

    def get_all_sessions(self) -> Dict[str, SessionEntry]:
        return self.sessions.copy()

    def record_heartbeat(self, session_id: str, can_realtime: bool = False, 
                         datacast_status: Optional[Dict[str, Any]] = None) -> int:
        """Update activity and return heartbeat count."""
        entry = self.sessions.get(session_id)
        if entry:
            entry.last_activity = time.monotonic()
            entry.can_realtime = can_realtime
            if datacast_status:
                entry.datacast_status = datacast_status
            
            count = self.heartbeat_count.get(session_id, 0) + 1
            self.heartbeat_count[session_id] = count
            return count
        return 0

    def is_leader(self, session_id: str, election_key: str) -> bool:
        return session_id in self.leader_cache.get(election_key, set())

    def is_any_leader(self, session_id: str) -> bool:
        for sessions in self.leader_cache.values():
            if session_id in sessions:
                return True
        return False

    def record_leader(self, session_id: str, election_key: str):
        self.leader_cache.setdefault(election_key, set()).add(session_id)

    def get_expired_sessions(self) -> List[str]:
        """Identify sessions that have timed out."""
        now = time.monotonic()
        expired = []
        for sid, entry in self.sessions.items():
            if now - entry.last_activity >= entry.timeout:
                expired.append(sid)
        return expired

    def queue_command(self, session_id: str, command: Dict[str, Any]):
        """Queue a command for a specific session."""
        entry = self.sessions.get(session_id)
        if entry:
            entry.pending_commands.append(command)

    def get_lineage(self, session_id: str) -> Dict[str, str]:
        """Return lineage info for event metadata injection."""
        entry = self.sessions.get(session_id)
        if not entry:
            return {}
            
        datacast_id = entry.task_id.split(':')[0] if ':' in entry.task_id else "unknown"
        return {
            "datacast_id": datacast_id,
            "source_uri": entry.source_uri or ""
        }
