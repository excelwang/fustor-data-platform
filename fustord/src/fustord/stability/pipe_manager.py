# fustord/src/fustord/runtime/pipe_manager.py
import asyncio
import logging
from typing import Dict, List, Optional, Any

from sensord_core.transport.receiver import Receiver
from .pipe import FustordPipe
from .mixins.manager_lifecycle import ManagerLifecycleMixin
from .mixins.manager_callbacks import ManagerCallbacksMixin

logger = logging.getLogger(__name__)

class FustordPipeManager(ManagerLifecycleMixin, ManagerCallbacksMixin):
    """
    Manages the lifecycle of FustordPipes and their associated Receivers.
    """
    
    def __init__(self):
        self._pipes: Dict[str, FustordPipe] = {}
        self._receivers: Dict[str, Receiver] = {} # Keyed by signature (driver, port)
        self._bridges: Dict[str, Any] = {}
        self._session_to_pipe: Dict[str, str] = {}
        self._init_lock = asyncio.Lock()
        self._pipe_locks: Dict[str, asyncio.Lock] = {}
        self._target_pipe_ids: List[str] = []
    
    def _get_pipe_lock(self, pipe_id: str) -> asyncio.Lock:
        """获取 per-pipe 锁（惰性创建）。"""
        return self._pipe_locks.setdefault(pipe_id, asyncio.Lock())
    
    def resolve_pipes_for_view(self, view_id: str) -> List[str]:
        """
        Maps a View ID to a list of Pipe IDs that service it.
        """
        pipe_ids = []
        for p_id, pipe in self._pipes.items():
            if pipe.find_handler_for_view(view_id):
                pipe_ids.append(p_id)
        
        return pipe_ids

    def get_pipes(self) -> Dict[str, FustordPipe]:
        return self._pipes.copy()

    def is_session_active(self, session_id: str) -> bool:
        """Check if session is active in any pipe."""
        return session_id in self._session_to_pipe

    async def list_sessions(self, view_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all active sessions across all pipes, optionally filtered by view_id."""
        sessions = []
        for pipe in self._pipes.values():
            if view_id and view_id not in pipe.view_ids:
                continue
            
            p_sessions = await pipe.get_all_sessions()
            for sid, info in p_sessions.items():
                if view_id and view_id not in info.get("view_ids", []):
                    continue
                sessions.append({"session_id": sid, **info})
        return sessions

    async def clear_all_sessions(self, view_id: Optional[str] = None):
        """Terminate all sessions across all pipes, optionally filtered by view."""
        for p_id in list(self._pipes.keys()):
            pipe = self._pipes[p_id]
            if view_id and view_id not in pipe.view_ids:
                continue
            
            # Use list() to avoid mutation during iteration
            p_sessions = await pipe.get_all_sessions()
            for sid in list(p_sessions.keys()):
                await self._on_session_closed(sid)

    async def cleanup_expired_sessions(self):
        """Identify and remove expired sessions from all pipes."""
        for p_id in list(self._pipes.keys()):
            bridge = self._bridges.get(p_id)
            if bridge:
                await bridge.cleanup_expired_sessions()

    def get_pipe(self, pipe_id: str) -> Optional[FustordPipe]:
        """Get a specific pipe instance by ID."""
        return self._pipes.get(pipe_id)

    def get_bridge(self, pipe_id: str) -> Optional[Any]:
        """Get the session bridge for a specific pipe."""
        return self._bridges.get(pipe_id)

    def get_receiver(self, receiver_id: str) -> Optional[Receiver]:
        """Get receiver by ID or internal signature."""
        from .config.unified import fustord_config
        
        # 1. Check if receiver_id is a config ID
        config = fustord_config.get_receiver(receiver_id)
        if config:
            sig = (config.driver, config.port)
            return self._receivers.get(sig)
        
        # 2. Check if it's an internal ID (e.g. recv_http_8102)
        for r in self._receivers.values():
            if r.id == receiver_id:
                return r
        
        return None

# Global singleton
pipe_manager = FustordPipeManager()
