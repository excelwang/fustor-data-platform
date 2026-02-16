# fustord/src/fustord/runtime/session_bridge.py
"""
Bridge between FustordPipe and the existing SessionManager.
"""
import asyncio
import logging
from typing import Any, Dict, Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipe import FustordPipe
    from fustord.stability.session_manager import SessionManager

from .mixins.bridge_store import PipeSessionStore
from .mixins.bridge_lifecycle import BridgeLifecycleMixin
from .mixins.bridge_commands import BridgeCommandsMixin

logger = logging.getLogger("fustord.session_bridge")


class PipeSessionBridge(BridgeLifecycleMixin, BridgeCommandsMixin):
    """
    Bridge that synchronizes sessions between FustordPipe and SessionManager.
    """
    
    def __init__(
        self,
        pipe: "FustordPipe",
        session_manager: "SessionManager"
    ):
        self._pipe = pipe
        self._session_manager = session_manager
        pipe.session_bridge = self
        
        self.store = PipeSessionStore(pipe.view_ids)
        self.event_queue = asyncio.Queue()
        self._pending_commands: Dict[str, asyncio.Future] = {} 
        self._LEADER_VERIFY_INTERVAL = 5 
    
    async def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session info from Pipe."""
        return await self._pipe.get_session_info(session_id)
    
    async def get_all_sessions(self) -> Dict[str, Dict[str, Any]]:
        """Get all active sessions."""
        return await self._pipe.get_all_sessions()
    
    @property
    def leader_session(self) -> Optional[str]:
        """Get the current leader session ID."""
        return self._pipe.leader_session


def create_session_bridge(
    pipe: "FustordPipe",
    session_manager: Optional["SessionManager"] = None
) -> PipeSessionBridge:
    """
    Create a session bridge for the given pipe.
    """
    if session_manager is None:
        from fustord.stability.session_manager import session_manager as global_session_manager
        session_manager = global_session_manager
    
    return PipeSessionBridge(pipe, session_manager)
