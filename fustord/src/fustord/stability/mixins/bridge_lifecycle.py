# fustord/src/fustord/runtime/pipe/bridge_lifecycle.py
import logging
import uuid
import time
from typing import Any, Dict, Optional, TYPE_CHECKING
from fustord.domain.view_state_manager import view_state_manager

if TYPE_CHECKING:
    from ..session_bridge import PipeSessionBridge

logger = logging.getLogger("fustord.session_bridge")

class BridgeLifecycleMixin:
    """
    Mixin for PipeSessionBridge handling session creation, heartbeats, and closure.
    """
    
    async def create_session(
        self: "PipeSessionBridge",
        task_id: str,
        client_ip: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None,
        allow_concurrent_push: Optional[bool] = None,
        session_id: Optional[str] = None,
        source_uri: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Create a session in the PipeSessionStore."""
        session_id = session_id or str(uuid.uuid4())
        
        if allow_concurrent_push is None:
            allow_concurrent_push = getattr(self._pipe, 'allow_concurrent_push', True)

        # Scoped election check (usually first view)
        view_id = self._pipe.view_ids[0]
        view_handler = self._pipe.find_handler_for_view(view_id)
        
        session_result = {"role": "leader"}
        if view_handler:
             try:
                 session_result = await view_handler.resolve_session_role(session_id, pipe_id=self._pipe.id)
             except Exception as e:
                 logger.error(f"View handler failed to process new session {session_id}: {e}")
        else:
             logger.warning(f"No view handler found for view {view_id}, defaulting to leader role")

        is_leader = (session_result.get("role") == "leader")
        election_key = session_result.get("election_key", view_id)
        
        if is_leader:
             self.store.record_leader(session_id, election_key)
             if not allow_concurrent_push:
                  locked_sid = await view_state_manager.get_locked_session_id(election_key)
                  if locked_sid:
                        # Check if locked_sid is active in ANY pipe
                        from fustord.stability.runtime_objects import pipe_manager
                        is_active = pipe_manager.is_session_active(locked_sid) if pipe_manager else False
                        
                        if not is_active:
                             logger.warning(f"View {election_key} locked by stale session {locked_sid}. Auto-unlocking.")
                             await view_state_manager.unlock_for_session(election_key, locked_sid)

                  locked = await view_state_manager.lock_for_session(election_key, session_id)
                  if not locked:
                        raise ValueError(f"View {election_key} is currently locked by another session")
        
        # Add to local store (Source of Truth)
        self.store.add_session(
            session_id=session_id,
            task_id=task_id,
            client_ip=client_ip,
            source_uri=source_uri,
            timeout=session_timeout_seconds
        )

        await self.event_queue.put({
            "type": "create",
            "session_id": session_id,
            "task_id": task_id,
            "is_leader": is_leader,
            "kwargs": kwargs
        })
        
        await self._pipe.on_session_created(
            session_id=session_id,
            task_id=task_id,
            client_ip=client_ip,
            is_leader=is_leader
        )
        
        timeout = session_timeout_seconds or self.store.default_timeout
        return {
            "session_id": session_id,
            "role": "leader" if is_leader else "follower",
            "session_timeout_seconds": timeout,
        }
    
    async def keep_alive(
        self: "PipeSessionBridge",
        session_id: str,
        client_ip: Optional[str] = None,
        can_realtime: bool = False,
        sensord_status: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Keep session alive (heartbeat)."""
        # 1. Update local store
        count = self.store.record_heartbeat(
            session_id=session_id,
            can_realtime=can_realtime,
            sensord_status=sensord_status
        )
        
        if count == 0:
            return {"status": "error", "message": f"Session {session_id} expired", "session_id": session_id}
        
        # 2. Update pipe stats/lifecycle
        await self._pipe.keep_session_alive(session_id, can_realtime=can_realtime, sensord_status=sensord_status)
        
        # 3. Periodic leader verification
        if count % self._LEADER_VERIFY_INTERVAL == 0:
            for vid in self._pipe.view_ids:
                view_handler = self._pipe.find_handler_for_view(vid)
                if view_handler:
                     try:
                         res = await view_handler.resolve_session_role(session_id, pipe_id=self._pipe.id)
                         is_l = (res.get("role") == "leader")
                         e_key = res.get("election_key", vid)
                         if is_l:
                             self.store.record_leader(session_id, e_key)
                         else:
                             if e_key in self.store.leader_cache:
                                 self.store.leader_cache[e_key].discard(session_id)
                     except Exception:
                         pass
    
        role = await self._pipe.get_session_role(session_id)
        
        # Extract pending commands from entry
        entry = self.store.get_session(session_id)
        commands = []
        if entry and entry.pending_commands:
            commands = entry.pending_commands[:]
            entry.pending_commands.clear()

        return {
            "role": role,
            "session_id": session_id,
            "can_realtime": can_realtime,
            "status": "ok",
            "commands": commands
        }
    
    async def close_session(self: "PipeSessionBridge", session_id: str) -> bool:
        """Close a session."""
        keys_to_clean = [k for k, s in self.store.leader_cache.items() if session_id in s]
        for key in keys_to_clean:
            await view_state_manager.unlock_for_session(key, session_id)
            await view_state_manager.release_leader(key, session_id)
        
        self.store.remove_session(session_id)
        await self.event_queue.put({"type": "close", "session_id": session_id})
        await self._pipe.on_session_closed(session_id)
        return True

    async def cleanup_expired_sessions(self: "PipeSessionBridge"):
        """Periodic cleanup task called by Pipe."""
        expired = self.store.get_expired_sessions()
        for sid in expired:
            logger.info(f"Bridge {self._pipe.id}: Session {sid} expired. Terminating.")
            await self.close_session(sid)
