# fustord/src/fustord/runtime/pipe/bridge_lifecycle.py
import logging
import uuid
import time
from typing import Any, Dict, Optional, TYPE_CHECKING
from fustord.view_state_manager import view_state_manager
from fustord.core.session_manager import session_manager as global_session_manager

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
        """Create a session in both Pipe and SessionManager."""
        session_id = session_id or str(uuid.uuid4())
        
        if allow_concurrent_push is None:
            allow_concurrent_push = getattr(self._pipe, 'allow_concurrent_push', True)

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
                       all_active = await global_session_manager.get_all_active_sessions()
                       is_active = any(locked_sid in view_sess for view_sess in all_active.values())
                       if not is_active:
                            logger.warning(f"View {election_key} locked by stale session {locked_sid}. Auto-unlocking.")
                            await view_state_manager.unlock_for_session(election_key, locked_sid)

                 locked = await view_state_manager.lock_for_session(election_key, session_id)
                 if not locked:
                       raise ValueError(f"View {election_key} is currently locked by another session")
        
        for vid in self._pipe.view_ids:
            await self._session_manager.create_session_entry(
                view_id=vid,
                session_id=session_id,
                task_id=task_id,
                client_ip=client_ip,
                allow_concurrent_push=allow_concurrent_push,
                session_timeout_seconds=session_timeout_seconds,
                source_uri=source_uri
            )
        
        lineage = {}
        if source_uri: lineage["source_uri"] = source_uri
        if task_id: lineage["sensord_id"] = task_id.split(":")[0] if ":" in task_id else task_id
            
        self.store.add_session(session_id, lineage)

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
        
        timeout = session_timeout_seconds or 30
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
        commands = []
        alive = False
        for vid in self._pipe.view_ids:
            v_alive, commands_batch = await self._session_manager.keep_session_alive(
                view_id=vid,
                session_id=session_id,
                client_ip=client_ip,
                can_realtime=can_realtime,
                sensord_status=sensord_status
            )
            if v_alive:
                alive = True
                commands.extend(commands_batch)
        
        if not alive:
            return {"status": "error", "message": f"Session {session_id} expired", "session_id": session_id}
        
        await self._pipe.keep_session_alive(session_id, can_realtime=can_realtime, sensord_status=sensord_status)
        
        count = self.store.record_heartbeat(session_id)
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
        return {
            "role": role,
            "session_id": session_id,
            "can_realtime": can_realtime,
            "status": "ok",
            "commands": commands
        }
    
    async def close_session(self: "PipeSessionBridge", session_id: str) -> bool:
        """Close a session in both systems."""
        for vid in self._pipe.view_ids:
            await self._session_manager.terminate_session(view_id=vid, session_id=session_id)

        keys_to_clean = [k for k, s in self.store.leader_cache.items() if session_id in s]
        for key in keys_to_clean:
            await view_state_manager.unlock_for_session(key, session_id)
            await view_state_manager.release_leader(key, session_id)
        
        self.store.remove_session(session_id)
        await self.event_queue.put({"type": "close", "session_id": session_id})
        await self._pipe.on_session_closed(session_id)
        return True
