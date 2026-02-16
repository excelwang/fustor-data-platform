# fustord/src/fustord/runtime/pipe/session_events.py
import asyncio
import logging
import time
from typing import Any, Dict, Optional, List

logger = logging.getLogger("fustord.pipe")

class SessionEventsMixin:
    """
    Mixin for FustordPipe handling session-related background tasks and events.
    """
    
    async def _session_event_loop(self) -> None:
        """Background loop to notify handlers of session events without blocking the API/Bridge."""
        logger.debug(f"Session event loop started for pipe {self.id}")
        
        while True:
            try:
                if not self.session_bridge:
                    await asyncio.sleep(1.0)
                    continue
                    
                event = await self.session_bridge.event_queue.get()
                if event is None:
                    break
                    
                etype = event.get("type")
                sid = event.get("session_id")
                
                # Wait for handlers to be initialized before notifying them
                if not self._handlers_ready.is_set():
                    await self.wait_until_ready(timeout=30.0)

                if etype == "create":
                    tid = event.get("task_id")
                    is_leader = event.get("is_leader", False)
                    kwargs = event.get("kwargs", {})
                    
                    logger.debug(f"Pipe {self.id}: Notifying handlers of NEW session {sid}")
                    for handler in self._view_handlers.values():
                        if hasattr(handler, 'on_session_start'):
                            try:
                                await handler.on_session_start(
                                    session_id=sid,
                                    task_id=tid,
                                    is_leader=is_leader,
                                    **kwargs
                                )
                            except Exception as e:
                                logger.error(f"Handler {handler} failed on_session_start for {sid}: {e}")

                elif etype == "close":
                    logger.debug(f"Pipe {self.id}: Notifying handlers of CLOSED session {sid}")
                    for handler in self._view_handlers.values():
                        if hasattr(handler, 'on_session_close'):
                            try:
                                await handler.on_session_close(session_id=sid)
                            except Exception as e:
                                logger.error(f"Handler {handler} failed on_session_close for {sid}: {e}")
                
                self.session_bridge.event_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in session event loop for pipe {self.id}: {e}", exc_info=True)
                await asyncio.sleep(1.0)

    async def on_session_created(
        self, 
        session_id: str, 
        task_id: Optional[str] = None, 
        is_leader: bool = False,
        **kwargs
    ) -> None:
        """
        Handle session creation notification (now minimal and non-blocking).
        """
        self.statistics["sessions_created"] += 1
        logger.info(f"Session {session_id} acknowledged by pipe {self.id} (role={'leader' if is_leader else 'follower'})")
    
    async def on_session_closed(self, session_id: str) -> None:
        """
        Handle session closure notification (now minimal and non-blocking).
        """
        self.statistics["sessions_closed"] += 1
        logger.info(f"Session {session_id} closed acknowledgement in pipe {self.id}")
    
    async def keep_session_alive(self, session_id: str, can_realtime: bool = False, datacast_status: Optional[Dict[str, Any]] = None) -> bool:
        """Update last activity for a session (stats only)."""
        if datacast_status:
            self._last_datacast_status datacastcast_status
        return True 

    async def get_session_role(self, session_id: str) -> str:
        """Get the role of a session (leader/follower)."""
        if not self.session_bridge:
            return "follower"
        
        # Check if this session is leader for ANY election key tracked for this pipe
        if self.session_bridge.store.is_any_leader(session_id):
            return "leader"
            
        return "follower"
    
    async def _session_cleanup_loop(self) -> None:
        """
        Periodic task to clean up expired sessions.
        """
        try:
            while self.is_running():
                await asyncio.sleep(10) # More frequent cleanup
                if self.session_bridge:
                    await self.session_bridge.cleanup_expired_sessions()
        except asyncio.CancelledError:
            pass

    async def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific session from local store."""
        if not self.session_bridge:
             return None
             
        entry = self.session_bridge.store.get_session(session_id)
        if entry:
            return {
                "session_id": entry.session_id,
                "task_id": entry.task_id,
                "client_ip": entry.client_ip,
                "source_uri": entry.source_uri,
                "created_at": entry.created_at,
                "last_activity": entry.last_activity,
            }
        return None
    
    async def get_all_sessions(self) -> Dict[str, Dict[str, Any]]:
        """Get all active sessions from local store."""
        if not self.session_bridge:
             return {}
             
        p_sessions = self.session_bridge.store.get_all_sessions()
        return {
            sid: {
                "task_id": entry.task_id,
                "view_ids": entry.view_ids,
                "source_uri": entry.source_uri,
                "created_at": entry.created_at,
                "last_activity": entry.last_activity,
                "is_leader": self.session_bridge.store.is_any_leader(sid)
            } 
            for sid, entry in p_sessions.items()
        }
        
    @property
    def leader_session(self) -> Optional[str]:
        """
        Get any current leader session ID for this pipe.
        """
        if not self.session_bridge:
             return None
        # Return the first session that matches any leader key
        for sid in self.session_bridge.store.sessions:
             if self.session_bridge.store.is_any_leader(sid):
                  return sid
        return None
