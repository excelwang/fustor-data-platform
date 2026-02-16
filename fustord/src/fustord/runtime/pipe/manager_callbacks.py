# fustord/src/fustord/runtime/pipe/manager_callbacks.py
import logging
import time
from typing import Optional, TYPE_CHECKING
from fustor_core.models.states import SessionInfo
from fustord.config.unified import fustord_config

if TYPE_CHECKING:
    from ..pipe_manager import FustordPipeManager

logger = logging.getLogger("fustord.pipe_manager")

class ManagerCallbacksMixin:
    """
    Mixin for FustordPipeManager handling receiver callbacks.
    """
    
    async def _on_session_created(self: "FustordPipeManager", session_id, task_id, p_id, client_info, session_timeout_seconds):
        fustord_pipe = self._pipes.get(p_id)
        if not fustord_pipe: raise ValueError(f"FustordPipe {p_id} not found")
        
        from fustord.api.session import _check_duplicate_task
        for vid in fustord_pipe.view_ids:
            if await _check_duplicate_task(vid, task_id):
                 raise ValueError(f"Task {task_id} already active on view {vid}")

        bridge = self._bridges.get(p_id)
        
        async with self._get_pipe_lock(p_id):
            source_uri = client_info.get("source_uri") if client_info else None
            result = await bridge.create_session(
                task_id=task_id, 
                client_ip=client_info.get("client_ip") if client_info else None, 
                session_id=session_id, 
                session_timeout_seconds=session_timeout_seconds,
                source_uri=source_uri
            )
            self._session_to_pipe[session_id] = p_id
            
            p_cfg = fustord_config.get_pipe(p_id)
            audit_interval = p_cfg.audit_interval_sec if p_cfg else None
            sentinel_interval = p_cfg.sentinel_interval_sec if p_cfg else None
            
            info = SessionInfo(
                session_id=session_id, 
                task_id=task_id, 
                view_id=p_id, 
                role=result["role"], 
                created_at=time.time(), 
                last_heartbeat=time.time()
            )
            info.source_uri = source_uri
            info.audit_interval_sec = audit_interval
            info.sentinel_interval_sec = sentinel_interval
            return info

    async def _on_event_received(self: "FustordPipeManager", session_id, events, source_type, is_end, metadata=None):
        p_id = self._session_to_pipe.get(session_id)
        if p_id:
            fustord_pipe = self._pipes.get(p_id)
            if fustord_pipe:
                res = await fustord_pipe.process_events(events, session_id, source_type, is_end=is_end, metadata=metadata)
                return res.get("success", False)
        raise ValueError(f"Session {session_id} not found or expired")

    async def _on_heartbeat(self: "FustordPipeManager", session_id, can_realtime=bool, sensord_status=None):
        pipe_id = self._session_to_pipe.get(session_id)
        if pipe_id:
            bridge = self._bridges.get(pipe_id)
            if bridge: return await bridge.keep_alive(session_id, can_realtime=can_realtime, sensord_status=sensord_status)
        return {"status": "error"}

    async def _on_session_closed(self: "FustordPipeManager", session_id):
        pipe_id = self._session_to_pipe.pop(session_id, None)
        if pipe_id:
            async with self._get_pipe_lock(pipe_id):
                bridge = self._bridges.get(pipe_id)
                if bridge: await bridge.close_session(session_id)

    async def _on_scan_complete(self: "FustordPipeManager", session_id: str, scan_path: str, job_id: Optional[str] = None):
        """Handle technical scan completion notification."""
        from fustord.core.session_manager import session_manager
        pipe_id = self._session_to_pipe.get(session_id)
        if pipe_id:
            fustord_pipe = self._pipes.get(pipe_id)
            if fustord_pipe:
                for vid in fustord_pipe.view_ids:
                    await session_manager.complete_sensord_job(vid, session_id, scan_path, job_id)
                logger.debug(f"Scan complete (job_id={job_id}) for session {session_id}")
