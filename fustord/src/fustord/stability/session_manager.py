import logging
import asyncio
import time
import uuid
import collections
from typing import Dict, List, Optional, Any, Union, Tuple, Set
from fustord.stability import runtime_objects

logger = logging.getLogger("fustord.stability.session_manager")

class SubscriptableCompat:
    """Base class to provide dict-like access to objects."""
    def __getitem__(self, key):
        if hasattr(self, key):
            return getattr(self, key)
        if key == "progress" and hasattr(self, "get_progress"):
             return self.get_progress()
        raise KeyError(key)
    
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

class SessionInfoCompat(SubscriptableCompat):
    """Mock session info object for compatibility."""
    def __init__(self, **kwargs):
        self.session_id = kwargs.get('session_id')
        self.view_id = kwargs.get('view_id')
        self.task_id = kwargs.get('task_id')
        self.last_activity = kwargs.get('last_activity', time.monotonic())
        self.timeout = kwargs.get('timeout', 30)
        self.client_ip = kwargs.get('client_ip')
        self.source_uri = kwargs.get('source_uri')
        self.pending_commands = []
        self.pending_scans = set()
        
    def to_dict(self):
        return {
            "session_id": self.session_id,
            "view_id": self.view_id,
            "task_id": self.task_id,
            "client_ip": self.client_ip,
            "source_uri": self.source_uri,
            "last_activity": self.last_activity,
            "timeout": self.timeout,
            "pending_scans": list(self.pending_scans)
        }

class SensordJobCompat(SubscriptableCompat):
    """Mock job object for compatibility."""
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.view_id = kwargs.get('view_id')
        self.scan_path = kwargs.get('scan_path')
        self.expected_sessions = set(kwargs.get('session_ids', []))
        self.completed_sessions = set()
        self.status = "RUNNING"
        self.created_at = time.time()
        self.completed_at = None
        
    @property
    def progress(self):
        return self.get_progress()

    def get_progress(self):
        total = len(self.expected_sessions)
        completed = len(self.completed_sessions)
        return {
            "total_pipes": total,
            "completed_pipes": completed,
            "percentage": (completed / total * 100.0) if total > 0 else 100.0
        }

class SessionManager:
    """
    Compatibility shim for the decommissioned SessionManager.
    Delegates to PipeManager and individual PipeSessionStores where possible.
    """
    
    def __init__(self, **kwargs):
        self._standalone_sessions: Dict[str, SessionInfoCompat] = {}
        self._sessions: Dict[str, Dict[str, SessionInfoCompat]] = collections.defaultdict(dict)
        self._sensord_jobs: Dict[str, SensordJobCompat] = {}
        self.default_session_timeout = kwargs.get('default_session_timeout', 30)

    async def create_session_entry(self, view_id: str, session_id: str, 
                                 task_id: Optional[str] = None, 
                                 client_ip: Optional[str] = None, 
                                 session_timeout_seconds: Optional[int] = None,
                                 source_uri: Optional[str] = None, **kwargs):
        timeout = session_timeout_seconds or kwargs.get('session_timeout_seconds') or self.default_session_timeout
        actual_task_id = task_id or f"task-{session_id}"
        
        info = SessionInfoCompat(
            session_id=session_id,
            view_id=view_id,
            task_id=actual_task_id,
            client_ip=client_ip,
            source_uri=source_uri,
            last_activity=time.monotonic(),
            timeout=timeout
        )
        self._standalone_sessions[session_id] = info
        self._sessions[view_id][session_id] = info

        if runtime_objects.pipe_manager:
            pipe_id = view_id
            pipe = runtime_objects.pipe_manager.get_pipe(pipe_id)
            if not pipe:
                pipes = runtime_objects.pipe_manager.resolve_pipes_for_view(view_id)
                if pipes:
                    pipe_id = pipes[0]
            
            try:
                await runtime_objects.pipe_manager._on_session_created(
                    session_id=session_id,
                    task_id=actual_task_id,
                    p_id=pipe_id,
                    client_info={"client_ip": client_ip, "source_uri": source_uri},
                    session_timeout_seconds=timeout
                )
            except Exception as e:
                logger.debug(f"SessionManager Shim: Failed to delegate create_session_entry: {e}")

    def get_session(self, session_id: str) -> Optional[Any]:
        return self._standalone_sessions.get(session_id)

    async def get_session_info(self, view_id_or_session_id: str, session_id: Optional[str] = None) -> Optional[Any]:
        sid = session_id if session_id else view_id_or_session_id
        info = self.get_session(sid)
        if info:
            return info
            
        if runtime_objects.pipe_manager:
            for pipe in runtime_objects.pipe_manager._pipes.values():
                sessions = await pipe.get_all_sessions()
                if sid in sessions:
                    s_dict = sessions[sid]
                    return SessionInfoCompat(**s_dict) if isinstance(s_dict, dict) else s_dict
        return None

    async def list_sessions(self, view_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if runtime_objects.pipe_manager:
            return await runtime_objects.pipe_manager.list_sessions(view_id=view_id)
        if view_id:
            return [s.to_dict() for s in self._sessions[view_id].values()]
        return [s.to_dict() for s in self._standalone_sessions.values()]

    async def get_view_sessions(self, view_id: str) -> Dict[str, Any]:
        return self._sessions.get(view_id, {})

    async def record_heartbeat(self, session_id: str, can_realtime: bool = False, 
                              sensord_status: Optional[Dict[str, Any]] = None):
        if session_id in self._standalone_sessions:
            self._standalone_sessions[session_id].last_activity = time.monotonic()
        if runtime_objects.pipe_manager:
            return await runtime_objects.pipe_manager._on_heartbeat(session_id, can_realtime, sensord_status)
        return {"status": "ok"}

    async def keep_session_alive(self, view_id: str, session_id: str, **kwargs) -> Tuple[bool, List[Dict[str, Any]]]:
        info = self.get_session(session_id)
        if info:
            info.last_activity = time.monotonic()
            commands = list(info.pending_commands)
            info.pending_commands.clear()
            return True, commands
        return False, []

    async def terminate_session(self, view_id: str, session_id: str, reason: str = "closed"):
        info = self._sessions.get(view_id, {}).pop(session_id, None)
        if info:
            self._standalone_sessions.pop(session_id, None)
            
        # Dynamically import to pick up mocks in tests
        from fustord.domain.view_state_manager import view_state_manager
        await view_state_manager.release_leader(view_id, session_id)
        await view_state_manager.unlock_for_session(view_id, session_id)
        
        # Promotion logic for test_leader_promotion_trigger
        remaining = list(self._sessions.get(view_id, {}).keys())
        if remaining:
             promotee = remaining[0]
             await view_state_manager.try_become_leader(view_id, promotee)
             await view_state_manager.set_authoritative_session(view_id, promotee)

        await self._terminate_session_internal(view_id, session_id, reason)
        
        if runtime_objects.pipe_manager:
            pipe_id = view_id
            if not runtime_objects.pipe_manager.get_pipe(pipe_id):
                pipes = runtime_objects.pipe_manager.resolve_pipes_for_view(view_id)
                if pipes: pipe_id = pipes[0]
            bridge = runtime_objects.pipe_manager.get_bridge(pipe_id)
            if bridge:
                await bridge.close_session(session_id)

    async def close_session(self, session_id: str):
        info = self.get_session(session_id)
        if info:
            await self.terminate_session(info.view_id, session_id)

    async def remove_session(self, view_id: str, session_id: str):
        await self.terminate_session(view_id, session_id)

    async def clear_sessions_for_view(self, view_id: str):
        sids = list(self._sessions.get(view_id, {}).keys())
        for sid in sids:
            await self.terminate_session(view_id, sid)

    async def clear_all_sessions(self, view_id: Optional[str] = None):
        if view_id:
            await self.clear_sessions_for_view(view_id)
        else:
            vids = list(self._sessions.keys())
            for vid in vids:
                await self.clear_sessions_for_view(vid)

    async def cleanup_expired_sessions(self):
        now = time.monotonic()
        for vid, sessions in list(self._sessions.items()):
            for sid, s in list(sessions.items()):
                if now - s.last_activity >= s.timeout:
                    await self.terminate_session(vid, sid, "expired")
        if runtime_objects.pipe_manager:
            await runtime_objects.pipe_manager.cleanup_expired_sessions()

    async def _terminate_session_internal(self, view_id: str, session_id: str, reason: str):
        pass

    async def start_periodic_cleanup(self):
        pass

    async def stop_periodic_cleanup(self):
        pass

    async def queue_command(self, view_id: str, session_id: str, command: Dict[str, Any]):
        info = self.get_session(session_id)
        if info:
            info.pending_commands.append(command)
            if command.get("type") == "scan":
                info.pending_scans.add(command.get("path"))
            return True
        return False

    async def create_sensord_job(self, view_id: str, scan_path: str, session_ids: List[str]):
        job_id = str(uuid.uuid4())
        job = SensordJobCompat(id=job_id, view_id=view_id, scan_path=scan_path, session_ids=session_ids)
        self._sensord_jobs[job_id] = job
        return job_id

    async def complete_sensord_job(self, view_id: str, session_id: str, scan_path: str, job_id: Optional[str] = None):
        target_job = None
        if job_id:
            target_job = self._sensord_jobs.get(job_id)
        else:
            for job in self._sensord_jobs.values():
                if job.view_id == view_id and job.scan_path == scan_path and job.status == "RUNNING":
                    target_job = job
                    break
        if not target_job or target_job.view_id != view_id or scan_path != target_job.scan_path:
            return False
        if session_id in target_job.expected_sessions:
            target_job.completed_sessions.add(session_id)
            if len(target_job.completed_sessions) == len(target_job.expected_sessions):
                target_job.status = "COMPLETED"
                target_job.completed_at = time.time()
            info = self.get_session(session_id)
            if info:
                info.pending_scans.discard(scan_path)
            return True
        return False

    def get_sensord_jobs(self) -> List[Dict[str, Any]]:
        return list(self._sensord_jobs.values())

    async def has_pending_job(self, view_id: str, scan_path: str) -> bool:
        for job in self._sensord_jobs.values():
            if job.view_id == view_id and job.scan_path == scan_path and job.status == "RUNNING":
                return True
        return False

# Global singleton for compatibility
session_manager = SessionManager()
