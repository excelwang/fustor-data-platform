import asyncio
import time
import uuid
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Set, Tuple

logger = logging.getLogger(__name__)

@dataclass
class DatacastJob:
    job_id: str
    view_id: str
    path: str
    status: str  # "PENDING", "RUNNING", "COMPLETED", "FAILED"
    created_at: float
    completed_at: Optional[float] = None
    expected_sessions: Set[str] = field(default_factory=set)
    completed_sessions: Set[str] = field(default_factory=set)

class JobManager:
    """
    Manages tracking of asynchronous jobs (e.g. scans) sent to datacasts.
    """
    def __init__(self):
        self._jobs: Dict[str, DatacastJob] = {}
        self._path_to_job_id: Dict[Tuple[str, str], str] = {} # (view_id, path) -> job_id
        self._cleanup_task: Optional[asyncio.Task] = None

    async def create_job(self, view_id: str, path: str, session_ids: List[str]) -> str:
        """Create a new job for multiple sessions."""
        job_id = str(uuid.uuid4())[:8]
        job = DatacastJob(
            job_id=job_id,
            view_id=view_id,
            path=path,
            status="RUNNING",
            created_at=time.time(),
            expected_sessions=set(session_ids)
        )
        self._jobs[job_id] = job
        self._path_to_job_id[(view_id, path)] = job_id
        logger.info(f"Created job {job_id} for view {view_id} on path {path}")
        return job_id

    def get_jobs(self, view_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List jobs, optionally filtered by view_id."""
        results = []
        for j in self._jobs.values():
            if view_id and j.view_id != view_id:
                continue
                
            total = len(j.expected_sessions)
            done = len(j.completed_sessions)
            percentage = round((done / total * 100.0), 2) if total > 0 else 100.0
            
            results.append({
                "job_id": j.job_id,
                "view_id": j.view_id,
                "path": j.path,
                "status": j.status,
                "progress": {
                    "completed_pipes": done,
                    "total_pipes": total,
                    "percentage": percentage
                },
                "created_at": j.created_at,
                "completed_at": j.completed_at
            })
        return results

    def get_job_id_by_path(self, view_id: str, path: str) -> Optional[str]:
        return self._path_to_job_id.get((view_id, path))

    async def complete_job_for_session(self, view_id: str, session_id: str, path: str, job_id: Optional[str] = None) -> bool:
        """Mark a job as complete for a specific session."""
        if not job_id:
            job_id = self._path_to_job_id.get((view_id, path))
            
        if not job_id or job_id not in self._jobs:
            logger.warning(f"No job found for job_id={job_id} or path={path}")
            return False
            
        job = self._jobs[job_id]
        job.completed_sessions.add(session_id)
        
        # Check completion
        # Note: We don't filter active sessions here as JobManager doesn't track sessions.
        # The caller (PipeManager/SessionBridge) should handle session lifecycle.
        if job.completed_sessions.issuperset(job.expected_sessions):
            job.status = "COMPLETED"
            job.completed_at = time.time()
            logger.info(f"Job {job_id} fully COMPLETED")
            
        return True

    async def start_cleanup_loop(self, interval: int = 60):
        if self._cleanup_task and not self._cleanup_task.done():
            return
        self._cleanup_task = asyncio.create_task(self._run_cleanup_loop(interval))

    async def _run_cleanup_loop(self, interval: int):
        try:
            while True:
                await asyncio.sleep(interval)
                await self.cleanup_old_jobs()
        except asyncio.CancelledError:
            pass

    async def cleanup_old_jobs(self, ttl_seconds: int = 3600):
        now = time.time()
        to_remove = []
        for jid, job in self._jobs.items():
            if job.status in ("COMPLETED", "FAILED") and job.completed_at:
                if now - job.completed_at > ttl_seconds:
                    to_remove.append(jid)
            elif now - job.created_at > 86400: # 24h failsafe
                to_remove.append(jid)
        
        for jid in to_remove:
            job = self._jobs.pop(jid, None)
            if job:
                self._path_to_job_id.pop((job.view_id, job.path), None)
                logger.debug(f"Cleaned up old job {jid}")

# Global Instance
job_manager = JobManager()
