# fustord/src/fustord/runtime/fustord_pipe.py
"""
FustordPipe - The V2 Pipe implementation for fustord.
"""
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from sensord_core.pipe import SensorPipe, PipeState
from sensord_core.pipe.handler import ViewHandler

if TYPE_CHECKING:
    from sensord_core.pipe.context import PipeContext

from .mixins.handler_dispatch import HandlerDispatchMixin
from .mixins.session_events import SessionEventsMixin
from .mixins.ingestion import IngestionMixin

logger = logging.getLogger("fustord.pipe")


class FustordPipe(
    HandlerDispatchMixin,
    SessionEventsMixin,
    IngestionMixin,
    SensorPipe
):
    """
    fustord-side Pipe for receiving and processing events.
    """
    
    def __init__(
        self,
        pipe_id: str,
        config: Dict[str, Any],
        view_handlers: Optional[List[ViewHandler]] = None,
        context: Optional["PipeContext"] = None
    ):
        super().__init__(pipe_id, config, context)
        
        # Core configuration
        self.view_ids = config.get("view_ids")
        if not self.view_ids:
            raise ValueError(f"FustordPipe '{pipe_id}': 'view_ids' is required in config.")

        self.allow_concurrent_push = config.get("allow_concurrent_push", True)
        self.queue_batch_size = config.get("queue_batch_size", 100)
        
        # State tracking
        self._view_handlers: Dict[str, ViewHandler] = {}
        for handler in (view_handlers or []):
            self.register_view_handler(handler)
            
        self.session_bridge: Optional[Any] = None 
        
        # Background tasks
        self._processing_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._session_event_task: Optional[asyncio.Task] = None
        self._init_task: Optional[asyncio.Task] = None
        
        # Concurrency & Synchronization
        self._handlers_ready = asyncio.Event()
        self._event_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._queue_drained = asyncio.Event()
        self._queue_drained.set()
        self._lock = asyncio.Lock()
        self._active_pushes = 0
        self._cached_leader_session = None

        # Drainage Sequence Tracking (M:N support)
        self._global_ingest_seq = 0
        self._global_processed_seq = 0
        self._view_max_ingest_seq: Dict[str, int] = {}
        self._seq_condition = asyncio.Condition()
        
        # Fault isolation configuration
        self.HANDLER_TIMEOUT = config.get("handler_timeout", 30.0)
        self.MAX_HANDLER_ERRORS = config.get("max_handler_errors", 50)
        self.HANDLER_RECOVERY_INTERVAL = config.get("handler_recovery_interval", 60.0)
        
        # Handler fault isolation state
        self._handler_errors: Dict[str, int] = {}
        self._disabled_handlers: set = set()
        self._disabled_handlers_timestamps: Dict[str, float] = {}

        # Statistics
        self.statistics = {
            "events_received": 0,
            "events_processed": 0,
            "sessions_created": 0,
            "sessions_closed": 0,
            "errors": 0,
        }
    
    async def start(self) -> None:
        """Start the pipe and begin processing events."""
        if self.is_running():
            logger.warning(f"Pipe {self.id} is already running")
            return
            
        self._set_state(PipeState.RUNNING, "Starting...")
        
        self._init_task = asyncio.create_task(self._initialize_handlers())
        self._processing_task = asyncio.create_task(
            self._processing_loop(),
            name=f"fustord-pipe-{self.id}"
        )
        self._cleanup_task = asyncio.create_task(
            self._session_cleanup_loop(),
            name=f"fustord-pipe-cleanup-{self.id}"
        )
        
        if self._session_event_task is None:
            self._session_event_task = asyncio.create_task(
                self._session_event_loop(),
                name=f"fustord-pipe-session-events-{self.id}"
            )
        
        logger.info(f"FustordPipe {self.id} started")
    
    async def stop(self) -> None:
        """Stop the pipe."""
        if not self.is_running():
            return
            
        logger.info(f"Stopping FustordPipe {self.id}")
        self._set_state(PipeState.STOPPING, "Stopping...")
        
        for task in [self._processing_task, self._cleanup_task, self._session_event_task, self._init_task]:
            if task:
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        
        self._processing_task = self._cleanup_task = self._session_event_task = self._init_task = None
            
        for h_id, handler in self._view_handlers.items():
            try:
                if hasattr(handler, 'close'):
                    await handler.close()
            except Exception as e:
                logger.error(f"Error stopping handler {h_id}: {e}")
                
        self._set_state(PipeState.STOPPED, "Stopped")

    async def _initialize_handlers(self) -> None:
        """Background task to initialize handlers."""
        self._handlers_ready.clear()
        t0 = time.time()
        try:
            init_tasks = [h.initialize() for h in self._view_handlers.values() if hasattr(h, 'initialize')]
            if init_tasks:
                await asyncio.gather(*init_tasks)
            
            self._handlers_ready.set()
            self._set_state(PipeState.RUNNING, "Ready")
            logger.info(f"Pipe {self.id}: Handlers initialized in {time.time() - t0:.2f}s")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Pipe {self.id}: Init failed: {e}", exc_info=True)
            self._set_state(PipeState.RUNNING | PipeState.ERROR, f"Init failed: {e}")

    async def wait_until_ready(self, timeout: float = 30.0) -> bool:
        """Wait for background initialization to complete."""
        try:
            await asyncio.wait_for(self._handlers_ready.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def get_dto(self) -> Dict[str, Any]:
        """Get pipestatus as a dictionary."""
        from fustord.domain.view_state_manager import view_state_manager
        
        all_sessions = await self.get_all_sessions()
        leaders = {}
        for vid in self.view_ids:
            l_sid = await view_state_manager.get_leader(vid)
            if l_sid:
                 leaders[vid] = l_sid

        return {
            "id": self.id,
            "view_ids": self.view_ids,
            "state": self.state.name if hasattr(self.state, 'name') else str(self.state),
            "info": self.info,
            "view_handlers": self.get_available_views(),
            "active_sessions": len(all_sessions),
            "leaders": {k: v for k, v in leaders.items() if v},
            "leader_session": next((v for v in leaders.values() if v), None),
            "statistics": self.statistics.copy(),
            "queue_size": self._event_queue.qsize(),
            "is_ready": self._handlers_ready.is_set(),
        }

    def __str__(self) -> str:
        return f"FustordPipe({self.id}, state={self.state.name})"
