"""
DatacastPipe orchestrates the flow: Source -> Sender
"""
import asyncio
import logging
from typing import Optional, Any, Dict, List, TYPE_CHECKING

from datacast_core.pipe import DatacastPipe, PipeState
from datacast_core.pipe.handler import SourceHandler
from datacast_core.pipe.sender import SenderHandler
from datacast_core.pipe.mapper import EventMapper
from datacast_core.models.states import PipeInstanceDTO

if TYPE_CHECKING:
    from datacast_core.pipe import PipeContext
    from datacast.domain.event_bus import EventBusInstanceRuntime

from .mixins.lifecycle import PipeLifecycleMixin
from .mixins.leader import PipeLeaderMixin
from .mixins.heartbeat import PipeHeartbeatMixin
from .mixins.data_sync import PipeDataSyncMixin

logger = logging.getLogger("datacast")


class DatacastPipe(
    PipeLifecycleMixin,
    PipeLeaderMixin,
    PipeHeartbeatMixin,
    PipeDataSyncMixin,
    DatacastPipe
):
    """
    datacast-side Pipe implementation.
    """
    
    # Class-level cache for management processor
    _mgmt_processor_factory = None
    _mgmt_processor_checked = False

    def __init__(
        self,
        pipe_id: str,
        config: Dict[str, Any],
        source_handler: SourceHandler,
        sender_handler: SenderHandler,
        event_bus: Optional["EventBusInstanceRuntime"] = None,
        bus_manager: Any = None,
        context: Optional["PipeContext"] = None
    ):
        super().__init__(pipe_id, config, context)
        
        self.task_id: Optional[str] = None
        self.source_handler = source_handler
        self.sender_handler = sender_handler
        self._bus = event_bus
        self._bus_manager = bus_manager
        
        if self._bus is None:
            logger.warning(f"Pipe {pipe_id}: Initialized without a bus.")
        
        self._mapper = EventMapper(config.get("fields_mapping", []))
        self.current_role: Optional[str] = None
        
        # Task handles
        self._main_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._snapshot_task: Optional[asyncio.Task] = None
        self._message_sync_task: Optional[asyncio.Task] = None
        self._audit_task: Optional[asyncio.Task] = None
        self._sentinel_task: Optional[asyncio.Task] = None
        self._data_supervisor_task: Optional[asyncio.Task] = None
        
        # Configuration
        self.session_timeout_seconds = config.get("session_timeout_seconds")
        self.heartbeat_interval_sec = 3.0
        self.audit_interval_sec = config.get("audit_interval_sec", 600)
        self.sentinel_interval_sec = config.get("sentinel_interval_sec", 120)
        self.batch_size = config.get("batch_size", 100)
        self.iterator_queue_size = config.get("iterator_queue_size", 1000)

        # Timing and backoff
        self.control_loop_interval = config.get("control_loop_interval", 1.0)
        self.follower_standby_interval = config.get("follower_standby_interval", 1.0)
        self.error_retry_interval = config.get("error_retry_interval", 1.0)
        self.max_consecutive_errors = int(config.get("max_consecutive_errors", 5))
        self.backoff_multiplier = config.get("backoff_multiplier", 2.0)
        self.max_backoff_seconds = config.get("max_backoff_seconds", 60.0)
        self.data_supervisor_interval = config.get("data_supervisor_interval", 1.0)
        self.task_zombie_timeout = config.get("task_zombie_timeout", 300)

        self._task_last_active: Dict[str, float] = {}
        self.statistics: Dict[str, Any] = {
            "events_pushed": 0,
            "last_pushed_event_id": None
        }
        self.audit_context: Dict[str, Any] = {}
        self._control_errors = 0
        self._data_errors = 0
        self._last_heartbeat_at = 0.0
        self.is_realtime_ready = False
        self._initial_snapshot_done = False

        self._mgmt = self._load_mgmt_processor()

    def _set_state(self, new_state: PipeState, info: Optional[str] = None):
        """Update pipestate only if it actually changed."""
        if self.state == new_state and self.info == info:
            return
        super()._set_state(new_state, info)

    async def start(self) -> None:
        """Start the pipe."""
        if self.is_running():
            logger.warning(f"Pipe {self.id} is already running")
            return
        
        self._set_state(PipeState.INITIALIZING, "Starting pipe...")
        
        try:
            await self.source_handler.initialize()
            await self.sender_handler.initialize()
        except Exception as e:
            self._set_state(PipeState.ERROR, f"Initialization failed: {e}")
            logger.error(f"Pipe {self.id} initialization failed: {e}")
            return

        self._main_task = asyncio.create_task(self._run_control_loop())
        self._data_supervisor_task = asyncio.create_task(self._run_data_supervisor_loop())

    async def stop(self) -> None:
        """Stop the pipe gracefully."""
        if self.state == PipeState.STOPPED:
            return
        
        self._set_state(PipeState.STOPPING, "Stopping...")
        
        tasks_to_cancel = [
            self._main_task, self._data_supervisor_task, self._heartbeat_task,
            self._snapshot_task, self._message_sync_task, self._audit_task, self._sentinel_task
        ]
        
        for task in tasks_to_cancel:
            if task and not task.done():
                task.cancel()
        
        active_tasks = [t for t in tasks_to_cancel if t]
        if active_tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*active_tasks, return_exceptions=True), timeout=2.0)
            except asyncio.TimeoutError:
                pass
        
        if self.has_active_session():
            try:
                await self.sender_handler.close_session(self.session_id)
            except Exception:
                pass
            await self.on_session_closed(self.session_id)
        
        try:
            await self.source_handler.close()
            await self.sender_handler.close()
        except Exception:
            pass

        self._set_state(PipeState.STOPPED, "Stopped")

    @property
    def bus(self) -> Optional["EventBusInstanceRuntime"]:
        """Legacy access to event bus."""
        return self._bus

    def get_dto(self) -> PipeInstanceDTO:
        """Get pipe data transfer object representation."""
        from datacast_core.models.states import PipeState as TaskState
        
        state = TaskState.STOPPED
        if self.state & PipeState.SNAPSHOT_SYNC:
            state |= TaskState.SNAPSHOT_SYNC
        elif self.state & PipeState.MESSAGE_SYNC:
            state |= TaskState.MESSAGE_SYNC
        elif self.state & PipeState.AUDIT_PHASE:
            state |= TaskState.AUDIT_PHASE
        
        if self.state & PipeState.CONF_OUTDATED:
            state |= TaskState.RUNNING_CONF_OUTDATE
        
        if self.state & PipeState.ERROR:
            state |= TaskState.ERROR
        
        if self.state & PipeState.RECONNECTING:
            state |= TaskState.RECONNECTING

        if self.state & PipeState.RUNNING and state == TaskState.STOPPED:
            state = TaskState.STARTING

        return PipeInstanceDTO(
            id=self.id,
            state=state,
            info=self.info or "",
            statistics=self.statistics.copy(),
            bus_id=self._bus.id if self._bus else None,
            task_id=self.task_id,
            current_role=self.current_role,
        )
