# datacast/src/datacast/runtime/pipe/data_sync.py
import asyncio
import logging
from typing import Any, Dict, Optional, List, Iterator, TYPE_CHECKING
from datacast_core.pipe import PipeState
from datacast_core.exceptions import SessionObsoletedError

if TYPE_CHECKING:
    from ..pipe import DatacastPipe

logger = logging.getLogger("datacast")

class PipeDataSyncMixin:
    """
    Mixin for DatacastPipe data plane synchronization and supervision.
    """
    
    async def _run_data_supervisor_loop(self: "DatacastPipe") -> None:
        """Dedicated supervisor loop for Data Plane tasks."""
        logger.info(f"Pipe {self.id}: Data supervisor loop started")
        
        while self.is_running():
            try:
                if not self.has_active_session():
                    await asyncio.sleep(self.data_supervisor_interval)
                    continue

                await self._supervise_data_tasks()

                await asyncio.sleep(self.data_supervisor_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Pipe {self.id}: Data supervisor loop error: {e}", exc_info=True)
                self._data_errors += 1
                await asyncio.sleep(5.0)

    async def _check_task_liveness(self: "DatacastPipe", task: asyncio.Task, task_name: str) -> bool:
        """Check if task appears stuck."""
        if task is None or task.done():
            return True
        
        now = asyncio.get_event_loop().time()
        last_active = self._task_last_active.get(task_name, now)
        
        if now - last_active > self.task_zombie_timeout:
            logger.warning(f"Pipe {self.id}: Task {task_name} appears stuck. Cancelling.")
            try:
                task.cancel()
            except Exception as e:
                logger.error(f"Pipe {self.id}: Error cancelling zombie task {task_name}: {e}")
            return False
        return True

    async def _supervise_data_tasks(self: "DatacastPipe") -> None:
        """Manage Data Tasks based on Role and state."""
        await self._check_task_liveness(self._message_sync_task, "message_sync")
        if self.current_role == "leader":
            await self._check_task_liveness(self._snapshot_task, "snapshot")
            await self._check_task_liveness(self._audit_task, "audit")
            
        if self._message_sync_task is None or self._message_sync_task.done():
            if self._message_sync_task and self._message_sync_task.done() and not self._message_sync_task.cancelled():
                try:
                    exc = self._message_sync_task.exception()
                    if exc:
                        logger.warning(f"Pipe {self.id}: Restarting crashed message sync: {exc}")
                        self._data_errors += 1
                        await asyncio.sleep(self.error_retry_interval)
                except (asyncio.CancelledError, asyncio.InvalidStateError):
                    pass
            
            logger.debug(f"Pipe {self.id}: DataSupervisor starting message sync phase")
            self._message_sync_task = asyncio.create_task(self._run_message_sync())

        if self.current_role == "leader":
            if not self._initial_snapshot_done:
                if self.is_realtime_ready:
                    if self._snapshot_task is None or self._snapshot_task.done():
                            self._snapshot_task = asyncio.create_task(self._run_snapshot_sync())
            
            if self._snapshot_task and self._snapshot_task.done():
                try:
                    self._snapshot_task.result() 
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                        logger.error(f"Pipe {self.id}: Snapshot failed: {e}")
                        self._data_errors += 1
                self._snapshot_task = None

            if self._initial_snapshot_done:
                if self.audit_interval_sec > 0 and (self._audit_task is None or self._audit_task.done()):
                    self._audit_task = asyncio.create_task(self._run_audit_loop())
                
                if self.sentinel_interval_sec > 0 and (self._sentinel_task is None or self._sentinel_task.done()):
                    self._sentinel_task = asyncio.create_task(self._run_sentinel_loop())
        
        elif self.current_role == "follower":
            await self._cancel_leader_tasks()
            if self.state & PipeState.RUNNING and not (self.state & PipeState.PAUSED):
                self._set_state((self.state | PipeState.PAUSED) & ~PipeState.SNAPSHOT_SYNC, "Follower mode - standby")
        
        else:
            if self._snapshot_task or self._audit_task or self._sentinel_task:
                await self._cancel_leader_tasks()

    async def _cancel_all_tasks(self: "DatacastPipe") -> None:
        """Cancel all pipe tasks."""
        tasks = []
        current = asyncio.current_task()
        for task in [self._heartbeat_task, self._audit_task, self._sentinel_task, self._snapshot_task, self._message_sync_task]:
            if task and task != current and not task.done():
                task.cancel()
                tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        self._heartbeat_task = None
        self._audit_task = None
        self._sentinel_task = None
        self._snapshot_task = None
        self._message_sync_task = None

    async def _handle_fatal_error(self: "DatacastPipe", error: Exception) -> None:
        """Handle fatal errors from background tasks."""
        if isinstance(error, asyncio.CancelledError):
            return
            
        logger.warning(f"Pipe {self.id} detected fatal error: {error}. Reconnecting.")
        
        if self.has_active_session():
            await self._cancel_all_tasks()
            await self.on_session_closed(self.session_id)
            
        if not isinstance(error, SessionObsoletedError):
            logger.error(f"Pipe {self.id} fatal background error: {error}", exc_info=True)
            self._data_errors += 1
            self._set_state(PipeState.RUNNING | PipeState.ERROR, str(error))

    async def _aiter_sync_phase(self: "DatacastPipe", phase_iter: Iterator[Any], queue_size: Optional[int] = None, yield_timeout: Optional[float] = None):
        """Safely wrap a synchronous iterator into an async generator."""
        from .worker import aiter_sync_phase_wrapper
        q_size = queue_size if queue_size is not None else self.iterator_queue_size
        async for item in aiter_sync_phase_wrapper(phase_iter, self.id, q_size, yield_timeout=yield_timeout):
            yield item

    async def _run_message_sync(self: "DatacastPipe") -> None:
        """Execute realtime message sync phase."""
        logger.info(f"Pipe {self.id}: Starting message sync phase")
        self._set_state(self.state | PipeState.MESSAGE_SYNC)
        try:
            if not self._bus:
                raise RuntimeError(f"Pipe {self.id}: Cannot run message sync without an EventBus.")
            await self._run_bus_message_sync()
        except Exception as e:
            await self._handle_fatal_error(e)
        finally:
            self._set_state(self.state & ~PipeState.MESSAGE_SYNC)

    async def _run_bus_message_sync(self: "DatacastPipe") -> None:
        """Execute message sync phase reading from an internal event bus."""
        from .phases import run_bus_message_sync
        await run_bus_message_sync(self)

    async def remap_to_new_bus(
        self: "DatacastPipe", 
        new_bus: Any, 
        needed_position_lost: bool
    ) -> None:
        """Remap this pipe to a new EventBus instance."""
        old_bus_id = self._bus.id if self._bus else None
        self._bus = new_bus
        
        if needed_position_lost:
            logger.warning(f"Pipe {self.id}: Position lost during bus remap. Triggering re-sync.")
            if self._message_sync_task and not self._message_sync_task.done():
                self._message_sync_task.cancel()
            
            self._set_state(
                 PipeState.RUNNING | PipeState.RECONNECTING, 
                 f"Bus remap with position loss - triggering re-sync"
             )
