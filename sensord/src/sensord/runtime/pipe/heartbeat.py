# sensord/src/sensord/runtime/pipe/heartbeat.py
import asyncio
import logging
from typing import Any, Dict, Optional, List, TYPE_CHECKING
from fustor_core.exceptions import SessionObsoletedError
from fustor_core.common.metrics import get_metrics

if TYPE_CHECKING:
    from ..sensord_pipe import SensordPipe

logger = logging.getLogger("sensord")

class PipeHeartbeatMixin:
    """
    Mixin for SensordPipe heartbeat and command management.
    """
    
    def map_batch(self: "SensordPipe", events: List[Any]) -> List[Any]:
        """Map events using the configured field mapper."""
        return self._mapper.map_batch(events)

    async def _handle_commands(self: "SensordPipe", commands: List[Dict[str, Any]]) -> None:
        """Process commands via management extension if available."""
        if not commands:
            return
            
        if self._mgmt:
            await self._mgmt.process_commands(self, commands)
        else:
            logger.debug(f"Pipe {self.id}: Received {len(commands)} commands but management extension is not installed.")

    def _load_mgmt_processor(self: "SensordPipe") -> Optional[Any]:
        """Dynamically load the L3 management processor if available."""
        # Use class-level cache from SensordPipe (self.__class__)
        cls = self.__class__
        if not hasattr(cls, '_mgmt_processor_factory'):
            cls._mgmt_processor_factory = None
            cls._mgmt_processor_checked = False

        if cls._mgmt_processor_factory:
             try:
                 return cls._mgmt_processor_factory()
             except Exception as e:
                 logger.error(f"Pipe {self.id}: Failed to instantiate cached management processor: {e}")
                 return None
                 
        if cls._mgmt_processor_checked:
            return None

        from importlib.metadata import entry_points
        try:
            eps = entry_points(group="sensord.command_processors")
            for ep in eps:
                try:
                    processor_class = ep.load()
                    if callable(processor_class):
                        cls._mgmt_processor_factory = processor_class
                        cls._mgmt_processor_checked = True
                        logger.info(f"Loaded management extension '{ep.name}' (cached)")
                        return processor_class()
                except Exception as e:
                    logger.warning(f"Pipe {self.id}: Failed to load management extension '{ep.name}': {e}")
        except Exception:
            pass
            
        cls._mgmt_processor_checked = True
        return None

    def _build_sensord_status(self: "SensordPipe") -> Dict[str, Any]:
        """Build sensord status dict for heartbeat reporting."""
        sensord_id = None
        if self.task_id and ":" in self.task_id:
            sensord_id = self.task_id.split(":")[0]

        source_is_healthy = True
        if hasattr(self.source_handler, 'is_healthy'):
             source_is_healthy = self.source_handler.is_healthy()

        return {
            "sensord_id": sensord_id,
            "pipe_id": self.id,
            "state": str(self.state),
            "role": self.current_role,
            "events_pushed": self.statistics.get("events_pushed", 0),
            "is_realtime_ready": self.is_realtime_ready,
            "component_health": {
                "source": {
                    "status": "ok" if source_is_healthy else "error",
                    "restart_count": getattr(self.source_handler, '_restart_count', 0),
                },
                "sender": {
                    "status": "ok" if self._control_errors == 0 else "degraded",
                    "consecutive_errors": self._control_errors,
                },
                "data_plane": {
                    "status": "ok" if self._data_errors == 0 else "degraded",
                    "consecutive_errors": self._data_errors,
                }
            }
        }

    async def _update_role_from_response(self: "SensordPipe", response: Dict[str, Any]) -> None:
        """Update role and heartbeat timer based on server response."""
        new_role = response.get("role")
        if new_role and new_role != self.current_role:
            get_metrics().gauge("fustor.sensord.role", 1 if new_role == "leader" else 0, {"pipe": self.id, "role": new_role})
            await self._handle_role_change(new_role)
        elif new_role:
             get_metrics().gauge("fustor.sensord.role", 1 if new_role == "leader" else 0, {"pipe": self.id, "role": new_role})
        
        self._last_heartbeat_at = asyncio.get_event_loop().time()

    async def _handle_role_change(self: "SensordPipe", new_role: str) -> None:
        """Handle role transition logic."""
        logger.info(f"Pipe {self.id}: Role changed from {self.current_role} to {new_role}")
        self.current_role = new_role
        
        if new_role == "leader":
            logger.info(f"Pipe {self.id}: Promoted to LEADER. Clearing audit cache.")
            self.audit_context.clear()
        
        if new_role != "leader":
            await self._cancel_leader_tasks()

    async def on_session_created(self: "SensordPipe", session_id: str, **kwargs) -> None:
        """Handle session creation."""
        self.current_role = kwargs.get("role", "follower")
        self.session_id = session_id
        
        server_timeout = kwargs.get("session_timeout_seconds")
        if server_timeout:
            auto_interval = max(0.1, server_timeout / 3.0)
            logger.debug(f"Pipe {self.id}: Calculated heartbeat interval: {auto_interval}s")
            self.heartbeat_interval_sec = auto_interval
        
        server_audit = kwargs.get("audit_interval_sec")
        if server_audit is not None:
            self.audit_interval_sec = float(server_audit)

        server_sentinel = kwargs.get("sentinel_interval_sec")
        if server_sentinel is not None:
             self.sentinel_interval_sec = float(server_sentinel)
            
        self._initial_snapshot_done = False
        self.is_realtime_ready = False
        
        if self._bus and getattr(self._bus, 'failed', False):
            try:
                self._bus.recover()
            except Exception as e:
                logger.warning(f"Pipe {self.id}: Bus recovery failed: {e}")
        
        if self._heartbeat_task and self._heartbeat_task.done():
            self._heartbeat_task = None
            
        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(self._run_heartbeat_loop())
        
        logger.info(f"Pipe {self.id}: Session {session_id} created, role={self.current_role}")

    async def on_session_closed(self: "SensordPipe", session_id: str) -> None:
        """Handle session closure."""
        logger.info(f"Pipe {self.id}: Session {session_id} closed")
        self.session_id = None
        self.current_role = None
        self.is_realtime_ready = False
        self._initial_snapshot_done = False
        self.audit_context.clear()
        
        self._heartbeat_task = None
        self._snapshot_task = None
        self._audit_task = None
        self._sentinel_task = None
        self._message_sync_task = None

    async def _run_heartbeat_loop(self: "SensordPipe") -> None:
        """Maintain session through periodic heartbeats."""
        while self.has_active_session():
            try:
                loop = asyncio.get_event_loop()
                now = loop.time()
                
                elapsed = now - self._last_heartbeat_at
                if elapsed < self.heartbeat_interval_sec:
                    await asyncio.sleep(min(1.0, self.heartbeat_interval_sec - elapsed))
                    continue

                response = await self.sender_handler.send_heartbeat(
                    self.session_id,
                    can_realtime=self.is_realtime_ready,
                    sensord_status=self._build_sensord_status(),
                )
                await self._update_role_from_response(response)
                
                if response and "commands" in response:
                    await self._handle_commands(response["commands"])
                
                if self._control_errors > 0:
                    self._control_errors = 0
                
            except SessionObsoletedError as e:
                await self._handle_fatal_error(e)
                break
            except Exception as e:
                backoff = self._handle_control_error(e, "heartbeat")
                await asyncio.sleep(max(self.heartbeat_interval_sec, backoff))
