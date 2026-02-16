# fustord/src/fustord/runtime/pipe/ingestion.py
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
from fustor_core.event import EventBase
from fustor_core.common.metrics import get_metrics

from fustord.domain.view_state_manager import view_state_manager

logger = logging.getLogger("fustord.pipe")

class IngestionMixin:
    """
    Mixin for FustordPipe handling event ingestion and drainage.
    """
    
    async def process_events(
        self,
        events: List[Any],
        session_id: str,
        source_type: str = "message",
        is_end: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Process a batch of events from an sensord.
        """
        if not self.is_running():
            return {"success": False, "error": "Pipe not running"}
        
        async with self._lock:
            self._active_pushes += 1
            self._queue_drained.clear()

        try:
            # Wait for readiness before accepting events
            if not await self.wait_until_ready(timeout=60.0):
                return {"success": False, "error": "Pipe not ready (initialization timeout)"}

            self.statistics["events_received"] += len(events)
            logger.debug(f"Pipe {self.id}: Received {len(events)} events from {session_id} (source={source_type})")
            
            # Convert dict events to EventBase if needed
            processed_events = []
            skipped_count = 0
            
            for event in events:
                if isinstance(event, dict):
                    try:
                        ev = EventBase.model_validate(event)
                        processed_events.append(ev)
                    except Exception as e:
                        logger.warning(f"Pipe {self.id}: Skipping malformed event in batch: {e}")
                        self.statistics["errors"] += 1
                        skipped_count += 1
                else:
                    processed_events.append(event)
            
            if skipped_count > 0:
                logger.warning(f"Pipe {self.id}: Skipped {skipped_count}/{len(events)} malformed events in batch")
            
            # Inject Lineage Info from cache
            if self.session_bridge:
                lineage_meta = self.session_bridge.store.get_lineage(session_id)
                if lineage_meta:
                    for ev in processed_events:
                        if ev.metadata is None:
                            ev.metadata = lineage_meta.copy()
                        else:
                            ev.metadata.update(lineage_meta)

            # Handle special phases
            if source_type == "config_report":
                metadata = kwargs.get("metadata", {})
                config_yaml = metadata.get("config_yaml")
                if config_yaml:
                    for vid in self.view_ids:
                        session_info = await self.get_session_info(session_id)
                        if session_info:
                            session_info.reported_config = config_yaml
                    logger.info(f"Pipe {self.id}: Cached reported config for session {session_id}")
                return {"success": True, "message": "Config cached"}

            # Queue for processing with Sequence Tracking
            if processed_events:
                seq = 0
                async with self._seq_condition:
                    self._global_ingest_seq += 1
                    seq = self._global_ingest_seq
                    for vid in self.view_ids:
                        self._view_max_ingest_seq[vid] = seq
                
                get_metrics().counter("fustor.fustord.events_received", len(processed_events), {"pipe": self.id, "source": source_type})
                await self._event_queue.put((processed_events, seq))
            
            is_snapshot_end = is_end or kwargs.get("is_snapshot_end", False)
            if source_type == "snapshot" and is_snapshot_end:
                is_leader = False
                for vid in self.view_ids:
                    if await view_state_manager.is_leader(vid, session_id):
                        is_leader = True
                        break

                if is_leader:
                    logger.info(f"Pipe {self.id}: Received SNAPSHOT end signal from leader session {session_id}. Draining...")
                    await self.wait_for_drain(timeout=30.0, target_active_pushes=1)
                    
                    for vid in self.view_ids:
                        await view_state_manager.set_snapshot_complete(vid, session_id)
                    
                    for handler in self._view_handlers.values():
                        if hasattr(handler, 'on_snapshot_complete'):
                            await handler.on_snapshot_complete(session_id=session_id, metadata=kwargs.get("metadata"))
                    # Support M:N view completion
                    for h_id, handler in self._view_handlers.items():
                        h_view_id = getattr(handler, 'view_id', None)
                        if not h_view_id and hasattr(handler, 'manager'):
                            h_view_id = getattr(handler.manager, 'view_id', None)
                        if h_view_id and str(h_view_id) not in self.view_ids:
                            await view_state_manager.set_snapshot_complete(str(h_view_id), session_id)
                else:
                    logger.warning(f"Pipe {self.id}: Received snapshot end signal from non-leader session {session_id}. Ignored.")

            # Handle audit completion signal
            if source_type == "audit" and is_end:
                logger.info(f"Pipe {self.id}: Received AUDIT end signal from session {session_id}. Finalizing...")
                await self.wait_for_drain(timeout=30.0, target_active_pushes=1)
                
                for handler in self._view_handlers.values():
                    if hasattr(handler, 'handle_audit_end'):
                        try:
                            await handler.handle_audit_end()
                        except Exception as e:
                            logger.error(f"Pipe {self.id}: Error in handle_audit_end for {handler}: {e}")

            # Handle command results
            if source_type == "command_result":
                if self.session_bridge:
                    for ev in processed_events:
                        cmd_id = ev.get("id") or ev.metadata.get("command_id")
                        if cmd_id:
                            self.session_bridge.resolve_command(cmd_id, ev)

            return {
                "success": True,
                "count": len(processed_events),
                "skipped": skipped_count,
                "source_type": source_type,
            }
        finally:
            async with self._lock:
                self._active_pushes -= 1
                if self._active_pushes == 0 and self._event_queue.empty():
                    self._queue_drained.set()

    async def wait_for_drain(self, timeout: float = None, target_active_pushes: int = 0, view_id: str = None) -> bool:
        """
        Wait until the event queue is empty and active pushes reach target.
        """
        start_time = time.time()
        
        if view_id:
            target_seq = 0
            async with self._seq_condition:
                target_seq = self._view_max_ingest_seq.get(view_id, 0)
            
            if target_seq == 0:
                return True 
            
            try:
                async with self._seq_condition:
                    await asyncio.wait_for(
                        self._seq_condition.wait_for(lambda: self._global_processed_seq >= target_seq),
                        timeout=timeout
                    )
                return True
            except asyncio.TimeoutError:
                return False

        try:
            if timeout:
                await asyncio.wait_for(self._event_queue.join(), timeout=timeout)
            else:
                await self._event_queue.join()
        except asyncio.TimeoutError:
            return False
            
        if target_active_pushes == 0:
            async with self._lock:
                if self._active_pushes == 0:
                    return True
            try:
                if timeout:
                    remaining = timeout - (time.time() - start_time)
                    if remaining <= 0: return False
                    await asyncio.wait_for(self._queue_drained.wait(), timeout=remaining)
                else:
                    await self._queue_drained.wait()
                return True
            except asyncio.TimeoutError:
                return False
        else:
            while True:
                async with self._lock:
                    if self._active_pushes <= target_active_pushes:
                        return True
                
                if timeout and (time.time() - start_time > timeout):
                    return False
                
                await asyncio.sleep(0.1)
