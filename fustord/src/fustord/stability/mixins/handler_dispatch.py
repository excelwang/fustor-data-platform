# fustord/src/fustord/runtime/pipe/handler_dispatch.py
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
from sensord_core.pipe.handler import ViewHandler
from sensord_core.event import EventBase
from sensord_core.common.metrics import get_metrics
from sensord_core.pipe import PipeState

logger = logging.getLogger("fustord.pipe")

class HandlerDispatchMixin:
    """
    Mixin for FustordPipe handling ViewHandler dispatch and management.
    """
    
    def register_view_handler(self, handler: ViewHandler) -> None:
        """
        Register a view handler for processing events.
        """
        handler_id = handler.id
        self._view_handlers[handler_id] = handler
        logger.debug(f"Registered view handler: {handler_id}")
    
    def get_view_handler(self, handler_id: str) -> Optional[ViewHandler]:
        """Get a view handler by ID."""
        return self._view_handlers.get(handler_id)

    def find_handler_for_view(self, view_id: str) -> Optional[ViewHandler]:
        """
        Find handler associated with a view_id, regardless of handler_id naming.
        """
        for handler in self._view_handlers.values():
            # Check ViewManagerAdapter pattern
            h_manager = getattr(handler, '_manager', None)
            if h_manager and getattr(h_manager, 'view_id', None) == view_id:
                return handler
            
            # Check ViewDriverAdapter pattern (has view_id property)
            h_view_id = getattr(handler, 'view_id', None)
            if h_view_id == view_id:
                return handler
            
            # Check driver pattern (FSViewDriver has view_id attribute)
            h_driver = getattr(handler, '_driver', None)
            if h_driver and getattr(h_driver, 'view_id', None) == view_id:
                return handler
            
            # Check config (ViewHandler base)
            if isinstance(handler.config, dict) and handler.config.get('view_id') == view_id:
                return handler

            # Fallback: check handler.id
            if handler.id == view_id:
                return handler
                
        return None
    
    def get_available_views(self) -> List[str]:
        """Get list of available view handler IDs."""
        return list(self._view_handlers.keys())

    async def _processing_loop(self) -> None:
        """Background loop for processing queued events."""
        logger.debug(f"Processing loop started for pipe {self.id}")
        
        while True:
            try:
                # 1. Wait for readiness (don't process events if views aren't ready)
                if not self._handlers_ready.is_set():
                    logger.debug(f"Pipe {self.id} processing loop: waiting for handlers...")
                    if not await self.wait_until_ready(timeout=60.0):
                        await asyncio.sleep(5.0)
                        continue

                # Wait for events in queue
                event_batch = await self._event_queue.get()
                
                if event_batch is None:  # Shutdown signal
                    break
                
                # Process each event
                t0 = time.time()
                
                # Check if batch is tuple (events, seq) or just events (legacy)
                current_seq = None
                if isinstance(event_batch, tuple):
                    events_to_process, current_seq = event_batch
                else:
                    events_to_process = event_batch

                for event in events_to_process:
                    await self._dispatch_to_handlers(event)
                    self.statistics["events_processed"] += 1
                
                duration = time.time() - t0
                get_metrics().counter("fustor.fustord.events_processed", len(events_to_process), {"pipe": self.id})
                get_metrics().histogram("fustor.fustord.processing_latency", duration, {"pipe": self.id})
                
                # Update sequence progress
                if current_seq is not None:
                    async with self._seq_condition:
                        self._global_processed_seq = current_seq
                        self._seq_condition.notify_all()
                
                self._event_queue.task_done()
                
                # Signal queue drain if empty and no pushes active
                if self._event_queue.empty():
                    async with self._lock:
                        if self._active_pushes == 0:
                            self._queue_drained.set()
                
            except asyncio.CancelledError:
                logger.debug(f"Processing loop cancelled for pipe {self.id}")
                break
            except Exception as e:
                logger.error(f"Error in processing loop: {e}", exc_info=True)
                self.statistics["errors"] += 1
                await asyncio.sleep(0.1)
    
    async def _dispatch_to_handlers(self, event: EventBase) -> None:
        """
        Dispatch an event to matching view handlers with fault isolation.
        """
        for handler_id, handler in self._view_handlers.items():
            # 1. Schema Routing Check
            if handler.schema_name and handler.schema_name != event.event_schema:
                if handler.schema_name != "view-manager": # Special case for aggregator
                    continue

            # Skip disabled handlers unless they have recovered
            if handler_id in self._disabled_handlers:
                if not self._attempt_handler_recovery(handler_id):
                    continue
                
            try:
                if hasattr(handler, 'process_event'):
                    # Apply timeout protection
                    try:
                        if event.metadata is None:
                            event.metadata = {}
                        event.metadata.setdefault("pipe_id", self.id)
                        
                        success = await asyncio.wait_for(
                            handler.process_event(event),
                            timeout=self.HANDLER_TIMEOUT
                        )
                        if not success:
                            self._record_handler_error(handler_id, "Returned False")
                            logger.warning(f"Handler {handler_id} returned False for event processing")
                    except asyncio.TimeoutError:
                        self._record_handler_error(handler_id, f"Timeout after {self.HANDLER_TIMEOUT}s")
                        logger.error(f"Handler {handler_id} timed out processing event")
            except Exception as e:
                self._record_handler_error(handler_id, str(e))
                logger.error(f"Error in handler {handler_id}: {e}", exc_info=True)

    def _attempt_handler_recovery(self, handler_id: str) -> bool:
        """Check if a disabled handler can be re-enabled."""
        last_disabled = self._disabled_handlers_timestamps.get(handler_id, 0)
        if time.time() - last_disabled > self.HANDLER_RECOVERY_INTERVAL:
            self._disabled_handlers.remove(handler_id)
            # Reset error count to give it a fresh start
            self._handler_errors[handler_id] = 0
            if handler_id in self._disabled_handlers_timestamps:
                del self._disabled_handlers_timestamps[handler_id]
            logger.debug(f"Handler {handler_id} re-enabled after cooldown period.")
            return True
        return False
    
    def _record_handler_error(self, handler_id: str, reason: str) -> None:
        """Record a handler error and disable if threshold exceeded."""
        self.statistics["errors"] += 1
        self._handler_errors[handler_id] = self._handler_errors.get(handler_id, 0) + 1
        
        if self._handler_errors[handler_id] >= self.MAX_HANDLER_ERRORS:
            if handler_id not in self._disabled_handlers:
                self._disabled_handlers.add(handler_id)
                self._disabled_handlers_timestamps[handler_id] = time.time()
                logger.warning(
                    f"Handler {handler_id} disabled after {self._handler_errors[handler_id]} errors. "
                    f"Last error: {reason}"
                )

    def get_view(self, handler_id: str, **kwargs) -> Any:
        """
        Get data from a specific view handler.
        """
        handler = self._view_handlers.get(handler_id)
        if handler and hasattr(handler, 'get_data_view'):
            return handler.get_data_view(**kwargs)
        return None

    async def get_aggregated_stats(self) -> Dict[str, Any]:
        """Get aggregated statistics from all view handlers."""
        pipe_stats = self.statistics.copy()
        pipe_stats["queue_size"] = self._event_queue.qsize()
        
        stats = {
            "pipe": pipe_stats,
            "views": {},
        }
        
        for handler_id, handler in self._view_handlers.items():
            if hasattr(handler, 'get_stats'):
                res = handler.get_stats()
                if asyncio.iscoroutine(res):
                    stats["views"][handler_id] = await res
                else:
                    stats["views"][handler_id] = res
        
        return stats
