# fustord/src/fustord/runtime/pipe/manager_lifecycle.py
import asyncio
import logging
from typing import List, Optional, TYPE_CHECKING
from fastapi import APIRouter
from importlib.metadata import entry_points
from fustor_core.transport.receiver import ReceiverRegistry, Receiver
from fustord.config.unified import fustord_config
from fustord.domain.view_manager.manager import get_cached_view_manager
from fustord.management.api.consistency import consistency_router

if TYPE_CHECKING:
    from ..pipe_manager import FustordPipeManager

logger = logging.getLogger("fustord.pipe_manager")

def _discover_receivers():
    """Discover and register receivers via entry points."""
    try:
        eps = entry_points(group="fustord.receivers")
        for ep in eps:
            try:
                receiver_cls = ep.load()
                ReceiverRegistry.register(ep.name, receiver_cls)
                logger.info(f"Discovered and registered receiver: {ep.name}")
            except Exception as e:
                logger.error(f"Failed to load receiver entry point '{ep.name}': {e}")
    except Exception as e:
        logger.error(f"Error discovering receiver entry points: {e}")

class ManagerLifecycleMixin:
    """
    Mixin for FustordPipeManager handling initialization, start, stop, and reload.
    """
    
    async def initialize_pipes(self: "FustordPipeManager", config_list: Optional[List[str]] = None):
        """
        Public API: Initialize pipes and receivers based on configuration.
        """
        async with self._init_lock:
            return await self._initialize_pipes_internal(config_list)

    async def _initialize_pipes_internal(self: "FustordPipeManager", config_list: Optional[List[str]] = None):
        """
        Internal initialization logic (no locking).
        """
        _discover_receivers()
        fustord_config.reload()
        self._target_pipe_ids = self._resolve_target_pipes(config_list)
        
        initialized_count = 0
        for p_id in self._target_pipe_ids:
            try:
                resolved = fustord_config.resolve_pipe_refs(p_id)
                if not resolved:
                    logger.error(f"Could not resolve configuration for pipe '{p_id}'")
                    continue

                p_cfg = resolved['pipe']
                r_cfg = resolved['receiver']
                
                if r_cfg.disabled:
                    logger.warning(f"FustordPipe '{p_id}' skipped (receiver '{p_cfg.receiver}' disabled)")
                    continue

                # 1. Initialize/Get Receiver
                r_sig = (r_cfg.driver, r_cfg.port)
                
                if r_sig not in self._receivers:
                    r_id = f"recv_{r_cfg.driver}_{r_cfg.port}"
                    receiver = ReceiverRegistry.create(
                        r_cfg.driver,
                        receiver_id=r_id,
                        bind_host=r_cfg.bind_host,
                        port=r_cfg.port,
                        config={"session_timeout_seconds": p_cfg.session_timeout_seconds}
                    )
                    
                    receiver.register_callbacks(
                        on_session_created=self._on_session_created,
                        on_event_received=self._on_event_received,
                        on_heartbeat=self._on_heartbeat,
                        on_session_closed=self._on_session_closed,
                        on_scan_complete=self._on_scan_complete
                    )
                    
                    consistency_api = APIRouter(prefix="/api/v1/pipe")
                    consistency_api.include_router(consistency_router)
                    receiver.mount_router(consistency_api)
                    
                    self._receivers[r_sig] = receiver
                    logger.info(f"Initialized {r_cfg.driver.upper()} Receiver on port {r_cfg.port}")
                
                receiver = self._receivers[r_sig]
                
                for ak in r_cfg.api_keys:
                    if ak.pipe_id == p_id:
                        receiver.register_api_key(ak.key, ak.pipe_id)

                # 2. Initialize Views
                view_handlers = []
                for v_id, v_cfg in resolved['views'].items():
                    if v_cfg.disabled:
                        continue
                    try:
                        vm = await get_cached_view_manager(v_id)
                        from fustord.domain.view_handler_adapter import create_view_handler_from_manager
                        handler = create_view_handler_from_manager(vm)
                        view_handlers.append(handler)
                    except Exception as e:
                        logger.error(f"Failed to load view group {v_id} for pipe {p_id}: {e}")

                if not view_handlers:
                    logger.warning(f"FustordPipe {p_id} has no valid views, skipping")
                    continue

                # 3. Create FustordPipe
                from fustord.stability.pipe import FustordPipe
                fustord_pipe = FustordPipe(
                    pipe_id=p_id,
                    config={
                        "view_ids": list(resolved['views'].keys()),
                        "allow_concurrent_push": p_cfg.allow_concurrent_push,
                        "session_timeout_seconds": p_cfg.session_timeout_seconds
                    },
                    view_handlers=view_handlers
                )
                
                self._pipes[p_id] = fustord_pipe
                from fustord.stability.session_bridge import create_session_bridge
                bridge = create_session_bridge(fustord_pipe)
                self._bridges[p_id] = bridge
                fustord_pipe.session_bridge = bridge
                
                logger.info(f"Initialized FustordPipe: {p_id} with {len(view_handlers)} views")
                initialized_count += 1
                
            except Exception as e:
                logger.error(f"Failed to initialize pipe {p_id}: {e}", exc_info=True)
            
        return {"initialized": initialized_count}

    def _resolve_target_pipes(self: "FustordPipeManager", config_list: Optional[List[str]]) -> List[str]:
        if config_list is None:
            return list(fustord_config.get_enabled_pipes().keys())
        
        targets = []
        for item in config_list:
            if item.endswith('.yaml') or item.endswith('.yml'):
                 pipes = fustord_config.get_pipes_from_file(item)
                 targets.extend(pipes.keys())
            else:
                if fustord_config.get_pipe(item):
                    targets.append(item)
                else:
                    logger.error(f"FustordPipe ID '{item}' not found in any loaded config")
        return targets

    async def start(self: "FustordPipeManager"):
        async with self._init_lock:
            for p_id, fustord_pipe in self._pipes.items():
                await fustord_pipe.start()
            for r_sig, receiver in self._receivers.items():
                await receiver.start()
            logger.info("fustord components started")
    
    async def stop(self: "FustordPipeManager"):
        async with self._init_lock:
            for r_sig, receiver in self._receivers.items():
                await receiver.stop()
            for p_id, fustord_pipe in self._pipes.items():
                await fustord_pipe.stop()
            logger.info("fustord components stopped")

    async def reload(self: "FustordPipeManager"):
        """Reload configuration and synchronize running pipes."""
        logger.info("Reloading fustord configuration...")
        fustord_config.reload()
        
        async with self._init_lock:
            current_pipe_ids = set(self._pipes.keys())
            modified = set()
            for p_id in current_pipe_ids:
                new_resolved = fustord_config.resolve_pipe_refs(p_id)
                if not new_resolved:
                    continue
                modified.add(p_id)

            diff = fustord_config.get_diff(current_pipe_ids)
            added = diff["added"]
            removed = diff["removed"]
            
            if not added and not removed and not modified:
                logger.info("No configuration changes affecting pipes.")
                return
                
            logger.info(f"Config change: added={added}, removed={removed}, modified={len(modified)}")
            
            for p_id in (removed | modified):
                fustord_pipe = self._pipes.pop(p_id, None)
                if fustord_pipe:
                    await fustord_pipe.stop()
                    self._bridges.pop(p_id, None)
            
            to_start = added | modified
            if to_start:
                await self._initialize_pipes_internal(list(to_start))
                for p_id in to_start:
                    if p_id in self._pipes:
                        await self._pipes[p_id].start()
            
            active_receiver_sigs = set()
            for p_id in self._pipes:
                resolved = fustord_config.resolve_pipe_refs(p_id)
                if resolved:
                    r_cfg = resolved['receiver']
                    active_receiver_sigs.add((r_cfg.driver, r_cfg.port))
            
            for sig in list(self._receivers.keys()):
                if sig not in active_receiver_sigs:
                    receiver = self._receivers.pop(sig)
                    await receiver.stop()
            
            for sig, receiver in self._receivers.items():
                await receiver.start()

        logger.info("fustord configuration reload complete.")
