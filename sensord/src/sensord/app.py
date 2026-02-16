# src/sensord/app.py
"""
sensord Application - Main orchestrator using new unified pipe config V2.
"""
import asyncio
import json
import logging
import os
import shutil
from typing import Dict, Any, Optional, List

from sensord_core.common import get_fustor_home_dir
# ID generation is removed per config-only requirement

# Import config services
from .domain.configs.pipe import PipeConfigService
from .domain.configs.source import SourceConfigService
from .domain.configs.sender import SenderConfigService
from sensord_core.models.config import AppConfig

from .config.unified import sensord_config, SensordPipeConfig

# Import driver and instance services
from .domain.drivers.source_driver import SourceDriverService
from .domain.drivers.sender_driver import SenderDriverService
from .domain.event_bus import EventBusManager
from .stability.pipe_manager import PipeManager
from .stability.sender_adapter import SenderHandlerAdapter
from .domain.source_handler_adapter import SourceHandlerAdapter

# State file path
HOME_FUSTOR_DIR = get_fustor_home_dir()
STATE_FILE_PATH = os.path.join(HOME_FUSTOR_DIR, "sensord-state.json")


class App:
    """
    Main application orchestrator.
    
    Refactored to use unified sensordConfigLoader V2 (Dict-based).
    """
    
    def __init__(self, config_list: Optional[List[str]] = None):
        """
        Initialize the application.
        
        Args:
            config_list: List of pipe IDs or filenames (e.g., 'set2.yaml') to start.
                        If None, loads 'default.yaml'.
        """
        self.logger = logging.getLogger("sensord")
        self.logger.info("Initializing application...")
        
        
        # Determine which pipes to start based on config_list
        self._target_pipe_ids = self._resolve_target_pipes(config_list)
        
        # Driver services
        self.source_driver_service = SourceDriverService()
        self.sender_driver_service = SenderDriverService()
        
        # Instance services
        self.event_bus_manager = EventBusManager(
            {},  # Will be populated dynamically
            self.source_driver_service
        )
        
        # self.pipe_runtime is now a property delegating to pipe_manager
        
        # Track config signature for reload detection
        self.config_signature = sensord_config.get_config_signature()
        
        # Initialize Config Services (adapter for PipeManager)
        # Note: In future, PipeManager might use sensord_config directly
        self.app_config = AppConfig()
        self.source_config_service = SourceConfigService(self.app_config)
        self.sender_config_service = SenderConfigService(self.app_config)
        self.pipe_config_service = PipeConfigService(
            self.app_config, 
            self.source_config_service, 
            self.sender_config_service
        )
        
        # Initialize PipeManager
        self.pipe_manager = PipeManager(
            self.pipe_config_service,
            self.source_config_service,
            self.sender_config_service,
            self.event_bus_manager,
            self.sender_driver_service,
            self.source_driver_service
        )
        
        # Set circular dependency
        self.event_bus_manager.set_dependencies(self.pipe_manager)

        self.logger.info(f"Target pipes: {self._target_pipe_ids}")
    
    @property
    def pipe_runtime(self) -> Dict[str, Any]:
        """Expose running pipes from PipeManager."""
        return self.pipe_manager.pool

    def _resolve_target_pipes(self, config_list: Optional[List[str]]) -> List[str]:
        """Resolve which pipe IDs to start."""
        if config_list is None:
            # Default behavior: only pipes from default.yaml
            all_default = sensord_config.get_default_pipes()
            targets = []
            for pid, pcfg in all_default.items():
                source = sensord_config.get_source(pcfg.source)
                if source and not source.disabled:
                    targets.append(pid)
            return targets
        
        targets = []
        for item in config_list:
            if item.endswith('.yaml') or item.endswith('.yml'):
                # Load all pipes from specific file
                pipes = sensord_config.get_pipes_from_file(item)
                if not pipes:
                    self.logger.warning(f"No pipes found in file '{item}'")
                targets.extend(pipes.keys())
            else:
                # Assume it's a pipe ID
                if sensord_config.get_pipe(item):
                    targets.append(item)
                else:
                    self.logger.error(f"Pipe ID '{item}' not found in any loaded config")
        return targets
    
    async def startup(self):
        """Start the application and target pipes."""
        self.logger.info("Starting application...")
        
        # Runtime validation for redundancy
        self._validate_runtime_uniqueness(self._target_pipe_ids)
        
        try:
            # Batch start via PipeManager
            results = await self.pipe_manager.start_all_enabled()
            self.logger.info(f"Startup results: {results}")
        except Exception as e:
            self.logger.error(f"Failed to start pipes: {e}", exc_info=True)

    def _validate_runtime_uniqueness(self, pipe_ids: List[str]):
        """Ensure no two pipes in the list share the same source and sender."""
        seen_pairs = {} # (source, sender) -> pipe_id
        for pid in pipe_ids:
            resolved = sensord_config.resolve_pipe_refs(pid)
            if not resolved:
                continue
            p_cfg = resolved['pipe']
            pair = (p_cfg.source, p_cfg.sender)
            if pair in seen_pairs:
                error_msg = (
                    f"CRITICAL: Redundant configuration detected in runtime. "
                    f"Pipe '{pid}' and Pipe '{seen_pairs[pair]}' both use "
                    f"Source '{pair[0]}' and Sender '{pair[1]}'. "
                    f"sensord will not start conflicting pipes to prevent data corruption."
                )
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            seen_pairs[pair] = pid
    
    async def _start_pipe(self, pipe_id: str):
        """Delegated to PipeManager now."""
        await self.pipe_manager.start_one(pipe_id, raise_on_error=True)

    async def _stop_pipe(self, pipe_id: str):
        """Delegated to PipeManager now."""
        await self.pipe_manager.stop_one(pipe_id)
    
    async def shutdown(self):
        """Gracefully shutdown all pipes."""
        self.logger.info("Shutting down application...")
        
        if self.pipe_manager:
            await self.pipe_manager.stop_all()
        
        await self.event_bus_manager.release_all_unused_buses()
        await self._save_state()
        self.logger.info("Application shutdown complete")

    async def reload_config(self):
        """
        Reload configuration from disk and synchronize running pipes.
        Triggered by SIGHUP.
        """
        self.logger.info("Reloading configuration...")
        sensord_config.reload()
        
        new_signature = sensord_config.get_config_signature()
        if new_signature == self.config_signature:
             self.logger.info("Configuration signature unchanged. Skipping reload logic.")
             return
             
        self.logger.info(f"Configuration changed (sig={new_signature[:8]}). Calculating diff...")
        self.config_signature = new_signature

        # Get diff between currently running pipes and new enabled pipes
        # Note: We only auto-reload pipes that were either in the original startup list 
        # or are in default.yaml if no list was provided.
        # For simplicity, we diff against ALL enabled pipes now, 
        # but a production implementation might restrict this to the original 'namespace'.
        
        current_running_ids = set(self.pipe_runtime.keys())
        diff = sensord_config.get_diff(current_running_ids)
        
        added = diff["added"]
        removed = diff["removed"]
        
        if not added and not removed:
            self.logger.info("No configuration changes affecting pipes.")
            return
            
        self.logger.info(f"Config change detected: added={added}, removed={removed}")
        
        # 0. Runtime validation for combined set (existing minus removed plus added)
        remaining = current_running_ids - removed
        to_validate = list(remaining | added)
        try:
            self._validate_runtime_uniqueness(to_validate)
        except ValueError as e:
            self.logger.error(f"Reload aborted: {e}")
            return

        # 1. Stop removed pipes
        for pid in removed:
            try:
                await self._stop_pipe(pid)
            except Exception as e:
                self.logger.error(f"Error stopping pipe '{pid}' during reload: {e}")
                
        # 2. Start added pipes
        for pid in added:
            try:
                await self._start_pipe(pid)
            except Exception as e:
                self.logger.error(f"Failed to start added pipe '{pid}' during reload: {e}")
        
        self.logger.info("Configuration reload complete.")

    async def _save_state(self):
        """Save runtime state to file."""
        try:
            state = {
                "pipes": {
                    pid: {"state": str(p.state)} 
                    for pid, p in self.pipe_runtime.items()
                }
            }
            if os.path.exists(STATE_FILE_PATH):
                shutil.copyfile(STATE_FILE_PATH, STATE_FILE_PATH + ".bak")
            
            with open(STATE_FILE_PATH, 'w') as f:
                json.dump(state, f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")