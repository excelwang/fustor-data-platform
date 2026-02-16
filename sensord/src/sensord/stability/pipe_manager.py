import asyncio
from typing import TYPE_CHECKING, Dict, Any, Optional, Union, List
import logging 
from dataclasses import dataclass

from sensord_core.stability import BasePipeManager, StartResult
from sensord.stability.pipe import SensordPipe

# PipeRuntime is now always SensordPipe
PipeRuntime = SensordPipe

from sensord.domain.source_handler_adapter import SourceHandlerAdapter
from sensord.stability.sender_adapter import SenderHandlerAdapter
from sensord.config.unified import sensord_config

logger = logging.getLogger("sensord")

from sensord_sdk.interfaces import PipeManagerInterface # Import the interface

class PipeManager(BasePipeManager, PipeManagerInterface): # Inherit from base and interface
    def __init__(
        self, 
        bus_manager: "EventBusManager",
        sender_driver_service: "SenderDriverService",
        source_driver_service: "SourceDriverService",
    ):
        super().__init__()
        self.bus_manager = bus_manager
        self.sender_driver_service = sender_driver_service
        self.source_driver_service = source_driver_service
        self.logger = logging.getLogger("sensord") 

    async def start_one(self, id: str, raise_on_error: bool = False) -> StartResult:
        """
        Start a single pipe instance with fault isolation.
        
        Args:
            id: Pipe configuration ID
            raise_on_error: If True, raise exceptions (for API calls). 
                           If False (default), return error in StartResult (for batch starts).
        
        Returns:
            StartResult with success status and optional error message
        """
        self.logger.debug(f"Enter start_one for pipe_id: {id}")

        if self.get_instance(id):
            self.logger.warning(f"Pipe instance '{id}' is already running or being managed.")
            return StartResult(sensord_pipe_id=id, success=True, skipped=True)

        pipe_config = sensord_config.get_pipe(id)
        if not pipe_config:
            error_msg = f"Pipe config '{id}' not found."
            self.logger.error(error_msg)
            if raise_on_error:
                raise NotFoundError(error_msg)
            return StartResult(sensord_pipe_id=id, success=False, error=error_msg)
        self.logger.debug(f"Found pipe config for {id}")

        if pipe_config.disabled:
            self.logger.info(f"Pipe instance '{id}' will not be started because its configuration is disabled.")
            return StartResult(sensord_pipe_id=id, success=True, skipped=True)

        source_config = sensord_config.get_source(pipe_config.source)
        if not source_config:
            error_msg = f"Source config '{pipe_config.source}' not found for pipe '{id}'."
            self.logger.error(error_msg)
            if raise_on_error:
                raise NotFoundError(error_msg)
            return StartResult(sensord_pipe_id=id, success=False, error=error_msg)
        self.logger.debug(f"Found source config for {id}")
        
        sender_config = sensord_config.get_sender(pipe_config.sender)
        if not sender_config:
            error_msg = f"Required Sender config '{pipe_config.sender}' not found."
            self.logger.error(error_msg)
            if raise_on_error:
                raise NotFoundError(error_msg)
            return StartResult(sensord_pipe_id=id, success=False, error=error_msg)
        self.logger.debug(f"Found sender config for {id}")
        
        self.logger.info(f"Attempting to start pipe instance '{id}'...")
        try:
            # D-05: Global Config Injection
            # Inject global fs_scan_workers default if not present in driver_params
            if source_config.driver == "fs":
                if "max_scan_workers" not in source_config.driver_params:
                    source_config.driver_params["max_scan_workers"] = sensord_config.fs_scan_workers
            
            # Obtain EventBus mandatory for all pipes (unifying architecture)
            field_mappings = getattr(pipe_config, "fields_mapping", [])
            
            # We assume start from position 0 for new pipes
            event_bus, needed_position_lost = await self.bus_manager.get_or_create_bus_for_subscriber(
                source_id=pipe_config.source,
                source_config=source_config,
                pipe_id=id, # Use pipe_id as subscriber_id
                required_position=0, 
                fields_mapping=field_mappings
            )
            self.logger.info(f"Subscribed to EventBus {event_bus.id} for pipe '{id}'")

            # Create Handlers
            source_driver_class = self.source_driver_service._get_driver_by_type(source_config.driver)
            source_driver = source_driver_class(id=pipe_config.source, config=source_config)
            source_handler = SourceHandlerAdapter(source_driver, config=source_config)

            sender_driver_class = self.sender_driver_service._get_driver_by_type(sender_config.driver)
            
            # Extract sender config and credentials
            # Extract sender config and credentials
            sender_credentials = {}
            if sender_config.credential:
                if hasattr(sender_config.credential, "model_dump"):
                    sender_credentials = sender_config.credential.model_dump()
                elif hasattr(sender_config.credential, "dict"):
                    sender_credentials = sender_config.credential.dict()
                else:
                    sender_credentials = dict(sender_config.credential)

            sender_driver_config = {
                "batch_size": sender_config.batch_size,
                "timeout_sec": sender_config.timeout_sec,
                "api_version": getattr(sender_config, "api_version", "v2"),
                **sender_config.driver_params
            }

            sender_driver = sender_driver_class(
                sender_id=pipe_config.sender,
                endpoint=sender_config.uri,
                credential=sender_credentials,
                config=sender_driver_config
            )
            sender_handler = SenderHandlerAdapter(sender_driver, config=sender_config)

            # Build runtime config
            runtime_config = {
                "batch_size": getattr(pipe_config, 'batch_size', 100),
                "audit_interval_sec": getattr(pipe_config, 'audit_interval_sec', 600),
                "sentinel_interval_sec": getattr(pipe_config, 'sentinel_interval_sec', 120),
                "session_timeout_seconds": None,
                "fields_mapping": field_mappings,
            }

            pipe = SensordPipe(
                pipe_id=id,
                config=runtime_config,
                source_handler=source_handler,
                sender_handler=sender_handler,
                event_bus=event_bus,
                bus_manager=self.bus_manager,
            )
            
            self.pool[id] = pipe
            await pipe.start()
            
            self.logger.info(f"Pipe instance '{id}' start initiated successfully.")
            return StartResult(sensord_pipe_id=id, success=True)

        except (AttributeError, KeyError) as e:
            # Provide more specific error messages for configuration issues
            error_str = str(e)
            friendly_msg = f"Configuration error for pipe '{id}': {error_str}"
            if "'source'" in error_str:
                friendly_msg = f"Pipe '{id}' is missing required field 'source' in its configuration"
            elif "'sender'" in error_str:
                friendly_msg = f"Pipe '{id}' is missing required field 'sender' in its configuration"
            
            self.logger.error(friendly_msg)
            if self.get_instance(id):
                self.pool.pop(id)
            if raise_on_error:
                raise ValueError(friendly_msg) from e
            return StartResult(sensord_pipe_id=id, success=False, error=friendly_msg)
            
        except Exception as e:
            self.logger.error(f"Failed to start pipe instance '{id}': {e}", exc_info=True)
            if self.get_instance(id):
                self.pool.pop(id)
            if raise_on_error:
                raise
            return StartResult(sensord_pipe_id=id, success=False, error=str(e))



    async def stop_one(self, id: str, should_release_bus: bool = True):
        instance = self.get_instance(id)
        if not instance:
            return

        self.logger.info(f"Attempting to stop {instance}...")
        try:
            # The instance's bus might not exist if stopped during snapshot sync phase
            bus_id = instance.bus.id if instance.bus else None
            
            await instance.stop()
            self.pool.pop(id, None)
            self.logger.info(f"{instance} stopped and removed from pool.")
            
            if should_release_bus and bus_id:
                # Use task_id for bus subscription release
                await self.bus_manager.release_subscriber(bus_id, instance.task_id)
        except KeyError:
            # Race condition: already removed
            self.logger.warning(f"Pipe instance '{id}' was already stopped/removed during stop request.")
        except Exception as e:
            self.logger.error(f"Failed to cleanly stop {instance}: {e}", exc_info=True)

    async def remap_pipe_to_new_bus(self, pipe_id: str, new_bus: "EventBusInstanceRuntime", needed_position_lost: bool):
        # Search by short id or task_id (bus uses task_id)
        pipe_instance = self.get_instance(pipe_id)
        if not pipe_instance:
            # Search by task_id in pool
            for inst in self.pool.values():
                if getattr(inst, 'task_id', None) == pipe_id:
                    pipe_instance = inst
                    break
        
        if not pipe_instance:
            self.logger.warning(f"Pipe task '{pipe_id}' not found in pool during bus remapping.")
            return

        old_bus_id = pipe_instance.bus.id if pipe_instance.bus else None
        self.logger.info(f"Remapping sync task '{pipe_instance.id}' (task_id={pipe_instance.task_id}) to new bus '{new_bus.id}'...")
        
        # Call the instance's remap method, which also handles the signal
        await pipe_instance.remap_to_new_bus(new_bus, needed_position_lost)
        
        if old_bus_id:
            await self.bus_manager.release_subscriber(old_bus_id, pipe_instance.task_id)
        
        self.logger.info(f"Pipe task '{pipe_instance.id}' remapped to bus '{new_bus.id}' successfully.")



    async def mark_dependent_pipes_outdated(self, dependency_type: str, dependency_id: str, reason_info: str, updates: Optional[Dict[str, Any]] = None):
        from sensord_core.pipe import PipeState
        affected_pipes = []
        for inst in self.list_instances():
            pipe_config = sensord_config.get_pipe(inst.id)
            if not pipe_config:
                continue
            
            if (dependency_type == "source" and pipe_config.source == dependency_id) or \
               (dependency_type == "sender" and pipe_config.sender == dependency_id):
                affected_pipes.append(inst)

        logger.info(f"Marking pipes dependent on {dependency_type} '{dependency_id}' as outdated.")
        for pipe_instance in affected_pipes:
            # Use PipeState which is what SensordPipe uses internally
            pipe_instance.state |= PipeState.CONF_OUTDATED
            pipe_instance.info = reason_info # Update info directly
        self.logger.info(f"Marked {len(affected_pipes)} pipes as outdated.")




    async def start_all_enabled(self) -> Dict[str, Any]:
        """
        Start all enabled pipes with fault isolation.
        
        Individual pipe failures do not block other pipes from starting.
        
        Returns:
            Summary dict with started/failed/skipped counts and details
        """
        all_pipe_configs = sensord_config.get_all_pipes()
        if not all_pipe_configs:
            return {"started": 0, "failed": 0, "skipped": 0, "details": []}

        enabled_ids = [pid for pid, cfg in all_pipe_configs.items() if not cfg.disabled]
        
        # Use base class implementation for parallel start
        return await self.start_all(enabled_ids)
            
    async def restart_outdated_pipes(self) -> int:
        from sensord_core.pipe import PipeState
        outdated_pipes = [
            inst for inst in self.list_instances() 
            if inst.state & PipeState.CONF_OUTDATED
        ]

        
        if not outdated_pipes:
            return 0
            
        self.logger.info(f"Found {len(outdated_pipes)} outdated sync tasks to restart.")
        
        for pipe in outdated_pipes:
            await self.stop_one(pipe.id) 
            await asyncio.sleep(1) # Give time for graceful shutdown
            await self.start_one(pipe.id)
        
        return len(outdated_pipes)



    async def stop_all(self):
        self.logger.info("Stopping all pipe instances...")
        keys_to_stop = list(self.pool.keys())
        stop_tasks = [self.stop_one(key, should_release_bus=False) for key in keys_to_stop]
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks)

        await self.bus_manager.release_all_unused_buses()

    async def trigger_audit(self, id: str):
        instance = self.get_instance(id)
        if not instance:
            raise NotFoundError(f"Pipe instance '{id}' not found.")
        await instance.trigger_audit()

    async def trigger_sentinel(self, id: str):
        instance = self.get_instance(id)
        if not instance:
            raise NotFoundError(f"Pipe instance '{id}' not found.")
        await instance.trigger_sentinel()
