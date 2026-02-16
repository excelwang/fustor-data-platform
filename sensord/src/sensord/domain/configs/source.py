# src/sensord/services/configs/source.py

import logging
from typing import Dict, Optional, List, Any


from sensord_core.models.config import AppConfig, SourceConfig
from sensord.stability.pipe_manager import PipeManager
from .base import BaseConfigService
from ..common import config_lock
from .. import schema_cache # Import schema_cache at the top level
from sensord_sdk.interfaces import SourceConfigServiceInterface # Import the interface

logger = logging.getLogger("sensord")

class SourceConfigService(BaseConfigService[SourceConfig], SourceConfigServiceInterface): # Inherit from the interface
    """
    Manages the lifecycle of SourceConfig objects.
    This service is responsible for CRUD operations on source configurations
    and inherits common logic from BaseConfigService.
    """
    def __init__(self, app_config: AppConfig):
        super().__init__(app_config, None, 'source')
        self.pipe_instance_service: Optional[PipeManager] = None

    def set_dependencies(self, pipe_instance_service: PipeManager):
        """
        Injects the PipeManager for dependency management.
        This is to resolve circular dependencies between services.
        """
        self.pipe_instance_service = pipe_instance_service

    def list_configs(self) -> Dict[str, SourceConfig]:
        """List all configs from YAML."""
        from sensord.config.unified import sensord_config
        return sensord_config.get_all_sources()

    def get_config(self, id: str) -> Optional[SourceConfig]:
        """Get config by ID from YAML."""
        from sensord.config.unified import sensord_config
        return sensord_config.get_source(id)

    async def add_config(self, id: str, config: SourceConfig) -> SourceConfig:
        """
        Adds a new source configuration.
        """
        return await super().add_config(id, config)

    def _add_config_to_app(self, id: str, config: SourceConfig):
        from sensord.config.unified import sensord_config
        sensord_config.add_source(id, config)

    def _delete_config_from_app(self, id: str) -> SourceConfig:
        from sensord.config.unified import sensord_config
        conf = sensord_config.get_source(id)
        sensord_config.delete_source(id)
        return conf

    async def update_config(self, id: str, updates: Dict[str, Any]) -> SourceConfig:
        """
        Updates a source configuration. If enabling, checks for a valid schema cache.
        """
        # If the user is trying to enable the source, perform the validation check.
        # If the user is trying to enable the source, perform the validation check.
        if 'disabled' in updates and not updates['disabled']:
            source_config = self.get_config(id) # Get current config to check driver
            driver_type = updates.get('driver') or (source_config.driver if source_config else None)
            
            if driver_type and not schema_cache.is_schema_valid(id):
                from ..drivers.source_driver import SourceDriverService
                source_driver_service = SourceDriverService()
                
                if source_driver_service.driver_requires_schema(driver_type):
                    raise ValueError(
                        f"Cannot enable source '{id}': Schema cache is not validated. "
                        f"Please run 'discover-schema' for this source first."
                    )
        
        # Proceed with the generic update logic from the base class.
        return await super().update_config(id, updates)

    async def cleanup_obsolete_configs(self) -> List[str]:
        """
        Finds and deletes all Source configurations that are both disabled and
        not used by any sync tasks.

        Returns:
            A list of the configuration IDs that were deleted.
        """
        from sensord.config.unified import sensord_config
        all_pipe_configs = sensord_config.get_all_pipes().values()
        in_use_source_ids = {p.source for p in all_pipe_configs}

        all_source_configs = self.list_configs()
        obsolete_ids = [
            source_id for source_id, config in all_source_configs.items()
            if config.disabled and source_id not in in_use_source_ids
        ]

        if not obsolete_ids:
            logger.debug("No obsolete source configurations to clean up.")
            return []

        logger.info(f"Found {len(obsolete_ids)} obsolete source configurations to clean up: {obsolete_ids}")

        deleted_ids = []
        async with config_lock:
            for an_id in obsolete_ids:
                # Use _delete_config_from_app to avoid re-acquiring lock/checking deps again
                # but we must check if it still exists (race condition)
                from sensord.config.unified import sensord_config
                if sensord_config.get_source(an_id):
                     self._delete_config_from_app(an_id)
                     # Also remove the schema cache files associated with the obsolete config
                     schema_cache.delete_schema(an_id)
                     logger.info(f"Removed schema cache for obsolete source '{an_id}'.")
                     deleted_ids.append(an_id)
        
        logger.info(f"Successfully cleaned up {len(deleted_ids)} source configurations.")
        return deleted_ids

    async def check_and_disable_missing_schema_sources(self) -> List[str]:
        """
        Checks all enabled source configurations for a valid schema cache.
        If a valid schema cache is missing, the source is automatically disabled.

        Returns:
            A list of IDs of sources that were disabled.
        """
        disabled_sources = []
        from ..drivers.source_driver import SourceDriverService
        source_driver_service = SourceDriverService()

        disabled_sources = []
        for source_id, config in self.list_configs().items():
            # If the source is enabled but its schema is not valid, disable it.
            # EXCEPTION: Skip check if the driver specifically declares it doesn't need formal discovery.
            if not config.disabled and not schema_cache.is_schema_valid(source_id):
                if not source_driver_service.driver_requires_schema(config.driver):
                    logger.debug(f"Source '{source_id}' (driver: {config.driver}) skipped schema check as it doesn't require discovery.")
                    continue

                logger.warning(
                    f"Source '{source_id}' is enabled but its schema is not validated. "
                    f"Disabling it to prevent runtime errors. "
                    f"Please run 'sensord discover-schema' to re-validate."
                )
                await self.disable(source_id)
                disabled_sources.append(source_id)
        
        return disabled_sources

    async def discover_and_cache_fields(self, source_id: str, admin_user: str, admin_password: str):
        """
        Connects to a data source, discovers its available fields, saves them to a
        local cache file, and creates a validation marker file upon success.
        """
        from sensord_core.models.config import PasswdCredential
        from ..drivers.source_driver import SourceDriverService

        source_config = self.get_config(source_id)
        if not source_config:
            raise ValueError(f"Source config '{source_id}' not found.")

        source_driver_service = SourceDriverService()

        try:
            # Invalidate old schema first to ensure a clean state
            schema_cache.invalidate_schema(source_id)
            logger.debug(f"Invalidated existing schema for source '{source_id}' before discovery.")

            payload = {"uri": source_config.uri}
            # FIX: Only create PasswdCredential and add it to the payload
            # if admin_user was actually provided.
            if admin_user:
                admin_creds = PasswdCredential(user=admin_user, passwd=admin_password)
                payload["admin_creds"] = admin_creds.model_dump()

            fields = await source_driver_service.get_available_fields(
                source_config.driver, **payload
            )
            
            # Save the new schema
            schema_cache.save_source_schema(source_id, fields)
            
            # **CRITICAL STEP**: Mark the new schema as valid
            schema_cache.validate_schema(source_id)
            
            logger.info(f"Fields for source '{source_id}' discovered, cached, and validated successfully.")
        except Exception as e:
            # If any step fails, ensure the cache is left in an invalid state.
            schema_cache.invalidate_schema(source_id)
            logger.error(f"Failed to discover and cache fields for source '{source_id}': {e}", exc_info=True)
            # Re-raise the exception to be handled by the CLI or calling service.
            raise
