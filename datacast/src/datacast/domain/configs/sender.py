# src/datacast/services/configs/sender.py
"""
Sender Configuration Service.

Sender configuration service.
The term "sender" aligns with the V2 architecture terminology.
"""
import logging
from typing import Dict, Optional, List

from datacast_core.models.config import AppConfig, SenderConfig
from datacast.stability.pipe_manager import PipeManager
from ..common import config_lock
from .base import BaseConfigService
from datacast_sdk.interfaces import SenderConfigServiceInterface

logger = logging.getLogger("datacast")


class SenderConfigService(BaseConfigService[SenderConfig], SenderConfigServiceInterface):
    """
    Manages the lifecycle of SenderConfig objects.
    
    Senders are responsible for transmitting events from datacast to fustord.
    Sender configuration service.
    """
    
    def __init__(self, app_config: AppConfig):
        super().__init__(app_config, None, 'sender')
        self.pipe_instance_service: Optional[PipeManager] = None

    def set_dependencies(self, pipe_instance_service: PipeManager):
        """Injects the PipeManager for dependency management."""
        self.pipe_instance_service = pipe_instance_service

    def list_configs(self) -> Dict[str, SenderConfig]:
        """List all configs from YAML."""
        from datacast.config.unified import DatacastConfig
        return DatacastConfig.get_all_senders()

    def get_config(self, id: str) -> Optional[SenderConfig]:
        """Get config by ID from YAML."""
        from datacast.config.unified import DatacastConfig
        return DatacastConfig.get_sender(id)

    def _add_config_to_app(self, id: str, config: SenderConfig):
        from datacast.config.unified import DatacastConfig
        DatacastConfig.add_sender(id, config)

    def _delete_config_from_app(self, id: str) -> SenderConfig:
        from datacast.config.unified import DatacastConfig
        conf = DatacastConfig.get_sender(id)
        DatacastConfig.delete_sender(id)
        return conf

    async def cleanup_obsolete_configs(self) -> List[str]:
        """
        Finds and deletes all Sender configurations that are both disabled and
        not used by any sync tasks.

        Returns:
            A list of the configuration IDs that were deleted.
        """
        all_pipe_configs = self.app_config.get_pipes().values()
        in_use_sender_ids = {p.sender for p in all_pipe_configs}

        all_sender_configs = self.list_configs()
        obsolete_ids = [
            sender_id for sender_id, config in all_sender_configs.items()
            if config.disabled and sender_id not in in_use_sender_ids
        ]

        if not obsolete_ids:
            logger.debug("No obsolete sender configurations to clean up.")
            return []

        logger.info(f"Found {len(obsolete_ids)} obsolete sender configurations to clean up: {obsolete_ids}")

        deleted_ids = []
        async with config_lock:
            sender_dict = self.app_config.get_senders()
            for an_id in obsolete_ids:
                if an_id in sender_dict:
                    sender_dict.pop(an_id)
                    deleted_ids.append(an_id)
            
            if deleted_ids:
                # Config persistence handled by YAML files now
                pass
        
        
        logger.info(f"Successfully cleaned up {len(deleted_ids)} sender configurations.")
        return deleted_ids

