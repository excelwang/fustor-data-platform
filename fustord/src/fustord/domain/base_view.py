"""
Base abstractions for Views in fustord.
Relocated from datacast_core to keep cordatacastcast-centric.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Iterator
import logging

# Note: EventBase will still come from datacast_core (soon to bdatacastcast_core)
from datacast_core.event import EventBase

logger = logging.getLogger(__name__)

class Handler(ABC):
    """
    Abstract base class for all Handlers.
    (Local version for fustord views)
    """
    schema_name: str = ""
    schema_version: str = "1.0"
    
    def __init__(self, handler_id: str, config: Dict[str, Any]):
        self.id = handler_id
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{handler_id}")
    
    async def initialize(self) -> None:
        pass
    
    async def close(self) -> None:
        pass
    
    def get_schema_info(self) -> Dict[str, str]:
        return {
            "name": self.schema_name,
            "version": self.schema_version,
        }

class ViewHandler(Handler):
    """
    Base class for view handlers (data consumers).
    """
    @abstractmethod
    async def process_event(self, event: EventBase) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    async def get_data_view(self, **kwargs) -> Any:
        raise NotImplementedError
    
    async def resolve_session_role(self, session_id: str, **kwargs) -> Dict[str, Any]:
        return {"role": "leader"}
    
    async def on_session_start(self, **kwargs) -> None:
        pass
    
    async def on_session_close(self, **kwargs) -> None:
        pass
    
    async def handle_audit_start(self) -> None:
        pass
    
    async def handle_audit_end(self) -> None:
        pass
    
    async def reset_audit_tracking(self) -> None:
        pass
    
    async def on_snapshot_complete(self, session_id: str, **kwargs) -> None:
        pass
    
    async def reset(self) -> None:
        pass

class ViewDriver(ABC):
    """
    Abstract Base Class for View Drivers.
    """
    target_schema: str = ""
    
    def __init__(self, id: str, view_id: str, config: Dict[str, Any]):
        self.id = id
        self.view_id = view_id
        self.config = config

    async def initialize(self):
        pass
    
    @property
    def requires_full_reset_on_session_close(self) -> bool:
        if self.config:
            return self.config.get("mode") == "live" or self.config.get("is_live") is True
        return False

    @abstractmethod
    async def process_event(self, event: EventBase) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    async def get_data_view(self, **kwargs) -> Any:
        raise NotImplementedError

    async def on_session_start(self, **kwargs):
        pass
    
    async def on_session_close(self, **kwargs):
        pass

    async def handle_audit_start(self):
        pass
    
    async def handle_audit_end(self):
        pass

    async def reset(self):
        pass
    
    async def cleanup_expired_suspects(self):
        pass

    async def close(self):
        pass

    async def resolve_session_role(self, session_id: str, **kwargs) -> Dict[str, Any]:
        return {"role": "leader"}
