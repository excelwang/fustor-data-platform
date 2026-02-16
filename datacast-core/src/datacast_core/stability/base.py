import logging
import asyncio
from typing import Dict, Any, List, Optional, TypeVar, Generic

logger = logging.getLogger("datacast_core.stability")

T = TypeVar("T")

class BaseInstanceService(Generic[T]):
    """
    Base class for instance management services (PipeManager, EventBusManager).
    Provides basic pool management and lifecycle hooks.
    """
    def __init__(self):
        self.pool: Dict[str, T] = {}
        self.logger = logger

    def get_instance(self, id: str) -> Optional[T]:
        """Get an instance by its ID."""
        return self.pool.get(id)

    def list_instances(self) -> List[T]:
        """List all managed instances."""
        return list(self.pool.values())

    async def stop_all(self):
        """Stop all managed instances."""
        instances = self.list_instances()
        if not instances:
            return
            
        self.logger.info(f"Stopping {len(instances)} instances...")
        tasks = []
        for inst in instances:
            # Check for standard stop method or coroutine
            if hasattr(inst, 'stop') and callable(inst.stop):
                res = inst.stop()
                if asyncio.iscoroutine(res):
                    tasks.append(res)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        self.pool.clear()
        self.logger.info("All instances stopped and pool cleared.")
