import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger("sensord")

class BaseInstanceService:
    """
    Base class for instance management services (PipeManager, EventBusManager).
    """
    def __init__(self):
        self.pool: Dict[str, Any] = {}

    def get_instance(self, id: str) -> Optional[Any]:
        """Get an instance by its ID."""
        return self.pool.get(id)

    def list_instances(self) -> List[Any]:
        """List all managed instances."""
        return list(self.pool.values())

    async def stop_all(self):
        """Stop all managed instances."""
        # Generic stop_all implementation
        import asyncio
        instances = self.list_instances()
        if not instances:
            return
            
        logger.info(f"Stopping {len(instances)} instances...")
        tasks = []
        for inst in instances:
            if hasattr(inst, 'stop') and callable(inst.stop):
                tasks.append(inst.stop())
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        self.pool.clear()
