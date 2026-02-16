import asyncio
import logging
from typing import Dict, Any, List, Optional, Union, TypeVar, Generic
from dataclasses import dataclass
from abc import ABC, abstractmethod

from .base import BaseInstanceService

logger = logging.getLogger("sensord_core.stability")

T = TypeVar("T")

@dataclass
class StartResult:
    """Result of a pipe start operation for fault isolation."""
    sensord_pipe_id: str
    success: bool
    error: Optional[str] = None
    skipped: bool = False

class BasePipeManager(BaseInstanceService[T], Generic[T], ABC):
    """
    Base class for Pipe Managers in sensord and fustord.
    Provides standardized lifecycle management and start/stop logic.
    """
    
    @abstractmethod
    async def start_one(self, id: str, **kwargs) -> StartResult:
        """
        Start a single pipe instance.
        Must be implemented by concrete classes.
        """
        pass

    @abstractmethod
    async def stop_one(self, id: str, **kwargs):
        """
        Stop a single pipe instance.
        Must be implemented by concrete classes.
        """
        pass

    async def start_all(self, ids: List[str]) -> Dict[str, Any]:
        """
        Start multiple pipes in parallel with fault isolation.
        
        Args:
            ids: List of pipe IDs to start.
        
        Returns:
            Summary dict with started/failed/skipped counts and details.
        """
        self.logger.info(f"Attempting to batch start {len(ids)} pipes (fault-isolated)...")
        
        # Use gather with return_exceptions=True for fault isolation
        results: List[Union[StartResult, Exception]] = await asyncio.gather(
            *[self.start_one(pid) for pid in ids],
            return_exceptions=True
        )
        
        # Normalize results
        normalized_results = []
        for pid, result in zip(ids, results):
            if isinstance(result, Exception):
                self.logger.error(f"Unexpected error starting pipe '{pid}': {result}", exc_info=True)
                normalized_results.append(StartResult(sensord_pipe_id=pid, success=False, error=str(result)))
            elif isinstance(result, StartResult):
                normalized_results.append(result)
            else:
                # Should not happen if start_one returns StartResult
                normalized_results.append(StartResult(sensord_pipe_id=pid, success=False, error="Unknown result type"))

        # Calculate summary
        started = sum(1 for r in normalized_results if r.success and not r.skipped)
        failed = sum(1 for r in normalized_results if not r.success)
        skipped = sum(1 for r in normalized_results if r.skipped)
        
        self.logger.info(f"Pipe batch startup complete: {started} started, {failed} failed, {skipped} skipped")
        
        # Log failures
        for r in normalized_results:
            if not r.success:
                self.logger.error(f"  - {r.sensord_pipe_id}: {r.error}")
        
        return {
            "started": started,
            "failed": failed,
            "skipped": skipped,
            "details": [vars(r) for r in normalized_results]
        }
