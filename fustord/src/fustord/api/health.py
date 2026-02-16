from fastapi import APIRouter
from typing import Dict, Any
from .. import runtime_objects
import logging

logger = logging.getLogger(__name__)

health_router = APIRouter(tags=["Health"], prefix="/health")

@health_router.get("/components", summary="Get component health status")
async def get_component_health() -> Dict[str, Any]:
    """Get health status of all components (pipes, handlers, connected sensords)."""
    pm = runtime_objects.pipe_manager
    if not pm:
        return {}
        
    result = {}
    for pipe_id, pipe in pm.get_pipes().items():
        # Get handlers health
        handler_health = {}
        if hasattr(pipe, '_view_handlers'):
            for h_id, h in pipe._view_handlers.items():
                is_disabled = False
                if hasattr(pipe, '_disabled_handlers'):
                     is_disabled = h_id in pipe._disabled_handlers
                
                handler_health[h_id] = {
                    "enabled": not is_disabled,
                    "type": getattr(h, "schema_name", "unknown")
                }

        # Get sensord health (if connected)
        sensord_status = getattr(pipe, '_last_sensord_status', {})
        sensord_health = sensord_status.get('component_health', {})
        
        result[pipe_id] = {
            "state": str(pipe.state),
            "handlers": handler_health,
            "sensord_id": sensord_status.get('sensord_id'),
            "sensord_health": sensord_health,
            "last_heartbeat": getattr(pipe, '_last_activity', 0)
        }
    return result
