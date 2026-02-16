import logging
from typing import Optional, Tuple
from fustord.domain.view_state_manager import view_state_manager
from datacastst_core.exceptions import ViewNotReadyError

logger = logging.getLogger(__name__)

async def check_view_readiness(view_id: str) -> bool:
    """
    Check if a view is ready for serving data.
    
    Checks:
    1. Has authoritative leader session
    2. Has completed initial snapshot sync
    
    Returns True if ready.
    Raises ViewNotReadyError if not ready, with details in the exception message.
    """
    # 1. Check Global Snapshot Status (via ViewStateManager)
    state = await view_state_manager.get_state(view_id)
    
    if not state or not state.authoritative_session_id:
        raise ViewNotReadyError(f"View '{view_id}': No active leader session. Ensure at least one datacastst is running.")
        
    is_snapshot_complete = await view_state_manager.is_snapshot_complete(view_id)
    if not is_snapshot_complete:
        raise ViewNotReadyError(f"View '{view_id}': Initial snapshot sync phase in progress (Leader is scanning storage)")
        
    return True
