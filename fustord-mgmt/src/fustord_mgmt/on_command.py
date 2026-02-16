# fustord-mgmt/src/fustord_mgmt/on_command.py
"""
On-Command Find Fallback Mechanism.

This module provides the logic to fallback to a realtime sensord command
when the memory view is incomplete or unavailable.
"""
import logging
import time
import asyncio
from typing import Any, Dict, List, Optional
from fastapi import HTTPException



logger = logging.getLogger("fustord_mgmt.on_command")

# Global semaphore to limit concurrent fallback scans across all requests
# to prevent overwhelming sensords or network.
# Global semaphore cache to limit concurrent fallback scans across all requests
_SEMAPHORE: Optional[asyncio.Semaphore] = None
_SEMAPHORE_LIMIT: int = 0
_SEMAPHORE_LOCK = asyncio.Lock()

async def get_semaphore(limit: int) -> asyncio.Semaphore:
    """
    Get or update the global semaphore based on the limit.
    Warning: Resizing (creating a new semaphore) allows a short burst of extra concurrency
    during the transition period as old tasks finish on the old semaphore.
    """
    global _SEMAPHORE, _SEMAPHORE_LIMIT
    
    # Fast path without lock if limit hasn't changed (approximate check)
    if _SEMAPHORE and _SEMAPHORE_LIMIT == limit:
        return _SEMAPHORE

    async with _SEMAPHORE_LOCK:
        if _SEMAPHORE is None or _SEMAPHORE_LIMIT != limit:
            # Check constraint
            safe_limit = max(1, limit)
            if safe_limit != limit:
                 logger.warning(f"Invalid On-Command concurrency limit {limit}, adjusting to {safe_limit}")
            
            _SEMAPHORE = asyncio.Semaphore(safe_limit)
            _SEMAPHORE_LIMIT = safe_limit
            logger.info(f"Updated On-Command concurrency semaphore to {safe_limit}")
        return _SEMAPHORE

async def on_command_fallback(view_id: str, params: Dict[str, Any], pipe_manager: "PipeManager") -> Dict[str, Any]:
    """
    Execute a remote scan on the sensord when local view query fails.
    """
    start_time = time.time()
    
    try:
        # 1. Resolve Pipes
        if not pipe_manager:
            raise RuntimeError("Pipe manager not provided")

        p_ids = pipe_manager.resolve_pipes_for_view(view_id)
        if not p_ids:
            raise RuntimeError(f"No active pipes found to service view {view_id}")

        # 2. Prepare Parallel Execution
        from fustord.config.unified import fustord_config
        fallback_timeout = fustord_config.fustord.on_command_fallback_timeout

        async def execute_on_pipe(p_id: str) -> Optional[Dict[str, Any]]:
            # Concurrency Control
            # Get current limit from config
            limit = fustord_config.fustord.on_command_concurrency_limit
            sem = await get_semaphore(limit)

            async with sem:
                pipe = pipe_manager.get_pipe(p_id)
                if not pipe: return None
                
                bridge = pipe_manager.get_bridge(p_id)
                if not bridge: return None

                # Use leader session or any active session
                target_session_id = pipe.leader_session
                if not target_session_id:
                    active_sessions = await bridge.get_all_sessions()
                    if active_sessions:
                        target_session_id = next(iter(active_sessions))
                
                if not target_session_id:
                    return None

                # 1. Prepare Command - Pass through parameters
                target_path = params.get("path", "/")
                
                # Intelligent depth logic: if recursive is false, depth is 1.
                # If recursive is true (or missing/default), use max_depth param or default 10.
                is_recursive = params.get("recursive", True)
                if not is_recursive:
                    max_depth = 1
                else:
                    max_depth = params.get("max_depth", params.get("depth", 10))

                cmd_params = {
                    "path": target_path,
                    "max_depth": int(max_depth),
                    "limit": int(params.get("limit", 1000)),
                    "pattern": params.get("pattern", "*"),
                }

                # 2. Execute via Bridge (Synchronous-like wait)
                try:
                    result = await bridge.send_command_and_wait(
                        session_id=target_session_id,
                        command="scan",
                        params=cmd_params,
                        timeout=fallback_timeout
                    )
                    if result:
                        result["success"] = True
                    return result
                except Exception as e:
                    logger.warning(f"Fallback command failed on session {target_session_id}: {e}")
                    return None

        # 3. Dispatch and Wait
        results = await asyncio.gather(*[execute_on_pipe(pid) for pid in p_ids])
        
        # 4. Check Success
        best_result = None
        for r in results:
            if r and r.get("success"):
                best_result = r
                break

        if not best_result:
            raise HTTPException(
                status_code=502,
                detail=f"Fallback command failed on all {len(p_ids)} pipes for view {view_id}"
            )

        # 5. Process and Return Result
        duration = time.time() - start_time
        
        # Map result to API format
        entries = best_result.get("files", best_result.get("entries", []))
        
        return {
            "path": params.get("path", "/"),
            "entries": entries,
            "metadata": {
                "source": "remote_fallback",
                "latency_ms": int(duration * 1000),
                "pipes_queried": len(p_ids),
                "successful_pipes": len([r for r in results if r]),
                "timestamp": time.time(),
                "sensord_id": best_result.get("sensord_id")
            }
        }
    except Exception as e:
        logger.error(f"View {view_id}: Fallback FAILED: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Fallback command failed on view {view_id}: {str(e)}"
        )
