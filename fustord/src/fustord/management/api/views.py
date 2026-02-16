# fustord/src/fustord/api/views.py
"""
Data Views API Router Generator.
"""

import logging
import time
import asyncio
from typing import Callable, Any, Optional, Dict
from fastapi import APIRouter, HTTPException, Depends, status
from importlib.metadata import entry_points

from fustord.management.auth.dependencies import get_view_id_from_auth
from fustord.domain.view_manager.manager import get_cached_view_manager
from fustord.config.unified import fustord_config
from fustord.stability import runtime_objects

logger = logging.getLogger(__name__)

# Config cache to avoid reloading YAML on every request
_config_cache_time: float = 0.0
_CONFIG_CACHE_TTL: float = 5.0  # seconds

def _reload_config_if_stale():
    """Reload config at most once per TTL period."""
    global _config_cache_time
    now = time.monotonic()
    if now - _config_cache_time > _CONFIG_CACHE_TTL:
        fustord_config.reload()
        _config_cache_time = now

# Base router for all view-related endpoints
view_router = APIRouter(tags=["Data Views"])

async def get_view_driver_instance(view_id: str, lookup_key: str):
    """
    Helper to get a view driver instance for a specific key (instance name or driver name).
    """
    manager = await get_cached_view_manager(view_id)
    driver_instance = manager.driver_instances.get(lookup_key)
    
    if not driver_instance:
        # Fallback to checking if there's only one driver instance and using that
        # (Useful if queried by driver name but configured with instance name)
        if len(manager.driver_instances) == 1:
            driver_instance = list(manager.driver_instances.values())[0]
        else:
            logger.warning(f"View driver instance '{lookup_key}' not found in ViewManager '{view_id}' (Available: {list(manager.driver_instances.keys())})")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"View '{lookup_key}' not found or not active"
            )
            
    return FallbackDriverWrapper(driver_instance, view_id)

class FallbackDriverWrapper:
    """
    Wraps a View Driver to provide On-Command Fallback on failure.
    Acts as a proxy, intercepting get_data_view calls.
    """
    def __init__(self, driver, view_id):
        self._driver = driver
        self._view_id = view_id
        
    def __getattr__(self, name):
        attr = getattr(self._driver, name)
        if callable(attr) and asyncio.iscoroutinefunction(attr):
            async def wrapped(*args, **kwargs):
                try:
                    return await attr(*args, **kwargs)
                except Exception as e:
                    # In Gap P0-3, we fallback to on-demand datacast scan on failure
                    if runtime_objects.on_command_fallback:
                        try:
                            return await runtime_objects.on_command_fallback(self._view_id, kwargs)
                        except Exception as fallback_e:
                            logger.error(f"Fallback failed for {self._view_id}: {fallback_e}")
                            # Re-raise original error to not mask root cause
                            raise e
                    else:
                        raise e
            return wrapped
        return attr

    async def get_data_view(self, **kwargs):
        # Explicit override for standard ABC method to centralize readiness check + fallback
        try:
            # 1. Centralized Readiness Check
            from fustord.stability.readiness import check_view_readiness
            await check_view_readiness(self._view_id)

            # 2. Attempt Primary Driver
            return await self._driver.get_data_view(**kwargs)

        except Exception as e:
            # 3. Fallback Mechanism
            # Check if fallback handler is registered (it should be if fustord-mgmt is installed)
            if hasattr(runtime_objects, "on_command_fallback") and runtime_objects.on_command_fallback:
                # Log only if it's NOT a ViewNotReadyError (which is expected during startup)
                from datacast_core.exceptions import ViewNotReadyError
                if not isinstance(e, ViewNotReadyError):
                    logger.warning(f"View {self._view_id} primary failed ({e}), triggering On-Command Fallback...")
                else:
                    logger.info(f"View {self._view_id} not ready ({e}), triggering On-Command Fallback...")
                
                try:
                    return await runtime_objects.on_command_fallback(self._view_id, kwargs, runtime_objects.pipe_manager)
                except Exception as fallback_e:
                    logger.error(f"Fallback failed for {self._view_id}: {fallback_e}")
                    # If fallback fails, we must expose the original reason
                    if isinstance(e, ViewNotReadyError):
                         raise HTTPException(
                            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail=f"View '{self._view_id}' is unavailable and fallback failed: {fallback_e}",
                            headers={"Retry-After": "5"}
                        )
                    raise e
            else:
                # No fallback capability
                from datacast_core.exceptions import ViewNotReadyError
                if isinstance(e, ViewNotReadyError):  # Use custom exception, not string matching
                     raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=str(e),
                        headers={"Retry-After": "5"}
                    )
                raise e


def make_metadata_limit_checker(view_name: str) -> Callable:
    """Creates a dependency that ensures a request doesn't exceed metadata limits."""
    async def check_limit(view_id: str = Depends(get_view_id_from_auth)):
        # Ensure latest config is loaded (with TTL caching)
        _reload_config_if_stale()
        # Get view config
        view_cfg = fustord_config.get_view(view_name)
        if not view_cfg:
            return True
            
        # max_tree_items is specific to view-fs, so we look in driver_params
        params = view_cfg.driver_params
        limit = params.get("max_tree_items", 100000)
        
        if limit <= 0: # 0 or negative means unlimited
            return True
            
        # Get current stats from driver
        manager = await get_cached_view_manager(view_name)
        # We need a driver instance to get stats
        driver_instance = None
        if manager.driver_instances:
            driver_instance = list(manager.driver_instances.values())[0]
            
        if driver_instance:
            stats = await driver_instance.get_directory_stats()
            item_count = stats.get("item_count", 0)
            
            if item_count > limit:
                logger.warning(f"Metadata check failed for view '{view_name}': {item_count} items (limit: {limit})")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "Metadata retrieval limit exceeded",
                        "view": view_name,
                        "current_count": item_count,
                        "limit": limit,
                        "suggestion": "Try a more specific path or reduce recursion depth."
                    }
                )
        return True
    return check_limit


def make_readiness_checker(view_name: str) -> Callable:
    """Creates a dependency that ensures a view is ready before allowing API access."""
    async def check_ready(authorized_identity: str = Depends(get_view_id_from_auth)):
        # authorized_identity is the pipe_id (from API key) or view_id (if dedicated view key).
        # For view readiness checks, we use view_name (from closure) which is the actual view ID from URL path.
        manager = await get_cached_view_manager(view_name)
        driver_instance = manager.driver_instances.get(view_name)
        
        # 1. Centralized Check
        from fustord.stability.readiness import check_view_readiness
        from datacast_core.exceptions import ViewNotReadyError
        
        try:
            await check_view_readiness(view_name)
        except ViewNotReadyError as e:
            # GAP-V1 Fix: If fallback is enabled, don't 503. Let the wrapper handle it.
            if runtime_objects.on_command_fallback:
                 logger.debug(f"View '{view_name}': Not ready ({e}). Allowing request for On-Command Fallback.")
                 return
            
            # No fallback, so we must 503
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=str(e),
                headers={"Retry-After": "5"}
            )

        # 2. Check Driver Instance Specific Readiness
        if not driver_instance and len(manager.driver_instances) == 1:
            driver_instance = list(manager.driver_instances.values())[0]
            
        if not driver_instance:
             raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"View '{view_name}' not found"
            )

        # Check if driver instance has readiness flag (e.g. initial snapshot done)
        # Note: FSViewDriver implementation uses is_ready property
        is_ready = getattr(driver_instance, "is_ready", True)
        if callable(is_ready):
            is_ready = is_ready()
            
        if not is_ready:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"View '{view_name}' is still performing initial synchronization",
                headers={"Retry-After": "5"}
            )
        return True
    return check_ready

def _discover_view_api_factories():
    """Discover API router factories from entry points."""
    factories = []
    try:
        eps = entry_points(group="fustord.view_api")
        for ep in eps:
            try:
                create_router_func = ep.load()
                factories.append((ep.name, create_router_func))
                logger.info(f"Discovered view API factory: {ep.name}")
            except Exception as e:
                logger.error(f"Failed to load view API entry point '{ep.name}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error discovering view API entry points: {e}", exc_info=True)
    
    return factories

def setup_view_routers():
    """
    Dynamically register API routers for all configured views.
    This is called during application lifespan startup to ensure that 
    newly injected configurations are picked up.
    """
    # Clear existing routes to avoid duplicates
    view_router.routes = []
    
    # Reload config to ensure we have the latest
    fustord_config.reload()
    current_view_configs = fustord_config.get_all_views()
    
    available_factories = {name: func for name, func in _discover_view_api_factories()}
    logger.debug(f"setup_view_routers started. Factories: {list(available_factories.keys())}")
    
    if not available_factories:
        logger.debug("No view API factories discovered!")
        logger.warning("No view API factories discovered. Check if view driver packages are installed.")
        return

    registered_count = 0
    logger.debug(f"current_view_configs: {list(current_view_configs.keys()) if current_view_configs else 'None'}")
    
    # 1. Registered via config
    if current_view_configs:
        for view_name, cfg in current_view_configs.items():
            driver_name = cfg.driver
            create_func = available_factories.get(driver_name)
            logger.debug(f"Processing view '{view_name}' with driver '{driver_name}'. Factory found: {create_func is not None}")
            
            if create_func:
                try:
                    # Create context-bound driver instance getter and readiness checker
                    async def get_driver_instance_for_instance(view_id: str, _key=view_name):
                        return await get_view_driver_instance(_key, _key)
                    
                    # Register with prefix matching the view_name (e.g., test-fs)

                    if driver_name == "multi-fs":
                        checker = None
                        limit_checker = None
                    else:
                        # GAP-V1 Fix: Remove readiness checker dependency. 
                        # We rely on FallbackDriverWrapper to handle readiness + fallback.
                        checker = None 
                        limit_checker = make_metadata_limit_checker(view_name)
                    
                    router = create_func(
                        get_driver_func=get_driver_instance_for_instance,
                        check_snapshot_func=checker,
                        get_view_id_dep=get_view_id_from_auth,
                        check_metadata_limit_func=limit_checker
                    )
                    
                    # Register with prefix matching the view_name (e.g., test-fs)
                    view_router.include_router(router, prefix=f"/{view_name}")
                    logger.debug(f"Registered router for '{view_name}' at prefix '/{view_name}'")
                    logger.info(f"Registered view API routes: {view_name} (driver: {driver_name}) at prefix /{view_name} WITH LIMIT CHECKER")
                    registered_count += 1
                except Exception as e:
                    logger.debug(f"Error registering view '{view_name}': {e}")
                    logger.error(f"Error registering view API routes '{view_name}': {e}", exc_info=True)
            else:
                logger.warning(f"View '{view_name}' configures driver '{driver_name}', but no API factory for that driver was found.")

    # 2. If no config or no views registered via config, fall back to driver names
    if registered_count == 0:
        for name, create_func in available_factories.items():
            try:
                # Fallback uses driver name as the lookup key
                async def get_driver_instance_fallback(view_id: str, _key=name):
                    return await get_view_driver_instance(_key, _key)
                
                checker = make_readiness_checker(name)
                limit_checker = make_metadata_limit_checker(name)
                
                router = create_func(
                    get_driver_func=get_driver_instance_fallback,
                    check_snapshot_func=checker,
                    get_view_id_dep=get_view_id_from_auth,
                    check_metadata_limit_func=limit_checker
                )
                
                view_router.include_router(router, prefix=f"/{name}")
                logger.info(f"Registered fallback view API routes: {name} at prefix /{name}")
            except Exception as e:
                logger.error(f"Error registering fallback API routes for '{name}': {e}", exc_info=True)

    # 3. Add generic jobs endpoints for all views
    # These must be added AFTER clearing routes and registering specific view routers
    view_router.add_api_route(
        "/{view_id}/jobs", 
        list_view_jobs, 
        methods=["GET"],
        summary="List datacast jobs for a view",
        response_model=Dict[str, Any]
    )
    view_router.add_api_route(
        "/{view_id}/jobs/{job_id}", 
        get_view_job_status, 
        methods=["GET"],
        summary="Get job status",
        response_model=Dict[str, Any]
    )
    view_router.add_api_route(
        "/{view_id}/sessions", 
        list_view_sessions, 
        methods=["GET"],
        summary="List active sessions for a view",
        response_model=Dict[str, Any]
    )



async def list_view_jobs(view_id: str, authorized_view_id: str = Depends(get_view_id_from_auth)):
    """List datacast jobs for a specific view."""
    if authorized_view_id != view_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="API Key not authorized for this view"
        )
    from fustord.domain.job_manager import job_manager
    jobs = job_manager.get_jobs(view_id=view_id)
    return {"jobs": jobs}


async def get_view_job_status(view_id: str, job_id: str, authorized_view_id: str = Depends(get_view_id_from_auth)):
    """Get status of a specific datacast job in a view."""
    if authorized_view_id != view_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="API Key not authorized for this view"
        )
    from fustord.domain.job_manager import job_manager
    all_jobs = job_manager.get_jobs(view_id=view_id)
    job = next((j for j in all_jobs if j["job_id"] == job_id), None)
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"datacast job '{job_id}' not found"
        )
        
    return job


async def list_view_sessions(view_id: str, authorized_view_id: str = Depends(get_view_id_from_auth)):
    """List active sessions for a specific view."""
    if authorized_view_id != view_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="API Key not authorized for this view"
        )
    
    from fustord.stability.runtime_objects import pipe_manager
    if not pipe_manager:
         return {"view_id": view_id, "active_sessions": [], "count": 0}
         
    sessions = await pipe_manager.list_sessions(view_id=view_id)
    
    # sessions from list_sessions already contains role, task_id etc.
    # But we want to ensure it matches the historical API format if needed.
    session_list = []
    for s in sessions:
        # Normalize fields for API consistency
        session_list.append({
            "session_id": s["session_id"],
            "task_id": s.get("task_id"),
            "datacast_id": s.get("task_id", "").split(":")[0] if s.get("task_id") and ":" in s["task_id"] else s.get("task_id"),
            "client_ip": s.get("client_ip"),
            "source_uri": s.get("source_uri"),
            "last_activity": s.get("last_activity"),
            "created_at": s.get("created_at"),
            "role": "leader" if s.get("is_leader") else "follower",
            "can_snapshot": s.get("is_leader", False),
            "can_audit": s.get("is_leader", False),
            "can_realtime": True, # Traditional default
            "can_send": True
        })
    
    return {
        "view_id": view_id,
        "active_sessions": session_list,
        "count": len(session_list)
    }


# Initial call to attempt registration (will be called again in lifespan for certainty)
setup_view_routers()
# VOLUME_SYNC_TEST
