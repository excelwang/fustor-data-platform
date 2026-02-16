import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict

from sensord_core.drivers import ViewDriver
# We might need to import from domain instead of extension directly if we want to use the manager
# But the driver is an extension. It should probably use the SDK or a provided interface.
# However, within fustord, it's easier to use fustord.domain.view_manager.manager.

logger = logging.getLogger(__name__)

class ForestFSViewDriver(ViewDriver):
    """
    Forest View Driver (Redesigned): Query-only aggregator.
    
    This driver no longer participates in event ingestion (process_event is no-op).
    It dynamically aggregates results from multiple member views at query time.
    """
    target_schema = "fs"
    # Live-mode flag: triggers full state reset when all sessions close
    # For a query-aggregator, this might not be needed, but we keep it for now
    requires_full_reset_on_session_close = False
    
    def __init__(self, id: str, view_id: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(id, view_id, config)
        self.logger = logger
        self.member_view_ids = (config or {}).get("member_view_ids", [])
        
    async def initialize(self):
        """Initialize the forest driver."""
        pass

    async def resolve_session_role(self, session_id: str, **kwargs) -> Dict[str, Any]:
        """
        ForestView is a read-only aggregator. 
        It doesn't participate in election; it acts as a 'follower' (viewer).
        """
        return {
            "role": "follower",
            "election_key": self.view_id
        }

    async def process_event(self, event: Any) -> bool:
        """
        ForestView is query-only. Ingestion is a no-op.
        """
        return True

    async def _get_member_drivers(self) -> Dict[str, ViewDriver]:
        """Dynamically resolve member view drivers."""
        from fustord.domain.view_manager.manager import get_cached_view_manager
        
        drivers = {}
        for vid in self.member_view_ids:
            try:
                vm = await get_cached_view_manager(vid)
                if vm and vm.driver_instances:
                    # For now, we assume each ViewManager manages sub-drivers
                    # and we pick the first one or iterate. 
                    # Usually, fustord ViewManager has only one driver instance per view_id.
                    for name, driver in vm.driver_instances.items():
                        drivers[f"{vid}:{name}"] = driver
            except Exception as e:
                self.logger.warning(f"ForestView failed to resolve member view '{vid}': {e}")
        return drivers

    async def get_directory_stats(self, strategy: str = "best") -> Dict[str, Any]:
        """
        Get aggregated directory statistics from members.
        """
        drivers = await self._get_member_drivers()
        if not drivers:
            return {
                "item_count": 0, "total_size": 0, "latency_ms": 0.0,
                "staleness_seconds": 0.0, "suspect_file_count": 0, "tree_count": 0
            }

        tasks = []
        for d in drivers.values():
            tasks.append(d.get_directory_stats())
        
        stats_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        if strategy == "best":
            best_stats = None
            for stats in stats_list:
                if isinstance(stats, Exception): continue
                if best_stats is None or stats.get("item_count", 0) > best_stats.get("item_count", 0):
                    best_stats = stats
            
            if best_stats:
                best_stats["member_count"] = len(drivers)
                return best_stats
        
        # Aggregate
        total_items = 0
        total_size = 0
        max_latency = 0.0
        max_staleness = 0.0
        suspect_count = 0
        
        valid_count = 0
        for stats in stats_list:
            if isinstance(stats, Exception): continue
            valid_count += 1
            total_items += stats.get("item_count", 0)
            total_size += stats.get("total_size", 0)
            max_latency = max(max_latency, stats.get("latency_ms", 0.0))
            max_staleness = max(max_staleness, stats.get("staleness_seconds", 0.0))
            suspect_count += stats.get("suspect_file_count", 0)
                
        return {
            "item_count": total_items,
            "total_size": total_size,
            "latency_ms": max_latency,
            "staleness_seconds": max_staleness,
            "suspect_file_count": suspect_count,
            "member_count": len(drivers),
            "valid_member_count": valid_count
        }

    async def get_subtree_stats_agg(self, path: str = "/") -> Dict[str, Any]:
        """
        Aggregate stats from all member views.
        """
        drivers = await self._get_member_drivers()
        if not drivers:
            return {"path": path, "members": [], "best": None}

        async def fetch(vid, driver):
            try:
                stats = await driver.get_subtree_stats(path)
                return vid, {"status": "ok", **stats}
            except Exception as e:
                return vid, {"status": "error", "error": str(e)}

        tasks = [fetch(vid, driver) for vid, driver in drivers.items()]
        responses = await asyncio.gather(*tasks)
        
        members = []
        best = None
        max_val = -1
        
        for vid, res in responses:
            res["member_id"] = vid
            members.append(res)
            if res.get("status") == "ok":
                val = res.get("file_count", 0)
                if val > max_val:
                    max_val = val
                    best = {"member_id": vid, "value": val}

        return {
            "path": path,
            "members": members,
            "best": best
        }

    async def get_directory_tree(self, path: str = "/", best: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """
        Get aggregated directory tree.
        """
        drivers = await self._get_member_drivers()
        if not drivers:
            return {"path": path, "members": {}}

        target_vids = list(drivers.keys())
        
        if best:
            stats = await self.get_subtree_stats_agg(path)
            if stats.get("best"):
                winner = stats["best"]["member_id"]
                if winner in drivers:
                    target_vids = [winner]

        member_results = {}
        async def fetch_tree(vid):
            driver = drivers[vid]
            try:
                data = await driver.get_directory_tree(path=path, **kwargs)
                return vid, {"status": "ok", "data": data}
            except Exception as e:
                return vid, {"status": "error", "error": str(e)}

        tasks = [fetch_tree(vid) for vid in target_vids]
        results = await asyncio.gather(*tasks)
        for vid, res in results:
            member_results[vid] = res

        return {
            "path": path,
            "members": member_results,
            "strategy": "best" if best else "all"
        }

    # --- Lifecycle methods (Broadcast to members if needed) ---
    # Actually, members have their own lifecycle managed by their own ViewManagers.
    # ForestView only queries them.

    async def on_session_start(self, **kwargs): pass
    async def on_session_close(self, **kwargs): pass
    async def on_snapshot_complete(self, session_id: str, **kwargs) -> None: pass
    async def handle_audit_start(self): pass
    async def handle_audit_end(self): pass
    async def cleanup_expired_suspects(self): pass

    async def reset(self):
        """Reset is no-op for query aggregator."""
        pass
