# fustord/src/fustord/runtime/pipe/bridge_commands.py
import asyncio
import logging
import uuid
from typing import Any, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from ..session_bridge import PipeSessionBridge

logger = logging.getLogger("fustord.session_bridge")

class BridgeCommandsMixin:
    """
    Mixin for PipeSessionBridge handling command sending and response correlation.
    """
    
    async def send_command_and_wait(
        self: "PipeSessionBridge", 
        session_id: str, 
        command: str, 
        params: Dict[str, Any], 
        timeout: float = 5.0
    ) -> Dict[str, Any]:
        """Send a command to the datacastst and wait for a response."""
        cmd_id = str(uuid.uuid4())
        future = asyncio.get_running_loop().create_future()
        self._pending_commands[cmd_id] = future
        
        try:
            command_payload = {"id": cmd_id, "type": command, **params}
            # Queue command locally in the session store
            self.store.queue_command(session_id, command_payload)
           
            logger.debug(f"Queued command {command} ({cmd_id}) for session {session_id}, waiting {timeout}s...")
            return await asyncio.wait_for(future, timeout=timeout)
        except Exception:
            if cmd_id in self._pending_commands:
                del self._pending_commands[cmd_id]
            raise
            
    def resolve_command(self: "PipeSessionBridge", command_id: str, result: Dict[str, Any]):
        """Resolve a pending command with a result."""
        if command_id in self._pending_commands:
            future = self._pending_commands.pop(command_id)
            if not future.done():
                if result.get("success", True):
                    future.set_result(result)
                else:
                    future.set_exception(Exception(result.get("error", "Command failed")))
        else:
            logger.warning(f"Received result for unknown/expired command {command_id}")
