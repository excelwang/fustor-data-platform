"""
sensord runner - entry point for headless operation.
"""
import asyncio
import signal
import logging
from typing import Optional, List

logger = logging.getLogger("sensord")


async def run_sensord(config_list: Optional[List[str]] = None):
    """
    Main entry point for running the sensord.
    
    Args:
        config_list: List of pipe config names/paths to start.
                    If None, loads from default.yaml.
    """
    from .app import App
    
    # app = App(config_list=config_list) - Moved inside supervisor loop
    
    # Handle shutdown signals
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def handle_signal(sig):
        logger.info(f"Received signal {sig.name}, initiating shutdown...")
        stop_event.set()

    app_ref = {"instance": None}

    def handle_reload(sig):
        logger.info(f"Received signal {sig.name}, reloading configuration...")
        if app_ref["instance"]:
            asyncio.create_task(app_ref["instance"].reload_config())
        else:
            logger.warning("No active sensord instance to reload.")

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))
    
    # Try SIGHUP if available (Unix)
    if hasattr(signal, 'SIGHUP'):
        loop.add_signal_handler(signal.SIGHUP, lambda s=signal.SIGHUP: handle_reload(s))

    # Supervisor Loop: Keep restarting the sensord on failure unless signaled to stop
    while not stop_event.is_set():
        app_instance = App(config_list=config_list)
        app_ref["instance"] = app_instance
        try:
            await app_instance.startup()
            logger.info("sensord started successfully. Waiting for signals...")
            
            # Keep running until signaled
            await stop_event.wait()
        except Exception as e:
            logger.critical(f"sensord runtime error: {e}", exc_info=True)
            if not stop_event.is_set():
                retry_delay = 5.0
                logger.info(f"Supervisor: sensord crashed. Restarting in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
        finally:
            # Ensure app cleans up resources on every cycle (crash or stop)
            logger.info("Stopping sensord instance...")
            try:
                await app_instance.shutdown()
            except Exception as e:
                logger.error(f"Error during sensord shutdown: {e}")

    logger.info("sensord supervisor loop terminated.")
