# fustord/src/fustord/runtime/__init__.py
"""
Runtime components for Fustor fustord.

This module provides the Pipe-based architecture for fustord:

FustordPipe Architecture:
============================

┌─────────────────────────────────────────────────────────────┐
│                    FustordPipe                           │
│  (receives events from sensords)                              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  ViewHandler Registry                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  FSViewHandler│  │  DBViewHandler│  │     ...      │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘

Example Usage:
--------------

    from fustord.stability import FustordPipe

    # Create pipe
    pipe = FustordPipe(
        pipe_id="view-1",
        config={"view_id": 1},
        view_handlers=[fs_view_handler]
    )

    # Start processing
    await pipe.start()

    # Process incoming events from sensord
    await pipe.process_events(events, session_id="sess-123")

    # Query views
    tree = pipe.get_view("fs", path="/")
"""

from .pipe import FustordPipe

from fustord.domain.view_handler_adapter import (
    ViewDriverAdapter,
    ViewManagerAdapter,
    create_view_handler_from_driver,
    create_view_handler_from_manager,
)

from .session_bridge import (
    PipeSessionBridge,
    create_session_bridge,
)



__all__ = [
    # Pipe
    "FustordPipe",
    
    # View Handler Adapters
    "ViewDriverAdapter",
    "ViewManagerAdapter",
    "create_view_handler_from_driver",
    "create_view_handler_from_manager",
    
    # Session Bridge
    "PipeSessionBridge",
    "create_session_bridge",
    

]
