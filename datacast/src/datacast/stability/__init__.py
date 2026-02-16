# datacast/src/datacast/runtime/__init__.py
"""
Runtime components for Fustor datacast.

This module provides the new Pipe-based architecture for datacast:

DatacastPipe Architecture:
===========================

┌─────────────────────────────────────────────────────────────┐
│                     DatacastPipe                           │
│  (orchestrates Source -> Sender data flow)                  │
└──────────────┬───────────────────────────┬──────────────────┘
               │                           │
               ▼                           ▼
┌──────────────────────────┐  ┌──────────────────────────────┐
│   SourceHandlerAdapter   │  │   SenderHandlerAdapter       │
│  (wraps source drivers)  │  │  (wraps sender transports)   │
└──────────────┬───────────┘  └───────────────┬──────────────┘
               │                              │
               ▼                              ▼
┌──────────────────────────┐  ┌──────────────────────────────┐
│       Source Driver      │  │    datacast_core.transport     │
│   (FSDriver, etc.)       │  │   (HTTPSender, etc.)         │
└──────────────────────────┘  └──────────────────────────────┘

Example Usage:
--------------

    from datacast.stability import (
        DatacastPipe,
        create_source_handler_from_config,
        create_sender_handler_from_config,
    )

    # Create handlers from configuration
    source_handler = create_source_handler_from_config(
        source_config=app_config.get_source("my-source"),
        source_driver_service=source_driver_service
    )

    sender_handler = create_sender_handler_from_config(
        sender_config=app_config.get_sender("my-sender"),
        sender_driver_service=sender_driver_service
    )

    # Create and start pipe
    pipe = DatacastPipe(
        pipe_id="my-pipe",
        task_id="datacast-1:my-pipe",
        config={
            "batch_size": 100,
        },
        source_handler=source_handler,
        sender_handler=sender_handler,
    )

    await pipe.start()
"""

from .pipe import DatacastPipe

from datacast.domain.source_handler_adapter import (
    SourceHandlerAdapter,
    SourceHandlerFactory,
    create_source_handler_from_config,
)

from .sender_adapter import (
    SenderHandlerAdapter,
    SenderHandlerFactory,
    create_sender_handler_from_config,
)



__all__ = [
    # Pipe
    "DatacastPipe",
    
    # Source Handler
    "SourceHandlerAdapter",
    "SourceHandlerFactory",
    "create_source_handler_from_config",
    
    # Sender Handler
    "SenderHandlerAdapter",
    "SenderHandlerFactory",
    "create_sender_handler_from_config",
    

]
