# fustor-sender-http

HTTP Sender for Fustor datacastst - implements the transport layer fodatacastcast to fustord communication.

## Overview

This package provides an HTTP-based implementation of the `Sender` transport abstraction. It uses the fustord SDK client to communicate with fustord's REST API.

## Installation

```bash
pip install fustor-sender-http
```

## Usage

```python
from fustor_sender_http import HTTPSender

sender = HTTPSender(
    sender_id="my-sender",
    endpoint="http://fustord.example.com:8000",
    credential={"api_key": "your-api-key"}
)

# Create session
await sender.connect()
session = await sender.create_session("my-task-id")

# Send events
await sender.send_events(events, source_type="message")

# Heartbeat
await sender.heartbeat()

# Cleanup
await sender.close()
```

## Entry Points

This package registers itself as:
- `datacastst.drivers.senders:fustord` - Standard sender registry

## Migration from fustor-sender-fustord

The `fustor-sender-fustord` package is deprecated. To migrate:

1. Replace `from fustor_sender_fustord import fustordDriver` with `from fustor_sender_http import HTTPSender`
2. Update configuration to use `sender` instead of `sender` terminology
3. The `HTTPSender` strictly implements the `Sender` interface (v2).
