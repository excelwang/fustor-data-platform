# fustor-sender-openapi

This package provides a robust `SenderDriver` implementation for the Fustor sensord service, designed to push data to any external service that exposes an OpenAPI (Swagger) specification. It intelligently discovers API endpoints for batch ingestion, session management, and heartbeat, and handles authentication using Basic Auth or API Keys.

## Features

*   **OpenAPI Specification Driven**: Dynamically parses the target service's OpenAPI specification to discover relevant endpoints for data ingestion, session creation, and heartbeats.
*   **Flexible Endpoint Discovery**: Supports custom `x-sensord-status-endpoint`, `x-sensord-ingest-batch-endpoint`, and `x-sensord-open-session-endpoint` extensions in the OpenAPI spec, or attempts to infer them from common patterns.
*   **Session Management**: Implements `create_session` and `heartbeat` to manage the session lifecycle with the target OpenAPI service.
*   **Checkpointing**: Retrieves the latest committed index from the target service to support resume functionality.
*   **Authentication**: Supports `Basic Auth` (username/password) and `API Key` (Bearer Token or `x-api-key` header) for secure communication.
*   **Retry Mechanism**: Includes a retry mechanism for network and HTTP errors to enhance reliability.
*   **Wizard Definition**: Provides a comprehensive configuration wizard for UI integration, guiding users through endpoint and credential setup.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`. It is registered as a `sensord.drivers.senders` entry point.

## Usage

To use the `fustor-sender-openapi` driver, configure a Sender in your Fustor sensord setup with the driver type `openapi`. You will need to provide the URL to the target service's OpenAPI specification and appropriate credentials.

Example (conceptual configuration in Fustor sensord):

```yaml
# Fustor 主目录下的 sensord-config.yaml
senders:
  my-openapi-sender:
    driver_type: openapi
    endpoint: http://your-target-service.com/openapi.json # URL to the OpenAPI spec
    credential:
      type: api_key
      key: YOUR_API_KEY_OR_BEARER_TOKEN
    # Optional advanced settings
    max_retries: 5
    retry_delay_sec: 10
```

## Dependencies

*   `sensord-core`: Provides the `SenderDriver` abstract base class and other core components.
*   `fustor-event-model`: Provides `EventBase` for event data structures.
*   `httpx`: A next-generation HTTP client for making asynchronous requests.
*   `pydantic`: For data validation and settings management.
