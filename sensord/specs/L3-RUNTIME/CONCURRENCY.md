# L3: [Sensord] Concurrency & Threading Model

> **Status**: Active
> **Layer**: Implementation (L3)
> **Parent**: L1-CONTRACTS.NON_BLOCKING_IO

---

## [model] Asyncio_Event_Loop

- **SINGLE_EVENT_LOOP**: Sensord core logic MUST run on a single `asyncio` event loop.
  - **Rationale**: Deterministic state management without complex locking.
  - **Constraint**: No blocking calls (file I/O, heavy computation) allowed in the main loop.
  - **Verification**: All `await` keywords map to non-blocking keys; no `time.sleep()`.

## [pattern] Thread_Bridge_Pattern

To bridge synchronous Blocking I/O (e.g., `os.scandir`, `inotify` reads) with the Asyncio world, Sensord uses the **Thread Bridge Pattern**:

1.  **Producer Thread**: A dedicated `threading.Thread` that executes the blocking operational loop.
2.  **Queue**: An `asyncio.Queue` (thread-safe for `put`, async for `get`) acts as the buffer.
3.  **Signaling**: A `threading.Event` (`stop_event`) is passed to the producer to signal graceful shutdown.
4.  **Draining**: The Async consumer MUST drain the queue during shutdown sequences to allow the producer thread to unblock and exit.

### Example Implementation

```python
def producer(queue, stop_event):
    while not stop_event.is_set():
        item = blocking_read()
        queue.put_nowait(item)

async def consumer(queue):
    while True:
        item = await queue.get()
        process(item)
        queue.task_done()
```
