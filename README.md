# loopmini

A boring, maintainable, drop-in event loop for asyncio applications on Linux and Mac. It tracks CPython, stays sound under Ctrl-C, and shares its reactor with Rust.

```python
import asyncio, loopmini

async def main():
    await asyncio.sleep(0.1)
    return await asyncio.to_thread(lambda: 42)

asyncio.run(main(), loop_factory=loopmini.new_event_loop)
```

## Install

```bash
pip install loopmini
```

## Design

The Python side subclasses `asyncio.AbstractEventLoop` and reuses the stock `Task`, `Future`, `Handle`, and `sslproto` machinery. Contextvars, cancellation, and task introspection therefore behave exactly as in the standard loop. CPython releases change little that loopmini must track, because the version-sensitive objects are CPython's own.

The Rust side owns fd readiness, timers, and cross-thread wakeup. A small level-triggered reactor core (the `polling` crate, kqueue/epoll) is hosted on a Tokio current-thread runtime, which waits on the reactor's own pollable fd. This split preserves the level-triggered `add_reader` contract that asyncio requires and Tokio's edge-triggered driver cannot express. Rust futures spawned on the runtime advance during every blocking poll, with the GIL released, on the same thread and reactor as the Python loop.

## Compatibility

Four external test suites act as conformance oracles, run with `pytest -m oracle`: CPython's own `test_asyncio`, and the suites of anyio, uvloop, and aiohttp, about 7,000 tests in all. All four pass. The few exclusions are listed with reasons in `tests/test_oracle.py`.

## Interrupts

A KeyboardInterrupt injected while the loop runs (the kernel interrupt mechanism, and the delivery path of Ctrl-C) leaves the loop in a resumable state. Each scheduled callback either runs or remains queued. The standard loop can lose the callback in the same situation. The test suite injects interrupts under stream, timer, and cross-thread load to hold this guarantee, and the loop must keep serving afterwards.

## Performance

Throughput matches the standard loop on real I/O workloads. A 30-second soak serving a fasthtml app under concurrent HTTP and websocket load holds a 2.6ms median response with no fd or memory growth. Microbenchmarks run 5 to 15% slower than the standard loop, and creating a task costs about twice as much, because each schedule crosses the Python/Rust boundary. uvloop is faster where speed is the requirement.

