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

The Python side subclasses `asyncio.BaseEventLoop` and reuses its task, future, executor, error-handling, networking, server, sendfile, and subprocess machinery. Reactor-neutral socket, Unix, pipe, and accept implementations come directly from CPython's selector loop. Loopmini supplies the scheduling and fd-readiness hooks plus socket and datagram transports, so version-sensitive asyncio behavior remains CPython's own.

The Rust side owns fd readiness, timers, and cross-thread wakeup through a small level-triggered reactor core (`polling`, using kqueue/epoll). The Python driver blocks directly in the reactor with the GIL released. The same PyO3-free core is available to Rust consumers; embedding runtimes can run futures on their own workers and wake the Python loop through its thread-safe scheduling path.

## Compatibility

Four external test suites act as conformance oracles, run with `pytest -m oracle`: CPython's own `test_asyncio`, and the suites of anyio, uvloop, and aiohttp, about 7,000 tests in all. All four pass. The few exclusions are listed with reasons in `tests/test_oracle.py`.

## Interrupts

A KeyboardInterrupt injected while the loop runs (the kernel interrupt mechanism, and the delivery path of Ctrl-C) leaves the loop in a resumable state. Each scheduled callback either runs or remains queued. The standard loop can lose the callback in the same situation. The test suite injects interrupts under stream, timer, and cross-thread load to hold this guarantee, and the loop must keep serving afterwards.

## Performance

Throughput matches the standard loop on real I/O workloads. A 30-second soak serving a fasthtml app under concurrent HTTP and websocket load holds a 2.6ms median response with no fd or memory growth. There is a small overhead involved in getting the better interrupt semantics, due to having to cross the Rust boundary. It applies only when something is scheduled onto the loop, and is then under 1µs per operation: `create_task`, an `await` that suspends, `asyncio.sleep`, or `call_soon`. Code that stays inside Python, including an `await` that does not suspend, pays nothing. The median overshoot of a 1ms timer is ~0.2ms, similar to Python's standard loop.
