"""Performance snapshots: loopmini vs the standard loop on loop-bound operations.

Deselected by default; run with `pytest -m bench -s`. Informational, no assertions
on timings: the numbers are for watching drift, not gating.
"""
import asyncio, time
import pytest, loopmini

pytestmark = pytest.mark.bench

def _timed(loop_factory, coro_fn):
    t0 = time.perf_counter()
    asyncio.run(coro_fn(), loop_factory=loop_factory)
    return time.perf_counter() - t0

async def bench_call_soon():
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    def cb(i):
        if i: loop.call_soon(cb, i-1)
        else: fut.set_result(None)
    loop.call_soon(cb, 200_000)
    await fut

async def bench_sleep0():
    for _ in range(20_000): await asyncio.sleep(0)

async def bench_spawn():
    async def noop(): pass
    await asyncio.gather(*(noop() for _ in range(20_000)))

async def bench_tcp_echo():
    async def handler(reader, writer):
        while (data := await reader.read(65536)):
            writer.write(data)
            await writer.drain()
        writer.close()
    server = await asyncio.start_server(handler, '127.0.0.1', 0)
    reader,writer = await asyncio.open_connection(*server.sockets[0].getsockname()[:2])
    payload = b'x'*1024
    for _ in range(5_000):
        writer.write(payload)
        await writer.drain()
        n = 0
        while n < len(payload): n += len(await reader.read(65536))
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()

BENCHES = [bench_call_soon, bench_sleep0, bench_spawn, bench_tcp_echo]

def test_bench():
    print()
    print(f'{"bench":<16} {"stdlib":>8} {"loopmini":>9}  ratio')
    for fn in BENCHES:
        std = _timed(None, fn)
        lm = _timed(loopmini.new_event_loop, fn)
        print(f'{fn.__name__[6:]:<16} {std:>7.3f}s {lm:>8.3f}s  {lm/std:>5.2f}x')
