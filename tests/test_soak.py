"""Soak gate: a kernel-shaped workload (fasthtml app + websocket bot + housekeeping
tick) on one loopmini loop under sustained load, watching stability, not speed.

Deselected by default; run with `pytest -m soak -s`. LOOPMINI_SOAK_SECONDS (default 30)
sets the duration. The verdict asserts zero errors, bounded fd growth, every mid-load
"cell" ran, and live servers at the end.
"""
import asyncio, os, socket, statistics, threading, time
import httpx, psutil, pytest, uvicorn, websockets
from fasthtml.common import fast_app, Titled, P

pytestmark = [pytest.mark.soak, pytest.mark.timeout(600)]

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

def build_workload(state):
    "Coroutines for the loop under test: fasthtml on uvicorn, ws echo + bot client, tick."
    app,rt = fast_app()
    @rt('/')
    def home(): return Titled('soak', P('hello'))
    @rt('/data')
    async def data():
        await asyncio.sleep(0)
        return {'n': state['ticks']}

    async def serve_http():
        cfg = uvicorn.Config(app, host='127.0.0.1', port=state['http_port'], log_level='warning')
        state['server'] = uvicorn.Server(cfg)
        await state['server'].serve()

    async def serve_ws():
        async def echo(ws):
            async for m in ws: await ws.send(m)
        async with websockets.serve(echo, '127.0.0.1', state['ws_port']): await asyncio.Event().wait()

    async def bot():
        for _ in range(50):  # the ws server may not have bound yet
            try:
                ws = await websockets.connect(f"ws://127.0.0.1:{state['ws_port']}")
                break
            except OSError: await asyncio.sleep(0.1)
        async with ws:
            while True:
                await ws.send(b'ping')
                assert await ws.recv() == b'ping'
                state['ws_rt'] += 1
                await asyncio.sleep(0.01)

    async def tick():
        while True:
            state['ticks'] += 1
            await asyncio.sleep(0.1)

    return serve_http, serve_ws, bot, tick

def http_load(state, stop, latencies, errors):
    with httpx.Client(base_url=f"http://127.0.0.1:{state['http_port']}", timeout=10) as c:
        while not stop.is_set():
            t0 = time.perf_counter()
            try:
                r = c.get('/data' if state['ws_rt'] % 2 else '/')
                r.raise_for_status()
                latencies.append(time.perf_counter() - t0)
            except Exception as e: errors.append(repr(e))

def test_soak():
    import loopmini
    seconds = int(os.environ.get('LOOPMINI_SOAK_SECONDS', '30'))
    state = dict(http_port=free_port(), ws_port=free_port(), ticks=0, ws_rt=0, server=None)
    loop = loopmini.new_event_loop()
    threading.Thread(target=loop.run_forever, daemon=True).start()
    futs = [asyncio.run_coroutine_threadsafe(coro_fn(), loop) for coro_fn in build_workload(state)]
    time.sleep(1)  # let the servers come up

    proc = psutil.Process()
    fds0,rss0 = proc.num_fds(), proc.memory_info().rss
    stop = threading.Event()
    latencies,errors,cells = [],[],[]
    workers = [threading.Thread(target=http_load, args=(state, stop, latencies, errors)) for _ in range(8)]
    for w in workers: w.start()

    async def cell(i):
        await asyncio.sleep(0.001)
        return i
    t_end = time.time() + seconds
    while time.time() < t_end:
        time.sleep(5)
        cells.append(asyncio.run_coroutine_threadsafe(cell(len(cells)), loop).result(10))
    stop.set()
    for w in workers: w.join()

    fds1,rss1 = proc.num_fds(), proc.memory_info().rss
    failed = [f.exception() for f in futs if f.done()]  # before cancel: exception() raises on a cancelled future
    for f in futs: f.cancel()
    time.sleep(0.2)  # let cancellations unwind before the loop stops
    loop.call_soon_threadsafe(loop.stop)
    lat = sorted(latencies)
    print(f'\nsoak seconds={seconds}')
    print(f'http reqs={len(lat)} p50={statistics.median(lat)*1e3:.2f}ms p99={lat[int(len(lat)*.99)]*1e3:.2f}ms')
    print(f"ws round-trips={state['ws_rt']} ticks={state['ticks']} cells={len(cells)}")
    print(f'fds {fds0}->{fds1}  rss {rss0/2**20:.1f}->{rss1/2**20:.1f} MiB  errors={len(errors)}')
    assert not errors, errors[:3]
    assert not failed
    assert state['ws_rt'] > seconds * 50 and state['ticks'] > seconds * 5  # both ran throughout
    assert fds1 - fds0 <= 4
    assert cells == list(range(len(cells)))
    assert state['server'].started
