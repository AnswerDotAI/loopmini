"Integration tests of the behaviours a kernel-hosting loop must get right."
import asyncio, contextvars, ctypes, os, pytest, random, signal, socket, sys, threading, time
import loopmini

def run(coro): return asyncio.run(coro, loop_factory=loopmini.new_event_loop)

def test_cancelled_timer_released():
    """A cancelled timer must leave the reactor's timer map at once.

    Kernels call wait_for(..., timeout=3600) in a loop; retaining each cancelled
    handle until its original deadline grows memory for an hour per handle.
    """
    loop = loopmini.new_event_loop()
    handles = [loop.call_later(3600, lambda: None) for _ in range(500)]
    for h in handles: h.cancel()
    assert loop._r.timer_count() == 0
    loop.close()

def test_scheduling_tasks_and_threads():
    async def main():
        loop = asyncio.get_running_loop()
        assert isinstance(loop, loopmini.Loop)
        t0 = time.monotonic()
        await asyncio.sleep(0.05)
        assert time.monotonic()-t0 >= 0.045
        var = contextvars.ContextVar('v')
        var.set('outer')
        async def child():
            assert var.get() == 'outer'
            var.set('inner')
            await asyncio.sleep(0.01)
            return 42
        t = asyncio.create_task(child())
        assert await t == 42
        assert var.get() == 'outer'
        assert await asyncio.gather(asyncio.sleep(0.01, 'a'), asyncio.sleep(0.01, 'b')) == ['a','b']
        async with asyncio.TaskGroup() as tg: r = tg.create_task(asyncio.sleep(0.01, 'tg'))
        assert r.result() == 'tg'
        with pytest.raises(TimeoutError): await asyncio.wait_for(asyncio.sleep(10), 0.05)
        hung = asyncio.create_task(asyncio.sleep(10))
        await asyncio.sleep(0)
        hung.cancel()
        with pytest.raises(asyncio.CancelledError): await hung
        fut = loop.create_future()
        threading.Thread(target=lambda: loop.call_soon_threadsafe(fut.set_result, 'ts')).start()
        assert await fut == 'ts'
        assert await asyncio.to_thread(threading.get_ident) != threading.get_ident()
        assert asyncio.current_task() is not None
    run(main())

def test_socket_io():
    async def main():
        loop = asyncio.get_running_loop()
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('127.0.0.1', 0))
        srv.listen()
        srv.setblocking(False)
        payload = b'x'*5_000_000 + b'\n'
        async def server():
            conn,_ = await loop.sock_accept(srv)
            with conn:
                data = b''
                while not data.endswith(b'\n'): data += await loop.sock_recv(conn, 65536)
                await loop.sock_sendall(conn, str(len(data)).encode()+b'\n')
        st = asyncio.create_task(server())
        cli = socket.socket()
        cli.setblocking(False)
        with cli:
            await loop.sock_connect(cli, srv.getsockname())
            await loop.sock_sendall(cli, payload)
            reply = b''
            while not reply.endswith(b'\n'): reply += await loop.sock_recv(cli, 4096)
        assert int(reply) == len(payload)
        await st
        srv.close()
    run(main())

def test_streams():
    async def main():
        async def handler(reader, writer):
            while (line := await reader.readline()):
                writer.write(line.upper())
                await writer.drain()
            writer.close()
            await writer.wait_closed()
        server = await asyncio.start_server(handler, '127.0.0.1', 0, limit=8_000_000)
        port = server.sockets[0].getsockname()[1]
        reader,writer = await asyncio.open_connection('127.0.0.1', port, limit=8_000_000)
        writer.write(b'hello\n')
        await writer.drain()
        assert await reader.readline() == b'HELLO\n'
        big = b'x'*3_000_000 + b'\n'  # well past the 64KiB write buffer, so drain must actually wait
        writer.write(big)
        await writer.drain()
        assert await reader.readline() == big.upper()
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()
    run(main())

def test_uvicorn_http():
    uvicorn = pytest.importorskip('uvicorn')
    import urllib.request
    async def app(scope, receive, send):
        assert scope['type'] == 'http'
        await send(dict(type='http.response.start', status=200, headers=[(b'content-type', b'text/plain')]))
        await send(dict(type='http.response.body', body=b'hello from loopmini'))
    async def main():
        server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=0, log_level='warning'))
        t = asyncio.create_task(server.serve())
        while not server.started: await asyncio.sleep(0.01)
        port = server.servers[0].sockets[0].getsockname()[1]
        url = f'http://127.0.0.1:{port}/'
        body = await asyncio.to_thread(lambda: urllib.request.urlopen(url).read())
        assert body == b'hello from loopmini'
        import httpx
        async with httpx.AsyncClient() as c: r = await c.get(url)
        assert r.text == 'hello from loopmini'
        server.should_exit = True
        await t
    run(main())

def test_task_survives_between_runs():
    loop = loopmini.new_event_loop()
    async def bg():
        await asyncio.sleep(0.05)
        return 'done'
    t = loop.create_task(bg())
    loop.run_until_complete(asyncio.sleep(0))
    assert not t.done()
    assert loop.run_until_complete(t) == 'done'
    loop.close()

def test_keyboard_interrupt_recovery():
    loop = loopmini.new_event_loop()
    result = {}
    def target():
        async def spin():
            while True: await asyncio.sleep(0.001)
        try: loop.run_until_complete(spin())
        except BaseException as e: result['exc'] = e
    th = threading.Thread(target=target)
    th.start()
    time.sleep(0.2)
    assert ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(th.ident), ctypes.py_object(KeyboardInterrupt)) == 1
    th.join(5)
    assert not th.is_alive()
    assert isinstance(result['exc'], KeyboardInterrupt)
    assert loop.run_until_complete(asyncio.sleep(0.01, 'ok')) == 'ok'
    async def cleanup():
        others = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in others: t.cancel()
        await asyncio.gather(*others, return_exceptions=True)
    loop.run_until_complete(cleanup())
    loop.close()

def test_ki_at_run_entry_no_orphan():
    """A KI surfacing at Handle._run entry must requeue, not drop, the handle.

    The dropped case orphans a task whose wakeup the handle carried: its waiter is
    already done, so cancel() cannot reach it and cleanup hangs forever. Ten rounds
    of injection against a fast sleep loop reproduced the drop within two rounds
    before the traceback-depth requeue rule; see kernmini meta/ROUGH.md rung 6.
    """
    for i in range(10):
        loop = loopmini.new_event_loop()
        result = {}
        def target():
            async def spin():
                while True: await asyncio.sleep(0.001)
            try: loop.run_until_complete(spin())
            except BaseException as e: result['exc'] = e
        th = threading.Thread(target=target)
        th.start()
        time.sleep(0.05)
        assert ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(th.ident), ctypes.py_object(KeyboardInterrupt)) == 1
        th.join(5)
        assert not th.is_alive(), f'round {i}: injection lost'
        async def cleanup():
            others = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for t in others: t.cancel()
            await asyncio.gather(*others, return_exceptions=True)
        done = threading.Event()
        def do_cleanup():
            loop.run_until_complete(cleanup())
            done.set()
        t2 = threading.Thread(target=do_cleanup, daemon=True)
        t2.start()
        assert done.wait(5), f'round {i}: cleanup hung (orphaned task)'
        loop.close()

def test_tls(tmp_path):
    import ssl, subprocess
    cert,key = tmp_path/'cert.pem', tmp_path/'key.pem'
    cmd = ['openssl','req','-x509','-newkey','rsa:2048','-keyout',str(key),'-out',str(cert),
        '-days','1','-nodes','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost']
    subprocess.run(cmd, check=True, capture_output=True)
    sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    sctx.load_cert_chain(cert, key)
    cctx = ssl.create_default_context(cafile=str(cert))
    async def main():
        async def handler(reader, writer):
            data = await reader.readline()
            writer.write(data.upper())
            await writer.drain()
            writer.close()
        server = await asyncio.start_server(handler, '127.0.0.1', 0, ssl=sctx)
        port = server.sockets[0].getsockname()[1]
        reader,writer = await asyncio.open_connection('127.0.0.1', port, ssl=cctx, server_hostname='localhost')
        writer.write(b'secure hello\n')
        await writer.drain()
        assert await reader.readline() == b'SECURE HELLO\n'
        writer.close()
        try: await writer.wait_closed()
        except ssl.SSLError: pass
        server.close()
        await server.wait_closed()
    run(main())

def test_subprocess():
    async def main():
        code = r'import sys; print(input().upper()); print("err", file=sys.stderr)'
        proc = await asyncio.create_subprocess_exec(sys.executable, '-c', code,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out,err = await proc.communicate(b'hello\n')
        assert (out, err, proc.returncode) == (b'HELLO\n', b'err\n', 0)
        spin = await asyncio.create_subprocess_exec(sys.executable, '-c',
            'import time; print("ready", flush=True); time.sleep(30)', stdout=asyncio.subprocess.PIPE)
        assert await spin.stdout.readline() == b'ready\n'
        spin.terminate()
        assert await spin.wait() == -signal.SIGTERM
        shell = await asyncio.create_subprocess_shell('echo shell-ok', stdout=asyncio.subprocess.PIPE)
        out,_ = await shell.communicate()
        assert out == b'shell-ok\n'
    run(main())

def test_interrupt_torture_under_load():
    """Kernel-style synchronous interrupts under load.

    KeyboardInterrupt is injected ONLY while a marked 'user cell' callback runs busy
    Python, mirroring kernmini's sync_execution_context contract. Unscoped injection can
    kill arbitrary machinery callbacks (connection_lost, waiter completions) and strand
    their waiters, on the standard loop just as here; scoping is the kernel's answer.
    The loop must survive every injection, keep all stream tasks progressing, keep
    accepting cross-thread callbacks, and shut down cleanly afterwards.
    """
    duration = float(os.environ.get('LOOPMINI_TORTURE_SECONDS', '0.5'))
    loop = loopmini.new_event_loop()
    n = 10
    counts = [0]*n
    ticks,cells,ki_in_cell,ki_outside = [0],[0],[0],[0]
    window = [0.0]
    done = threading.Event()

    def cell():
        # Busy period must exceed the GIL switch interval (5ms), or the injector thread
        # only ever gets the GIL while the window is closed and can never fire. The window
        # is published as a deadline so the injector can leave a margin (see below).
        def busy():
            window[0] = time.perf_counter() + 0.02
            while time.perf_counter() < window[0]: pass
        # An async-injected exception raised at an eval-breaker check escapes the raising
        # frame even past a same-frame try/except (observed on CPython 3.13.15), so the
        # guard must sit in a parent frame, exactly where a kernel shell's guard sits.
        try:
            busy()
            cells[0] += 1
        except KeyboardInterrupt: ki_in_cell[0] += 1
        finally: window[0] = 0.0
        if not done.is_set(): loop.call_later(0.001, cell)

    async def workload():
        async def handler(reader, writer):
            while (line := await reader.readline()):
                writer.write(line)
                await writer.drain()
        server = await asyncio.start_server(handler, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        async def pinger(i):
            reader,writer = await asyncio.open_connection('127.0.0.1', port)
            try:
                while not done.is_set():
                    writer.write(b'ping\n')
                    await writer.drain()
                    assert await reader.readline() == b'ping\n'
                    counts[i] += 1
                    await asyncio.sleep(random.uniform(0, 0.002))
            finally: writer.close()
        loop.call_soon(cell)
        pingers = [asyncio.ensure_future(pinger(i)) for i in range(n)]
        while not done.is_set(): await asyncio.sleep(0.01)
        for p in pingers: p.cancel()
        await asyncio.gather(*pingers, return_exceptions=True)
        await asyncio.sleep(0.05)  # let server-side handlers see EOF and finish
        server.close()
        await server.wait_closed()

    def run_loop():
        fut = asyncio.ensure_future(workload(), loop=loop)
        while not fut.done():
            try: loop.run_until_complete(fut)
            except KeyboardInterrupt: ki_outside[0] += 1
        fut.result()
    th = threading.Thread(target=run_loop, daemon=True)
    th.start()

    def hammer_threadsafe():
        def tick(): ticks[0] += 1
        while not done.is_set():
            loop.call_soon_threadsafe(tick)
            time.sleep(0.0005)
    hth = threading.Thread(target=hammer_threadsafe, daemon=True)
    hth.start()

    time.sleep(0.3)
    injections,last = 0,0.0
    t_end = time.monotonic() + duration
    while time.monotonic() < t_end:
        time.sleep(random.uniform(0.001, 0.01))
        # Inject only once per window and with >=5ms of it left: the exception lands at
        # the loop thread's next GIL handoff, and the margin keeps that inside the cell,
        # never in loop machinery (whose death would strand waiters).
        w = window[0]
        if w != last and time.perf_counter() < w - 0.005:
            last = w
            assert ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(th.ident), ctypes.py_object(KeyboardInterrupt)) == 1
            injections += 1

    before = list(counts)
    tick_before,cell_before = ticks[0],cells[0]
    time.sleep(0.5)
    stalled = [i for i in range(n) if counts[i] == before[i]]
    tick_ok,cell_ok = ticks[0] > tick_before, cells[0]+ki_in_cell[0] > cell_before
    done.set()
    th.join(10)
    hth.join(2)
    assert injections > duration * 30, f'injector barely fired ({injections}); test needs rebalancing'
    assert ki_in_cell[0] + ki_outside[0] == injections
    assert stalled == [] and tick_ok and cell_ok and not th.is_alive()
    loop.close()
