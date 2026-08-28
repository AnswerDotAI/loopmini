"An asyncio event loop whose reactor (fd readiness, timers, cross-thread wakeup) runs in Rust."
import asyncio, concurrent.futures, errno, functools, itertools, os, signal, socket, ssl as ssl_mod, stat as stat_mod
import subprocess as subprocess_mod, sys, threading, time, traceback, warnings
from asyncio import base_events, events, futures, sslproto, tasks
import weakref
from ._core import Reactor
from .transports import SockTransport, MiniServer, DatagramTransport, ReadPipeTransport, WritePipeTransport
from .subproc import SubprocessTransport

__all__ = ["Loop", "new_event_loop"]

def _fileno(fd): return fd if isinstance(fd, int) else fd.fileno()

def _check_callback(callback, name):
    if asyncio.iscoroutine(callback) or asyncio.iscoroutinefunction(callback): raise TypeError(f'coroutines cannot be used with {name}()')
    if not callable(callback): raise TypeError(f'a callable object was expected by {name}(), got {callback!r}')

def _sighandler_noop(signum, frame): pass

def _timed_run(h, run):
    "Debug-mode dispatch: log callbacks exceeding `loop.slow_callback_duration`, like the standard loop"
    t0 = time.monotonic()
    run()
    dt = time.monotonic() - t0
    if dt >= h._loop.slow_callback_duration: base_events.logger.warning('Executing %s took %.3f seconds', base_events._format_handle(h), dt)

class _TimedHandle(events.Handle):
    def _run(self): _timed_run(self, lambda: events.Handle._run(self))

class _TimerHandle(events.TimerHandle):
    "A stock TimerHandle plus the reactor key that lets cancellation drop the timer at once."
    __slots__ = ('_reactor_key',)

class _TimedTimerHandle(_TimerHandle):
    def _run(self): _timed_run(self, lambda: events.TimerHandle._run(self))

# Tests and log scrapers match on '<Handle ...>' reprs, so the subclasses keep those names
_TimedHandle.__name__ = _TimedHandle.__qualname__ = 'Handle'
_TimerHandle.__name__ = _TimerHandle.__qualname__ = 'TimerHandle'
_TimedTimerHandle.__name__ = _TimedTimerHandle.__qualname__ = 'TimerHandle'

class Loop(asyncio.AbstractEventLoop):
    def __init__(self,
        reactor=None, # A `_core.Reactor`-shaped object; an embedding host passes one on its own runtime
    ):
        self._r,self._running,self._closed,self._debug = reactor if reactor is not None else Reactor(),False,False,False
        # Anchor the reactor clock to time.monotonic (same underlying clock, so the
        # offset is constant): libraries compare loop.time() against monotonic directly
        self._time_offset = time.monotonic() - self._r.time()
        self._thread_id = None
        self.slow_callback_duration = 0.1
        self._asyncgens = weakref.WeakSet()
        self._asyncgens_shutdown_called = False
        self._default_executor = self._exception_handler = self._task_factory = None
        self._signal_handlers = {}
        self._readers,self._writers = {},{}

    def time(self): return self._r.time() + self._time_offset

    def _check_thread(self):
        # Debug-mode guard, as in the standard loop: catches non-threadsafe cross-thread calls
        if self._thread_id is None or self._thread_id == threading.get_ident(): return
        raise RuntimeError('Non-thread-safe operation invoked on an event loop other than the current one')

    def call_soon(self, callback, *args, context=None):
        self._check_closed()
        _check_callback(callback, 'call_soon')
        if self._debug: self._check_thread()
        h = (_TimedHandle if self._debug else events.Handle)(callback, args, self, context)
        if h._source_traceback: del h._source_traceback[-1]
        self._r.schedule(h)
        return h

    def call_soon_threadsafe(self, callback, *args, context=None):
        self._check_closed()
        _check_callback(callback, 'call_soon_threadsafe')
        h = (_TimedHandle if self._debug else events.Handle)(callback, args, self, context)
        if h._source_traceback: del h._source_traceback[-1]
        self._r.schedule_ts(h)
        return h

    def call_later(self, delay, callback, *args, context=None):
        _check_callback(callback, 'call_later')
        h = self.call_at(self.time()+delay, callback, *args, context=context)
        if h._source_traceback: del h._source_traceback[-1]
        return h

    def call_at(self, when, callback, *args, context=None):
        self._check_closed()
        _check_callback(callback, 'call_at')
        if self._debug: self._check_thread()
        h = (_TimedTimerHandle if self._debug else _TimerHandle)(when, callback, args, self, context)
        if h._source_traceback: del h._source_traceback[-1]
        h._reactor_key = self._r.schedule_at(when - self._time_offset, h)
        return h

    def _timer_handle_cancelled(self, handle):
        # False (already fired) is fine: dispatch skips the promoted handle's cancelled flag
        self._r.cancel_timer(handle._reactor_key)

    def create_future(self): return futures.Future(loop=self)

    def create_task(self, coro, *, name=None, context=None, eager_start=False):
        self._check_closed()
        if self._task_factory is None: return tasks.Task(coro, loop=self, name=name, context=context, eager_start=eager_start)
        task = self._task_factory(self, coro) if context is None else self._task_factory(self, coro, context=context)
        if name is not None: task.set_name(name)
        return task

    def set_task_factory(self, factory):
        if factory is not None and not callable(factory): raise TypeError('task factory must be a callable or None')
        self._task_factory = factory
    def get_task_factory(self): return self._task_factory

    def run_forever(self):
        self._check_closed()
        if self._running: raise RuntimeError('This event loop is already running')
        if events._get_running_loop() is not None: raise RuntimeError('Cannot run the event loop while another loop is running')
        main = threading.current_thread() is threading.main_thread()
        if main: old_wakeup = self._setup_signal_wakeup()
        old_agen_hooks = sys.get_asyncgen_hooks()
        sys.set_asyncgen_hooks(firstiter=self._asyncgen_firstiter_hook, finalizer=self._asyncgen_finalizer_hook)
        self._running = True
        self._thread_id = threading.get_ident()
        events._set_running_loop(self)
        try: self._r.run()
        finally:
            events._set_running_loop(None)
            self._running = False
            self._thread_id = None
            sys.set_asyncgen_hooks(*old_agen_hooks)
            if main: self._teardown_signal_wakeup(old_wakeup)

    def _setup_signal_wakeup(self):
        self._ssock,self._csock = socket.socketpair()
        for s in (self._ssock,self._csock): s.setblocking(False)
        old = signal.set_wakeup_fd(self._csock.fileno())
        self.add_reader(self._ssock.fileno(), self._drain_signal_sock)
        return old

    def _drain_signal_sock(self):
        try: data = self._ssock.recv(4096)
        except (BlockingIOError, InterruptedError): return
        for sig in data:
            h = self._signal_handlers.get(sig)
            if h is not None and not h.cancelled(): self._r.schedule(h)

    def add_signal_handler(self, sig, callback, *args):
        _check_callback(callback, 'add_signal_handler')
        h = events.Handle(callback, args, self, None)
        self._signal_handlers[sig] = h
        try: signal.signal(sig, _sighandler_noop)
        except OSError as e:
            del self._signal_handlers[sig]
            if e.errno == errno.EINVAL: raise RuntimeError(f'sig {sig} cannot be caught') from None
            raise
        except ValueError:
            del self._signal_handlers[sig]
            raise

    def remove_signal_handler(self, sig):
        if sig not in self._signal_handlers: return False
        del self._signal_handlers[sig]
        try: signal.signal(sig, signal.default_int_handler if sig == signal.SIGINT else signal.SIG_DFL)
        except OSError as e:
            if e.errno == errno.EINVAL: raise RuntimeError(f'sig {sig} cannot be caught') from None
            raise
        return True

    def _teardown_signal_wakeup(self, old):
        self.remove_reader(self._ssock.fileno())
        signal.set_wakeup_fd(old)
        self._ssock.close()
        self._csock.close()

    def _run_until_complete_cb(self, fut):
        # KeyboardInterrupt/SystemExit propagate via run_forever; retrieving the exception
        # here (and not stopping) is CPython's gh-issue-336 behaviour
        if not fut.cancelled():
            exc = fut.exception()
            if isinstance(exc, (SystemExit, KeyboardInterrupt)): return
        self.stop()

    def run_until_complete(self, future):
        fut = tasks.ensure_future(future, loop=self)
        fut.add_done_callback(self._run_until_complete_cb)
        try: self.run_forever()
        finally: fut.remove_done_callback(self._run_until_complete_cb)
        if not fut.done(): raise RuntimeError('Event loop stopped before Future completed.')
        return fut.result()

    def stop(self): self._r.stop()
    def is_running(self): return self._running
    def is_closed(self): return self._closed

    def close(self):
        if self._running: raise RuntimeError('Cannot close a running event loop')
        if self._closed: return
        self._closed = True
        for sig in list(self._signal_handlers): self.remove_signal_handler(sig)
        self._r.close()
        if self._default_executor is not None: self._default_executor.shutdown(wait=False)

    def _check_closed(self):
        if self._closed: raise RuntimeError('Event loop is closed')

    def _asyncgen_firstiter_hook(self, agen):
        if self._asyncgens_shutdown_called:
            self.call_exception_handler(dict(message=f'asynchronous generator {agen!r} was scheduled after loop.shutdown_asyncgens() call', asyncgen=agen))
        self._asyncgens.add(agen)

    def _asyncgen_finalizer_hook(self, agen):
        self._asyncgens.discard(agen)
        if not self.is_closed(): self.call_soon_threadsafe(self.create_task, agen.aclose())

    async def shutdown_asyncgens(self):
        self._asyncgens_shutdown_called = True
        if not len(self._asyncgens): return
        closing = list(self._asyncgens)
        self._asyncgens.clear()
        results = await tasks.gather(*[ag.aclose() for ag in closing], return_exceptions=True)
        for result, agen in zip(results, closing):
            if isinstance(result, Exception):
                self.call_exception_handler(dict(message=f'an error occurred during closing of asynchronous generator {agen!r}',
                    exception=result, asyncgen=agen))

    async def shutdown_default_executor(self, timeout=None):
        ex,self._default_executor = self._default_executor,None
        if ex is None: return
        fut = self.create_future()
        def _shut():
            ex.shutdown(wait=True)
            try: self.call_soon_threadsafe(futures._set_result_unless_cancelled, fut, None)
            except RuntimeError: pass  # loop closed after a timed-out join, as in the standard loop
        threading.Thread(target=_shut).start()
        try: await (fut if timeout is None else tasks.wait_for(fut, timeout))
        except TimeoutError: warnings.warn(f'executor did not finish joining its threads within {timeout} seconds', RuntimeWarning)

    def _add_io(self, fd, callback, args, handles, add):
        fd = _fileno(fd)
        h = events.Handle(callback, args, self, None)
        old = handles.get(fd)
        if old is not None: old.cancel()
        handles[fd] = h
        add(fd, h)

    def _remove_io(self, fd, handles, remove):
        # A queued readiness callback may still fire this turn; cancelling the stored
        # handle makes the dispatcher skip it, matching the standard loop's remove_reader
        fd = _fileno(fd)
        h = handles.pop(fd, None)
        if h is not None: h.cancel()
        if fd < 0: return False
        return remove(fd)

    def add_reader(self, fd, callback, *args): self._add_io(fd, callback, args, self._readers, self._r.add_reader)
    def remove_reader(self, fd): return self._remove_io(fd, self._readers, self._r.remove_reader)
    def add_writer(self, fd, callback, *args): self._add_io(fd, callback, args, self._writers, self._r.add_writer)
    def remove_writer(self, fd): return self._remove_io(fd, self._writers, self._r.remove_writer)

    def _wait_io(self, fd, handles, add, rm):
        fut = self.create_future()
        def cb():
            if not fut.done(): fut.set_result(None)
        add(fd, cb)
        h = handles[fd]
        # Remove only our own registration: a cancelled wait must not tear down a newer one
        def cleanup(f):
            if handles.get(fd) is h: rm(fd)
        fut.add_done_callback(cleanup)
        return fut

    def _readable(self, fd): return self._wait_io(fd, self._readers, self.add_reader, self.remove_reader)
    def _writable(self, fd): return self._wait_io(fd, self._writers, self.add_writer, self.remove_writer)

    @staticmethod
    def _check_nonblocking(sock):
        if sock.getblocking(): raise ValueError('the socket must be non-blocking')

    async def sock_recv(self, sock, nbytes):
        self._check_nonblocking(sock)
        while True:
            try: return sock.recv(nbytes)
            except (BlockingIOError, InterruptedError): await self._readable(sock.fileno())

    async def sock_sendall(self, sock, data):
        self._check_nonblocking(sock)
        view = memoryview(data)
        while view:
            try: view = view[sock.send(view):]
            except (BlockingIOError, InterruptedError): await self._writable(sock.fileno())

    async def sock_accept(self, sock):
        self._check_nonblocking(sock)
        while True:
            try:
                conn,addr = sock.accept()
                conn.setblocking(False)
                return conn,addr
            except (BlockingIOError, InterruptedError): await self._readable(sock.fileno())

    async def sock_connect(self, sock, address):
        self._check_nonblocking(sock)
        try: return sock.connect(address)
        except (BlockingIOError, InterruptedError): pass
        await self._writable(sock.fileno())
        err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err: raise OSError(err, f'Connect call failed {address}')

    async def getaddrinfo(self, host, port, *, family=0, type=0, proto=0, flags=0):
        return await self.run_in_executor(None, functools.partial(socket.getaddrinfo, host, port, family=family, type=type, proto=proto, flags=flags))

    async def getnameinfo(self, sockaddr, flags=0):
        return await self.run_in_executor(None, functools.partial(socket.getnameinfo, sockaddr, flags))

    async def _wrap_socket(self, sock, protocol_factory, sslcontext, server_hostname, server_side=False, server=None):
        protocol = protocol_factory()
        waiter = self.create_future()
        if sslcontext is not None:
            sslp = sslproto.SSLProtocol(self, protocol, sslcontext, waiter, server_side=server_side, server_hostname=server_hostname)
            SockTransport(self, sock, sslp, server=server)
            transport = sslp._app_transport
        else: transport = SockTransport(self, sock, protocol, waiter=waiter, server=server)
        try: await waiter
        except BaseException:
            transport.close()
            raise
        return transport, protocol

    @staticmethod
    def _check_ssl_args(ssl, ssl_handshake_timeout):
        if ssl_handshake_timeout is not None and not ssl: raise ValueError('ssl_handshake_timeout is only meaningful with ssl')

    async def create_connection(self, protocol_factory, host=None, port=None, *, ssl=None, sock=None, family=0,
        proto=0, flags=0, local_addr=None, server_hostname=None, ssl_handshake_timeout=None, **kw):
        if server_hostname is not None and not ssl: raise ValueError('server_hostname is only meaningful with ssl')
        self._check_ssl_args(ssl, ssl_handshake_timeout)
        if sock is None:
            infos = await self.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM, proto=proto, flags=flags)
            if not infos: raise OSError(f'getaddrinfo({host!r}) returned empty list')
            errors = []
            for af,st,pr,_,addr in infos:
                sock = socket.socket(af, st, pr)
                sock.setblocking(False)
                try:
                    if local_addr is not None: sock.bind(local_addr)
                    await self.sock_connect(sock, addr)
                    break
                except OSError as e:
                    sock.close()
                    sock = None
                    errors.append(e)
                except BaseException:
                    sock.close()
                    raise
            if sock is None:
                try:
                    if len(errors) == 1: raise errors[0]
                    raise OSError(f'could not connect to {host}:{port}', errors)
                finally: errors = None  # a kept list would give the exception a referrer
        else: sock.setblocking(False)
        sslcontext = None
        if ssl:
            sslcontext = ssl_mod.create_default_context() if ssl is True else ssl
            if server_hostname is None: server_hostname = host
        return await self._wrap_socket(sock, protocol_factory, sslcontext, server_hostname)

    async def create_server(self, protocol_factory, host=None, port=None, *, sock=None, backlog=100, ssl=None,
        family=socket.AF_UNSPEC, flags=socket.AI_PASSIVE, reuse_address=None, reuse_port=None,
        start_serving=True, ssl_handshake_timeout=None, **kw):
        if ssl is True: raise ValueError('ssl=True needs an SSLContext holding the server certificate')
        self._check_ssl_args(ssl, ssl_handshake_timeout)
        if host is not None or port is not None:
            if sock is not None: raise ValueError('host/port and sock can not be specified at the same time')
            if host == '': hosts = [None]
            elif isinstance(host, str) or not hasattr(host, '__iter__'): hosts = [host]
            else: hosts = host
            all_infos = await tasks.gather(*[self.getaddrinfo(h, port, family=family, type=socket.SOCK_STREAM, flags=flags) for h in hosts])
            infos = set(itertools.chain.from_iterable(all_infos))
            socks,completed = [],False
            try:
                for af,st,pr,_,addr in infos:
                    try: s = socket.socket(af, st, pr)
                    except OSError: continue  # bad family/type/protocol combination
                    socks.append(s)
                    if reuse_address or reuse_address is None: s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    if reuse_port and af in (socket.AF_INET, socket.AF_INET6): s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                    # Disable dual-stack on the ipv6 socket, or its bind of :: takes the port over for v4 too
                    if socket.has_ipv6 and af == socket.AF_INET6 and hasattr(socket, 'IPPROTO_IPV6'):
                        s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, True)
                    try: s.bind(addr)
                    except OSError as err:
                        msg = f'error while attempting to bind on address {addr!r}: {str(err).lower()}'
                        if err.errno == errno.EADDRNOTAVAIL:  # assume the family is not enabled (bpo-30945)
                            socks.pop()
                            s.close()
                            continue
                        raise OSError(err.errno, msg) from None
                if not socks: raise OSError(f'could not bind on any address out of {[i[4] for i in infos]!r}')
                completed = True
            finally:
                if not completed:
                    for s in socks: s.close()
        else:
            if sock is None: raise ValueError('Neither host/port nor sock were specified')
            if sock.type != socket.SOCK_STREAM: raise ValueError(f'A Stream Socket was expected, got {sock!r}')
            socks = [sock]
        for s in socks:
            s.listen(backlog)
            s.setblocking(False)
        server = MiniServer(self, socks, protocol_factory, ssl)
        if start_serving: server._start_serving()
        return server

    async def create_unix_connection(self, protocol_factory, path=None, *, ssl=None, sock=None,
        server_hostname=None, ssl_handshake_timeout=None, **kw):
        if ssl and server_hostname is None: raise ValueError('you have to pass server_hostname when using ssl')
        if server_hostname is not None and not ssl: raise ValueError('server_hostname is only meaningful with ssl')
        self._check_ssl_args(ssl, ssl_handshake_timeout)
        if sock is None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.setblocking(False)
            try: await self.sock_connect(sock, path)
            except BaseException:
                sock.close()
                raise
        else: sock.setblocking(False)
        sslcontext = (ssl_mod.create_default_context() if ssl is True else ssl) if ssl else None
        return await self._wrap_socket(sock, protocol_factory, sslcontext, server_hostname)

    async def create_unix_server(self, protocol_factory, path=None, *, sock=None, backlog=100, ssl=None,
        start_serving=True, ssl_handshake_timeout=None, cleanup_socket=True, **kw):
        if ssl is True: raise ValueError('ssl=True needs an SSLContext holding the server certificate')
        self._check_ssl_args(ssl, ssl_handshake_timeout)
        if sock is None:
            def _unlink_stale():
                try:
                    if stat_mod.S_ISSOCK(os.stat(path).st_mode): os.remove(path)
                except (FileNotFoundError, OSError): pass
            await self.run_in_executor(None, _unlink_stale)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try: sock.bind(path)
            except OSError as e:
                sock.close()
                if e.errno == errno.EADDRINUSE: raise OSError(errno.EADDRINUSE, f'Address {path!r} is already in use') from None
                raise
        else: path = sock.getsockname()
        sock.listen(backlog)
        sock.setblocking(False)
        unlink = ()
        if cleanup_socket and path and path[0] not in (0, '\x00'):
            try: unlink = ((path, (await self.run_in_executor(None, os.stat, path)).st_ino),)
            except FileNotFoundError: pass
        server = MiniServer(self, [sock], protocol_factory, ssl, unlink_paths=unlink)
        if start_serving: server._start_serving()
        return server

    async def connect_accepted_socket(self, protocol_factory, sock, *, ssl=None, ssl_handshake_timeout=None, **kw):
        sock.setblocking(False)
        return await self._wrap_socket(sock, protocol_factory, ssl, None, server_side=True)

    async def create_datagram_endpoint(self, protocol_factory, local_addr=None, remote_addr=None, *, family=0,
        proto=0, flags=0, reuse_port=None, allow_broadcast=None, sock=None):
        if sock is None:
            if remote_addr or local_addr:
                infos = await self.getaddrinfo(*(remote_addr or local_addr), family=family,
                    type=socket.SOCK_DGRAM, proto=proto, flags=flags)
                if not infos: raise OSError('getaddrinfo returned empty list')
                family = infos[0][0]
            sock = socket.socket(family or socket.AF_INET, socket.SOCK_DGRAM, proto)
            try:
                if reuse_port: sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                if allow_broadcast: sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                sock.setblocking(False)
                if local_addr: sock.bind(local_addr)
                if remote_addr: await self.sock_connect(sock, tuple(remote_addr))
            except BaseException:
                sock.close()
                raise
        else: sock.setblocking(False)
        # The canonical peer address (a 4-tuple for IPv6), whatever form the caller used
        remote_addr = None
        try: remote_addr = sock.getpeername()
        except OSError: pass
        protocol = protocol_factory()
        waiter = self.create_future()
        transport = DatagramTransport(self, sock, protocol, address=remote_addr or None, waiter=waiter)
        try: await waiter
        except BaseException:
            transport.close()
            raise
        return transport, protocol

    async def connect_read_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        waiter = self.create_future()
        transport = ReadPipeTransport(self, pipe, protocol, waiter)
        try: await waiter
        except BaseException:
            transport.close()
            raise
        return transport, protocol

    async def connect_write_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        waiter = self.create_future()
        transport = WritePipeTransport(self, pipe, protocol, waiter)
        try: await waiter
        except BaseException:
            transport.close()
            raise
        return transport, protocol

    async def _subprocess(self, protocol_factory, args, shell, stdin, stdout, stderr, bufsize,
        universal_newlines, encoding, errors, text, **kwargs):
        if universal_newlines or encoding or errors or text: raise ValueError('text mode not supported by the event loop')
        protocol = protocol_factory()
        transport = SubprocessTransport(self, protocol, args, shell, stdin, stdout, stderr, bufsize, **kwargs)
        await transport._setup()
        return transport, protocol

    async def subprocess_exec(self, protocol_factory, program, *args, stdin=subprocess_mod.PIPE,
        stdout=subprocess_mod.PIPE, stderr=subprocess_mod.PIPE, universal_newlines=False, shell=False,
        bufsize=0, encoding=None, errors=None, text=None, **kwargs):
        if shell: raise ValueError('subprocess_exec() does not take a shell argument of True')
        return await self._subprocess(protocol_factory, (program,)+args, False, stdin, stdout, stderr,
            bufsize, universal_newlines, encoding, errors, text, **kwargs)

    async def subprocess_shell(self, protocol_factory, cmd, *, stdin=subprocess_mod.PIPE,
        stdout=subprocess_mod.PIPE, stderr=subprocess_mod.PIPE, universal_newlines=False, shell=True,
        bufsize=0, encoding=None, errors=None, text=None, **kwargs):
        if not isinstance(cmd, (bytes, str)): raise ValueError('cmd must be a string')
        if not shell: raise ValueError('subprocess_shell() requires shell=True')
        return await self._subprocess(protocol_factory, cmd, True, stdin, stdout, stderr,
            bufsize, universal_newlines, encoding, errors, text, **kwargs)

    async def start_tls(self, transport, protocol, sslcontext, *, server_side=False, server_hostname=None,
        ssl_handshake_timeout=None, ssl_shutdown_timeout=None):
        waiter = self.create_future()
        sslp = sslproto.SSLProtocol(self, protocol, sslcontext, waiter, server_side=server_side,
            server_hostname=server_hostname, call_connection_made=False)
        transport.pause_reading()
        # gh-142352: TLS bytes may already sit in a server-side StreamReader; move them
        # into the SSL protocol's incoming BIO or the handshake never sees them
        if server_side and isinstance(protocol, asyncio.streams.StreamReaderProtocol):
            reader = getattr(protocol, '_stream_reader', None)
            if reader is not None and reader._buffer:
                sslp._incoming.write(reader._buffer)
                reader._buffer.clear()
        transport.set_protocol(sslp)
        self.call_soon(sslp.connection_made, transport)
        self.call_soon(transport.resume_reading)
        try: await waiter
        except BaseException:
            transport.close()
            raise
        return sslp._app_transport

    def run_in_executor(self, executor, func, *args):
        self._check_closed()
        if executor is None:
            if self._default_executor is None: self._default_executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix='asyncio')
            executor = self._default_executor
        return futures.wrap_future(executor.submit(func, *args), loop=self)

    def set_default_executor(self, executor): self._default_executor = executor

    def get_debug(self): return self._debug
    def set_debug(self, enabled): self._debug = bool(enabled)

    def set_exception_handler(self, handler): self._exception_handler = handler
    def get_exception_handler(self): return self._exception_handler

    def call_exception_handler(self, context):
        if self._exception_handler is None: return self.default_exception_handler(context)
        try: self._exception_handler(self, context)
        except (SystemExit, KeyboardInterrupt): raise
        except BaseException as exc:
            # A broken handler must not take down the loop: report it via the default one
            try: self.default_exception_handler(dict(message='Unhandled error in exception handler',
                exception=exc, context=context))
            except (SystemExit, KeyboardInterrupt): raise
            except BaseException: base_events.logger.error('Exception in default exception handler', exc_info=True)

    def default_exception_handler(self, context):
        message = context.get('message') or 'Unhandled exception in event loop'
        exc = context.get('exception')
        exc_info = (type(exc), exc, exc.__traceback__) if exc is not None else False
        log_lines = [message]
        for key in sorted(context):
            if key in ('message', 'exception'): continue
            value = context[key]
            if key == 'source_traceback':
                value = 'Object created at (most recent call last):\n' + ''.join(traceback.format_list(value)).rstrip()
            elif key == 'handle_traceback':
                value = 'Handle created at (most recent call last):\n' + ''.join(traceback.format_list(value)).rstrip()
            else: value = repr(value)
            log_lines.append(f'{key}: {value}')
        base_events.logger.error('\n'.join(log_lines), exc_info=exc_info)

def new_event_loop(reactor=None):
    "An event loop on a Rust reactor, for `asyncio.run(..., loop_factory=new_event_loop)`."
    return Loop(reactor)
