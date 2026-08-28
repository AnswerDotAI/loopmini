"An asyncio event loop whose reactor (fd readiness, timers, cross-thread wakeup) runs in Rust."
import errno, functools, os, signal, socket, subprocess, sys, threading, time, weakref
from asyncio import base_events, events, selector_events, sslproto, unix_events
from ._core import Reactor
from .transports import SockTransport, DatagramTransport
from .subproc import SubprocessTransport

__all__ = ["Loop", "new_event_loop"]

_SelLoop = selector_events.BaseSelectorEventLoop
_UnixLoop = unix_events._UnixSelectorEventLoop

def _fileno(fd): return fd if isinstance(fd, int) else fd.fileno()

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

class Loop(base_events.BaseEventLoop):
    def __init__(self,
        reactor=None, # A `_core.Reactor`-shaped object; an embedding host passes one on its own runtime
    ):
        super().__init__()
        self._r = reactor if reactor is not None else Reactor()
        # Anchor the reactor clock to time.monotonic (same underlying clock, so the
        # offset is constant): libraries compare loop.time() against monotonic directly
        self._time_offset = time.monotonic() - self._r.time()
        self._signal_handlers = {}
        self._unix_server_sockets = {}
        self._transports = weakref.WeakValueDictionary()
        self._readers,self._writers = {},{}

    def time(self): return self._r.time() + self._time_offset

    def _call_soon(self, callback, args, context):
        h = (_TimedHandle if self._debug else events.Handle)(callback, args, self, context)
        if h._source_traceback: del h._source_traceback[-1]
        self._r.schedule(h)
        return h

    def call_soon_threadsafe(self, callback, *args, context=None):
        self._check_closed()
        self._check_callback(callback, 'call_soon_threadsafe')
        h = (_TimedHandle if self._debug else events.Handle)(callback, args, self, context)
        if h._source_traceback: del h._source_traceback[-1]
        self._r.schedule_ts(h)
        return h

    def call_at(self, when, callback, *args, context=None):
        if when is None: raise TypeError('when cannot be None')
        self._check_closed()
        if self._debug:
            self._check_thread()
            self._check_callback(callback, 'call_at')
        h = (_TimedTimerHandle if self._debug else _TimerHandle)(when, callback, args, self, context)
        if h._source_traceback: del h._source_traceback[-1]
        h._reactor_key = self._r.schedule_at(when - self._time_offset, h)
        return h

    def _timer_handle_cancelled(self, handle):
        # False (already fired) is fine: dispatch skips the promoted handle's cancelled flag
        self._r.cancel_timer(handle._reactor_key)

    def run_forever(self):
        self._run_forever_setup()
        main = threading.current_thread() is threading.main_thread()
        try:
            if main: old_wakeup = self._setup_signal_wakeup()
            try: self._r.run()
            finally:
                if main: self._teardown_signal_wakeup(old_wakeup)
        finally: self._run_forever_cleanup()

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
        self._check_callback(callback, 'add_signal_handler')
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

    def stop(self): self._r.stop()

    def close(self):
        if self.is_running(): raise RuntimeError('Cannot close a running event loop')
        if self.is_closed(): return
        for sig in list(self._signal_handlers): self.remove_signal_handler(sig)
        self._r.close()
        super().close()

    def _add_io(self, fd, callback, args, handles, add, context=None):
        fd = _fileno(fd)
        h = events.Handle(callback, args, self, context)
        old = handles.get(fd)
        if old is not None: old.cancel()
        handles[fd] = h
        add(fd, h)
        return h

    def _remove_io(self, fd, handles, remove):
        # A queued readiness callback may still fire this turn; cancelling the stored
        # handle makes the dispatcher skip it, matching the standard loop's remove_reader
        fd = _fileno(fd)
        h = handles.pop(fd, None)
        if h is not None: h.cancel()
        if fd < 0: return False
        return remove(fd)

    def add_reader(self, fd, callback, *args): return self._add_io(fd, callback, args, self._readers, self._r.add_reader)
    def remove_reader(self, fd): return self._remove_io(fd, self._readers, self._r.remove_reader)
    def add_writer(self, fd, callback, *args): return self._add_io(fd, callback, args, self._writers, self._r.add_writer)
    def remove_writer(self, fd): return self._remove_io(fd, self._writers, self._r.remove_writer)
    def _add_reader(self, fd, callback, *args): return self.add_reader(fd, callback, *args)
    def _remove_reader(self, fd): return self.remove_reader(fd)
    def _add_writer(self, fd, callback, *args): return self.add_writer(fd, callback, *args)
    def _remove_writer(self, fd): return self.remove_writer(fd)

    _ensure_fd_no_transport,_sock_read_done,_sock_write_done = _SelLoop._ensure_fd_no_transport,_SelLoop._sock_read_done,_SelLoop._sock_write_done
    sock_recv,_sock_recv = _SelLoop.sock_recv,_SelLoop._sock_recv
    sock_recv_into,_sock_recv_into = _SelLoop.sock_recv_into,_SelLoop._sock_recv_into
    sock_recvfrom,_sock_recvfrom = _SelLoop.sock_recvfrom,_SelLoop._sock_recvfrom
    sock_recvfrom_into,_sock_recvfrom_into = _SelLoop.sock_recvfrom_into,_SelLoop._sock_recvfrom_into
    sock_sendall,_sock_sendall = _SelLoop.sock_sendall,_SelLoop._sock_sendall
    sock_sendto,_sock_sendto = _SelLoop.sock_sendto,_SelLoop._sock_sendto
    sock_accept,_sock_accept = _SelLoop.sock_accept,_SelLoop._sock_accept
    sock_connect,_sock_connect,_sock_connect_cb = _SelLoop.sock_connect,_SelLoop._sock_connect,_SelLoop._sock_connect_cb

    def _make_socket_transport(self, sock, protocol, waiter=None, *, extra=None, server=None, context=None):
        return SockTransport(self, sock, protocol, waiter, extra, server, context)

    def _make_ssl_transport(self, sock, protocol, sslcontext, waiter=None, *, server_side=False, server_hostname=None, extra=None, server=None,
        ssl_handshake_timeout=None, ssl_shutdown_timeout=None, call_connection_made=True, context=None):
        ssl_protocol = sslproto.SSLProtocol(self, protocol, sslcontext, waiter, server_side, server_hostname,
            call_connection_made=call_connection_made, ssl_handshake_timeout=ssl_handshake_timeout, ssl_shutdown_timeout=ssl_shutdown_timeout)
        SockTransport(self, sock, ssl_protocol, extra=extra, server=server, context=context)
        return ssl_protocol._app_transport

    def _make_datagram_transport(self, sock, protocol, address=None, waiter=None, extra=None):
        return DatagramTransport(self, sock, protocol, address, waiter, extra)
    def _make_read_pipe_transport(self, pipe, protocol, waiter=None, extra=None): return unix_events._UnixReadPipeTransport(self, pipe, protocol, waiter, extra)
    def _make_write_pipe_transport(self, pipe, protocol, waiter=None, extra=None):
        return unix_events._UnixWritePipeTransport(self, pipe, protocol, waiter, extra)

    async def _make_subprocess_transport(self, protocol, args, shell, stdin, stdout, stderr, bufsize, extra=None, **kwargs):
        popen = functools.partial(subprocess.Popen, args, shell=shell, stdin=stdin, stdout=stdout, stderr=stderr, bufsize=bufsize, **kwargs)
        proc = await self.run_in_executor(None, popen)
        waiter = self.create_future()
        transport = SubprocessTransport(self, protocol, args, shell, stdin, stdout, stderr, bufsize, proc, waiter, extra, **kwargs)
        try: await waiter
        except BaseException:
            transport.close()
            await transport._wait()
            raise
        return transport

    _start_serving = _SelLoop._start_serving
    _accept_connection = _SelLoop._accept_connection
    _accept_connection2 = _SelLoop._accept_connection2

    def _stop_serving(self, sock):
        path = sock.getsockname() if sock in self._unix_server_sockets else None
        self.remove_reader(sock.fileno())
        sock.close()
        if path is None: return
        inode = self._unix_server_sockets.pop(sock)
        try:
            if os.stat(path).st_ino == inode: os.unlink(path)
        except OSError: pass

    create_unix_connection = _UnixLoop.create_unix_connection
    create_unix_server = _UnixLoop.create_unix_server

if not hasattr(base_events.BaseEventLoop, '_run_forever_setup'):  # the helpers first appear in 3.12; transcribed from 3.11's inline run_forever, which is frozen
    def _run_forever_setup(self):
        self._check_closed()
        self._check_running()
        self._set_coroutine_origin_tracking(self._debug)
        self._thread_id = threading.get_ident()
        self._old_agen_hooks = sys.get_asyncgen_hooks()
        sys.set_asyncgen_hooks(firstiter=self._asyncgen_firstiter_hook, finalizer=self._asyncgen_finalizer_hook)
        events._set_running_loop(self)
    def _run_forever_cleanup(self):
        self._thread_id = None
        events._set_running_loop(None)
        self._set_coroutine_origin_tracking(False)
        sys.set_asyncgen_hooks(*self._old_agen_hooks)
    Loop._run_forever_setup, Loop._run_forever_cleanup = _run_forever_setup, _run_forever_cleanup

def new_event_loop(reactor=None):
    "An event loop on a Rust reactor, for `asyncio.run(..., loop_factory=new_event_loop)`."
    return Loop(reactor)
