"TCP transports and server for the Rust-reactor loop, feeding stock asyncio protocols."
import asyncio, os, socket, weakref, stat, sys
from asyncio import futures

class _FlowControl:
    "Write-buffer flow control shared by buffering transports, with CPython's _FlowControlMixin semantics."
    _protocol_paused = False
    _high,_low = 65536,16384

    def get_write_buffer_size(self): return len(self._buffer)
    def get_write_buffer_limits(self): return (self._low, self._high)

    def set_write_buffer_limits(self, high=None, low=None):
        if high is None: high = 65536 if low is None else 4*low
        if low is None: low = high//4
        if not high >= low >= 0: raise ValueError(f'high ({high}) must be >= low ({low}) must be >= 0')
        self._high,self._low = high,low
        self._maybe_pause_protocol()

    def _maybe_pause_protocol(self):
        if not self._protocol_paused and len(self._buffer) > self._high:
            self._protocol_paused = True
            try: self._protocol.pause_writing()
            except Exception as e: self._loop.call_exception_handler(dict(message='protocol.pause_writing() failed', exception=e, transport=self, protocol=self._protocol))

    def _maybe_resume_protocol(self):
        if self._protocol_paused and len(self._buffer) <= self._low:
            self._protocol_paused = False
            try: self._protocol.resume_writing()
            except Exception as e: self._loop.call_exception_handler(dict(message='protocol.resume_writing() failed', exception=e, transport=self, protocol=self._protocol))

__all__ = ["SockTransport", "MiniServer"]

class SockTransport(_FlowControl, asyncio.Transport):
    "Bidirectional TCP transport over the loop's fd-readiness primitives."
    max_size = 262144
    _start_tls_compatible = True

    def __init__(self, loop, sock, protocol, waiter=None, extra=None, server=None):
        super().__init__(extra)
        self._extra['socket'] = sock
        try: self._extra['sockname'] = sock.getsockname()
        except OSError: pass
        try: self._extra['peername'] = sock.getpeername()
        except OSError: pass
        self._loop,self._sock,self._protocol,self._server = loop,sock,protocol,server
        self._buffered = isinstance(protocol, asyncio.BufferedProtocol)
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            try: sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError: pass
        self._buffer = bytearray()
        self._closing = self._reading = self._eof = self._lost = False
        if server is not None: server._attach(self)
        loop.call_soon(protocol.connection_made, self)
        loop.call_soon(self.resume_reading)
        if waiter is not None: loop.call_soon(futures._set_result_unless_cancelled, waiter, None)

    def get_protocol(self): return self._protocol
    def set_protocol(self, protocol): self._protocol,self._buffered = protocol,isinstance(protocol, asyncio.BufferedProtocol)
    def is_closing(self): return self._closing
    def is_reading(self): return self._reading

    def pause_reading(self):
        if self._closing or not self._reading: return
        self._reading = False
        self._loop.remove_reader(self._sock.fileno())

    def resume_reading(self):
        if self._closing or self._reading: return
        self._reading = True
        self._loop.add_reader(self._sock.fileno(), self._read_ready)

    def _read_ready(self):
        if not self._reading: return
        try:
            if self._buffered:
                n = self._sock.recv_into(self._protocol.get_buffer(-1))
                if n: return self._protocol.buffer_updated(n)
            else:
                data = self._sock.recv(self.max_size)
                if data: return self._protocol.data_received(data)
        except (BlockingIOError, InterruptedError): return
        except OSError as e: return self._fatal_error(e, 'Fatal read error on socket transport')
        if self._protocol.eof_received():
            self._reading = False
            self._loop.remove_reader(self._sock.fileno())
        else: self.close()

    def write(self, data):
        if not data or self._closing: return
        if not self._buffer:
            try: data = data[self._sock.send(data):]
            except (BlockingIOError, InterruptedError): pass
            except OSError as e: return self._fatal_error(e, 'Fatal write error on socket transport')
            if not data: return
            self._loop.add_writer(self._sock.fileno(), self._write_ready)
        self._buffer.extend(data)
        self._maybe_pause_protocol()

    def writelines(self, list_of_data): self.write(b''.join(list_of_data))

    def _write_ready(self):
        if not self._buffer: return
        try: n = self._sock.send(self._buffer)  # no explicit memoryview: a raising send would leave an export alive in the traceback, and clear() then fails
        except (BlockingIOError, InterruptedError): return
        except OSError as e: return self._fatal_error(e, 'Fatal write error on socket transport')
        del self._buffer[:n]
        self._maybe_resume_protocol()
        if self._buffer: return
        self._loop.remove_writer(self._sock.fileno())
        if self._eof: self._sock.shutdown(socket.SHUT_WR)
        if self._closing: self._schedule_lost(None)

    def can_write_eof(self): return True

    def write_eof(self):
        if self._closing or self._eof: return
        self._eof = True
        if not self._buffer: self._sock.shutdown(socket.SHUT_WR)

    def _schedule_lost(self, exc):
        # close(), abort(), and fatal errors may overlap; connection_lost runs exactly once
        if self._lost: return
        self._lost = True
        self._loop.call_soon(self._call_connection_lost, exc)

    def close(self):
        if self._closing: return
        self._closing = True
        if self._reading:
            self._reading = False
            self._loop.remove_reader(self._sock.fileno())
        if not self._buffer: self._schedule_lost(None)

    def abort(self): self._force_close(None)

    def _fatal_error(self, exc, message):
        if not isinstance(exc, OSError):
            self._loop.call_exception_handler(dict(message=message, exception=exc, transport=self, protocol=self._protocol))
        self._force_close(exc)

    def _force_close(self, exc):
        if self._lost: return
        if self._buffer:
            self._buffer.clear()
            self._loop.remove_writer(self._sock.fileno())
        if self._reading:
            self._reading = False
            self._loop.remove_reader(self._sock.fileno())
        self._closing = True
        self._schedule_lost(exc)

    def _call_connection_lost(self, exc):
        try: self._protocol.connection_lost(exc)
        finally:
            self._sock.close()
            if self._server is not None:
                self._server._detach(self)
                self._server = None

class MiniServer(asyncio.AbstractServer):
    "Listening sockets plus the accept loop; hands accepted connections to the loop."
    def __init__(self, loop, sockets, protocol_factory, sslcontext=None, unlink_paths=()):
        self._loop,self._sockets,self._factory,self._ssl = loop,list(sockets),protocol_factory,sslcontext
        self._unlink_paths = unlink_paths  # (path, inode) pairs: unlinked at close only if the inode still matches
        self._serving = False
        self._serving_forever_fut = None
        self._transports = weakref.WeakSet()
        self._waiters = []  # becomes None once closed with no remaining client transports

    @property
    def sockets(self): return tuple(self._sockets or ())
    def is_serving(self): return self._serving
    def get_loop(self): return self._loop
    def _attach(self, transport): self._transports.add(transport)

    def _detach(self, transport):
        self._transports.discard(transport)
        if not self._transports and self._sockets is None: self._wakeup()

    def _wakeup(self):
        waiters,self._waiters = self._waiters,None
        for w in waiters:
            if not w.done(): w.set_result(None)

    def _start_serving(self):
        if self._serving: return
        self._serving = True
        for s in self._sockets: self._loop.add_reader(s.fileno(), self._accept_ready, s)

    async def start_serving(self): self._start_serving()

    def _accept_ready(self, s):
        for _ in range(16):
            try: conn,addr = s.accept()
            except (BlockingIOError, InterruptedError): return
            except OSError: return
            conn.setblocking(False)
            self._loop.create_task(self._accept_conn(conn))

    async def _accept_conn(self, conn):
        try: await self._loop._wrap_socket(conn, self._factory, self._ssl, None, server_side=True, server=self)
        except Exception as e:
            conn.close()
            self._loop.call_exception_handler(dict(message='Error on accepted connection', exception=e))

    def close(self):
        socks,self._sockets = self._sockets,None
        if socks is None: return
        for s in socks:
            self._loop.remove_reader(s.fileno())
            s.close()
        for p,ino in self._unlink_paths:
            try:
                if os.stat(p).st_ino == ino: os.unlink(p)
            except OSError: pass
        self._unlink_paths = ()
        self._serving = False
        if self._serving_forever_fut is not None and not self._serving_forever_fut.done():
            self._serving_forever_fut.cancel()
            self._serving_forever_fut = None
        if not self._transports: self._wakeup()

    def close_clients(self):
        for t in list(self._transports): t.close()

    def abort_clients(self):
        for t in list(self._transports): t.abort()

    async def wait_closed(self):
        if self._waiters is None: return
        w = self._loop.create_future()
        self._waiters.append(w)
        await w

    async def serve_forever(self):
        if self._serving_forever_fut is not None: raise RuntimeError(f'server {self!r} is already being awaited on serve_forever()')
        if self._sockets is None: raise RuntimeError(f'server {self!r} is closed')
        self._start_serving()
        self._serving_forever_fut = self._loop.create_future()
        try: await self._serving_forever_fut
        except asyncio.CancelledError:
            try:
                self.close()
                self.close_clients()
                await self.wait_closed()
            finally: raise
        finally: self._serving_forever_fut = None

    async def __aenter__(self): return self
    async def __aexit__(self, *exc):
        self.close()
        await self.wait_closed()

class DatagramTransport(asyncio.DatagramTransport):
    "UDP transport: sendto with backpressure buffering, datagram_received/error_received dispatch."
    max_size = 65536

    def __init__(self, loop, sock, protocol, address=None, waiter=None):
        super().__init__()
        self._extra['socket'] = sock
        try: self._extra['sockname'] = sock.getsockname()
        except OSError: pass
        if address is not None: self._extra['peername'] = address
        self._loop,self._sock,self._protocol,self._address = loop,sock,protocol,address
        self._buffer = []
        self._closing = self._lost = False
        loop.call_soon(protocol.connection_made, self)
        loop.call_soon(loop.add_reader, sock.fileno(), self._read_ready)
        if waiter is not None: loop.call_soon(futures._set_result_unless_cancelled, waiter, None)

    def get_protocol(self): return self._protocol
    def set_protocol(self, protocol): self._protocol = protocol
    def is_closing(self): return self._closing

    def _read_ready(self):
        try: data,addr = self._sock.recvfrom(self.max_size)
        except (BlockingIOError, InterruptedError): return
        except OSError as e: return self._protocol.error_received(e)
        self._protocol.datagram_received(data, addr)

    def sendto(self, data, addr=None):
        if self._address is not None and addr not in (None, self._address):
            raise ValueError(f'Invalid address: must be None or {self._address}')
        if self._closing or not data: return
        if not self._buffer:
            try:
                if self._address is not None: self._sock.send(data)
                else: self._sock.sendto(data, addr)
                return
            except (BlockingIOError, InterruptedError): self._loop.add_writer(self._sock.fileno(), self._sendto_ready)
            except OSError as e: return self._protocol.error_received(e)
        self._buffer.append((bytes(data), addr))

    def _sendto_ready(self):
        while self._buffer:
            data,addr = self._buffer[0]
            try:
                if self._address is not None: self._sock.send(data)
                else: self._sock.sendto(data, addr)
            except (BlockingIOError, InterruptedError): return
            except OSError as e:
                del self._buffer[0]
                return self._protocol.error_received(e)
            del self._buffer[0]
        self._loop.remove_writer(self._sock.fileno())
        if self._closing: self._schedule_lost(None)

    def close(self):
        if self._closing: return
        self._closing = True
        self._loop.remove_reader(self._sock.fileno())
        if not self._buffer: self._schedule_lost(None)

    def abort(self):
        if self._buffer:
            self._buffer.clear()
            self._loop.remove_writer(self._sock.fileno())
        self._closing = True
        self._loop.remove_reader(self._sock.fileno())
        self._schedule_lost(None)

    def _schedule_lost(self, exc):
        if self._lost: return
        self._lost = True
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc):
        try: self._protocol.connection_lost(exc)
        finally: self._sock.close()

class ReadPipeTransport(asyncio.ReadTransport):
    "Read side of a pipe: os.read on readiness, eof_received then connection_lost at EOF."
    max_size = 262144

    def __init__(self, loop, pipe, protocol, waiter=None):
        super().__init__()
        self._extra['pipe'] = pipe
        self._loop,self._pipe,self._protocol,self._fd = loop,pipe,protocol,pipe.fileno()
        self._closing = self._reading = self._lost = False
        os.set_blocking(self._fd, False)
        loop.call_soon(protocol.connection_made, self)
        loop.call_soon(self.resume_reading)
        if waiter is not None: loop.call_soon(futures._set_result_unless_cancelled, waiter, None)

    def get_protocol(self): return self._protocol
    def set_protocol(self, protocol): self._protocol = protocol
    def is_closing(self): return self._closing
    def is_reading(self): return self._reading

    def pause_reading(self):
        if self._closing or not self._reading: return
        self._reading = False
        self._loop.remove_reader(self._fd)

    def resume_reading(self):
        if self._closing or self._reading: return
        self._reading = True
        self._loop.add_reader(self._fd, self._read_ready)

    def _read_ready(self):
        if not self._reading: return
        try: data = os.read(self._fd, self.max_size)
        except (BlockingIOError, InterruptedError): return
        except OSError as e: return self._force_close(e)
        if data: return self._protocol.data_received(data)
        self._closing = True
        self._reading = False
        self._loop.remove_reader(self._fd)
        self._loop.call_soon(self._protocol.eof_received)
        self._schedule_lost(None)

    def close(self):
        if self._closing: return
        self._closing = True
        if self._reading:
            self._reading = False
            self._loop.remove_reader(self._fd)
        self._schedule_lost(None)

    def _force_close(self, exc):
        if self._lost: return
        if self._reading:
            self._reading = False
            self._loop.remove_reader(self._fd)
        self._closing = True
        self._schedule_lost(exc)

    def _schedule_lost(self, exc):
        if self._lost: return
        self._lost = True
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc):
        try: self._protocol.connection_lost(exc)
        finally: self._pipe.close()

class WritePipeTransport(_FlowControl, asyncio.Transport):
    """Write side of a pipe: buffered os.write with the same flow control as SockTransport.

    The asyncio.Transport base supplies read-side methods raising NotImplementedError,
    matching the standard write-pipe transport's contract. A reader watches the fd where
    the platform reports peer close that way (sockets, and unnamed pipes off macOS).
    """

    def __init__(self, loop, pipe, protocol, waiter=None):
        super().__init__()
        self._extra['pipe'] = pipe
        self._loop,self._pipe,self._protocol,self._fd = loop,pipe,protocol,pipe.fileno()
        mode = os.fstat(self._fd).st_mode
        if not (stat.S_ISFIFO(mode) or stat.S_ISSOCK(mode) or stat.S_ISCHR(mode)):
            raise ValueError('Pipe transport is only for pipes, sockets and character devices')
        self._buffer = bytearray()
        self._closing = self._lost = self._has_reader = False
        os.set_blocking(self._fd, False)
        loop.call_soon(protocol.connection_made, self)
        named_fifo = sys.platform == 'darwin' and os.fstat(self._fd).st_nlink > 0
        if stat.S_ISSOCK(mode) or (stat.S_ISFIFO(mode) and not named_fifo):
            self._has_reader = True
            loop.call_soon(loop.add_reader, self._fd, self._hangup_ready)
        if waiter is not None: loop.call_soon(futures._set_result_unless_cancelled, waiter, None)

    def _hangup_ready(self): self._force_close(BrokenPipeError() if self._buffer else None)

    def get_protocol(self): return self._protocol
    def set_protocol(self, protocol): self._protocol = protocol
    def is_closing(self): return self._closing
    def write(self, data):
        if not data or self._closing or self._lost: return
        if not self._buffer:
            try: data = data[os.write(self._fd, data):]
            except (BlockingIOError, InterruptedError): pass
            except OSError as e: return self._force_close(e)
            if not data: return
            self._loop.add_writer(self._fd, self._write_ready)
        self._buffer.extend(data)
        self._maybe_pause_protocol()

    def writelines(self, list_of_data): self.write(b''.join(list_of_data))

    def _write_ready(self):
        if not self._buffer: return
        try: n = os.write(self._fd, self._buffer)
        except (BlockingIOError, InterruptedError): return
        except OSError as e: return self._force_close(e)
        del self._buffer[:n]
        self._maybe_resume_protocol()
        if self._buffer: return
        self._loop.remove_writer(self._fd)
        if self._closing: self._schedule_lost(None)

    def can_write_eof(self): return True

    def write_eof(self): self.close()

    def close(self):
        if self._closing: return
        self._closing = True
        if not self._buffer: self._schedule_lost(None)

    def abort(self): self._force_close(None)

    def _force_close(self, exc):
        if self._lost: return
        if self._buffer:
            self._buffer.clear()
            self._loop.remove_writer(self._fd)
        self._closing = True
        self._schedule_lost(exc)

    def _schedule_lost(self, exc):
        if self._lost: return
        self._lost = True
        if self._has_reader:
            self._has_reader = False
            self._loop.remove_reader(self._fd)
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc):
        try: self._protocol.connection_lost(exc)
        finally: self._pipe.close()
