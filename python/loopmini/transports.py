"Socket transports for the Rust-reactor loop, feeding stock asyncio protocols."
import asyncio, socket, sys
_server_takes_transport = sys.version_info >= (3, 13)  # Server._attach/_detach gained the transport arg in 3.13 (gh-113538)
from asyncio import constants, futures, transports

class _TransportLifecycle:
    "Protocol binding and exactly-once connection_lost delivery shared by fd transports."
    def get_protocol(self): return self._protocol
    def set_protocol(self, protocol): self._protocol = protocol
    def is_closing(self): return self._closing

    def _schedule_lost(self, exc):
        if self._lost: return
        self._lost = True
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc):
        try: self._protocol.connection_lost(exc)
        finally: self._close_resource()

__all__ = ["SockTransport", "DatagramTransport"]

class SockTransport(_TransportLifecycle, transports._FlowControlMixin):
    "Bidirectional TCP transport over the loop's fd-readiness primitives."
    max_size = 262144
    _start_tls_compatible = True
    _sendfile_compatible = constants._SendfileMode.FALLBACK

    def __init__(self, loop, sock, protocol, waiter=None, extra=None, server=None, context=None):
        super().__init__(extra, loop)
        self._extra['socket'] = sock
        try: self._extra['sockname'] = sock.getsockname()
        except OSError: pass
        try: self._extra['peername'] = sock.getpeername()
        except OSError: pass
        self._loop,self._sock,self._protocol,self._server,self._fd = loop,sock,protocol,server,sock.fileno()
        loop._transports[self._fd] = self
        self._buffered = isinstance(protocol, asyncio.BufferedProtocol)
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            try: sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError: pass
        self._buffer = bytearray()
        self._closing = self._reading = self._eof = self._lost = False
        if server is not None: server._attach(self) if _server_takes_transport else server._attach()
        loop.call_soon(protocol.connection_made, self, context=context)
        loop.call_soon(self.resume_reading, context=context)
        if waiter is not None: loop.call_soon(futures._set_result_unless_cancelled, waiter, None)

    def set_protocol(self, protocol): self._protocol,self._buffered = protocol,isinstance(protocol, asyncio.BufferedProtocol)

    def get_write_buffer_size(self): return len(self._buffer)

    def is_reading(self): return self._reading

    def _stop_reading(self):
        if not self._reading: return
        self._reading = False
        self._loop.remove_reader(self._fd)

    def pause_reading(self):
        if not self._closing: self._stop_reading()

    def resume_reading(self):
        if self._closing or self._reading: return
        self._reading = True
        self._loop.add_reader(self._fd, self._read_ready)

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
            self._loop.remove_reader(self._fd)
        else: self.close()

    def write(self, data):
        if not data or self._closing or self._lost: return
        if not self._buffer:
            try: data = data[self._sock.send(data):]
            except (BlockingIOError, InterruptedError): pass
            except OSError as e: return self._fatal_error(e, 'Fatal write error on socket transport')
            if not data: return
            self._loop.add_writer(self._fd, self._write_ready)
        self._buffer.extend(data)
        self._maybe_pause_protocol()

    def writelines(self, list_of_data): self.write(b''.join(list_of_data))

    def _write_ready(self):
        if not self._buffer: return
        try: n = self._sock.send(self._buffer)
        except (BlockingIOError, InterruptedError): return
        except OSError as e: return self._fatal_error(e, 'Fatal write error on socket transport')
        del self._buffer[:n]
        self._maybe_resume_protocol()
        if self._buffer: return
        self._loop.remove_writer(self._fd)
        if self._eof: self._sock.shutdown(socket.SHUT_WR)
        if self._closing: self._schedule_lost(None)

    def can_write_eof(self): return True

    def write_eof(self):
        if self._closing or self._eof: return
        self._eof = True
        if not self._buffer: self._sock.shutdown(socket.SHUT_WR)

    def close(self):
        if self._closing: return
        self._closing = True
        self._stop_reading()
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
            self._loop.remove_writer(self._fd)
        self._stop_reading()
        self._closing = True
        self._schedule_lost(exc)

    def _close_resource(self):
        self._sock.close()
        if self._server is not None:
            self._server._detach(self) if _server_takes_transport else self._server._detach()
            self._server = None

class DatagramTransport(_TransportLifecycle, asyncio.DatagramTransport):
    "UDP transport: sendto with backpressure buffering, datagram_received/error_received dispatch."
    max_size = 65536

    def __init__(self, loop, sock, protocol, address=None, waiter=None, extra=None):
        super().__init__(extra)
        self._extra['socket'] = sock
        try: self._extra['sockname'] = sock.getsockname()
        except OSError: pass
        if address is not None: self._extra['peername'] = address
        self._loop,self._sock,self._protocol,self._address = loop,sock,protocol,address
        loop._transports[sock.fileno()] = self
        self._buffer = []
        self._closing = self._lost = False
        loop.call_soon(protocol.connection_made, self)
        loop.call_soon(loop.add_reader, sock.fileno(), self._read_ready)
        if waiter is not None: loop.call_soon(futures._set_result_unless_cancelled, waiter, None)

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

    def _close_resource(self): self._sock.close()
