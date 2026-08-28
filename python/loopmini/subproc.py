"Subprocess transport: Popen plus pipe transports, with a reaper thread per child."
import asyncio, collections, functools, subprocess, threading

__all__ = ["SubprocessTransport"]

class _PipeReadProto(asyncio.Protocol):
    def __init__(self, subtr, fd): self.subtr,self.fd,self.pipe = subtr,fd,None
    def connection_made(self, transport): self.pipe = transport
    def data_received(self, data): self.subtr._call(self.subtr._protocol.pipe_data_received, self.fd, data)
    def connection_lost(self, exc): self.subtr._pipe_connection_lost(self.fd, exc)

class _PipeWriteProto(asyncio.BaseProtocol):
    def __init__(self, subtr, fd): self.subtr,self.fd,self.pipe = subtr,fd,None
    def connection_made(self, transport): self.pipe = transport
    def connection_lost(self, exc): self.subtr._pipe_connection_lost(self.fd, exc)
    def pause_writing(self): self.subtr._protocol.pause_writing()
    def resume_writing(self): self.subtr._protocol.resume_writing()

class SubprocessTransport(asyncio.SubprocessTransport):
    def __init__(self, loop, protocol, args, shell, stdin, stdout, stderr, bufsize, **kwargs):
        super().__init__()
        self._loop,self._protocol = loop,protocol
        self._closed = self._finished = False
        # Buffers protocol calls until connection_made wires the protocol: a fast child
        # can produce output (even exit) while later pipes are still being connected
        self._pending_calls = collections.deque()
        self._returncode = None
        self._exit_waiters = []
        self._pipes = {}
        self._disconnected = set()
        self._popen = functools.partial(subprocess.Popen, args, shell=shell, stdin=stdin, stdout=stdout,
            stderr=stderr, bufsize=bufsize, **kwargs)

    async def _setup(self):
        # In the executor because fork/exec blocks (the standard loop blocks its loop thread here)
        proc = self._proc = await self._loop.run_in_executor(None, self._popen)
        self._extra['subprocess'] = proc
        del self._popen
        if proc.stdin is not None:
            tr,_ = await self._loop.connect_write_pipe(lambda: _PipeWriteProto(self, 0), proc.stdin)
            self._pipes[0] = tr
        if proc.stdout is not None:
            tr,_ = await self._loop.connect_read_pipe(lambda: _PipeReadProto(self, 1), proc.stdout)
            self._pipes[1] = tr
        if proc.stderr is not None:
            tr,_ = await self._loop.connect_read_pipe(lambda: _PipeReadProto(self, 2), proc.stderr)
            self._pipes[2] = tr
        # Called directly so the protocol is fully wired before subprocess_exec returns
        self._protocol.connection_made(self)
        for cb,data in self._pending_calls: self._loop.call_soon(cb, *data)
        self._pending_calls = None
        # Same model as asyncio's default ThreadedChildWatcher: one waiting thread per child
        threading.Thread(target=self._reap, daemon=True).start()

    def _reap(self):
        returncode = self._proc.wait()
        try: self._loop.call_soon_threadsafe(self._process_exited, returncode)
        except RuntimeError: pass  # loop closed before the child exited

    def _call(self, cb, *data):
        if self._pending_calls is not None: self._pending_calls.append((cb, data))
        else: cb(*data)

    def _process_exited(self, returncode):
        self._returncode = returncode
        self._call(self._protocol.process_exited)
        for w in self._exit_waiters:
            if not w.cancelled(): w.set_result(returncode)
        self._exit_waiters = []
        self._try_finish()

    def _pipe_connection_lost(self, fd, exc):
        self._call(self._protocol.pipe_connection_lost, fd, exc)
        self._disconnected.add(fd)
        self._try_finish()

    def _try_finish(self):
        # The subprocess protocol's connection_lost fires once: process exited, all pipes gone
        if self._finished or self._returncode is None or self._disconnected < set(self._pipes): return
        self._finished = True
        self._loop.call_soon(self._protocol.connection_lost, None)

    async def _wait(self):
        if self._returncode is not None: return self._returncode
        fut = self._loop.create_future()
        self._exit_waiters.append(fut)
        return await fut

    def get_pid(self): return self._proc.pid
    def get_returncode(self): return self._returncode
    def get_pipe_transport(self, fd): return self._pipes.get(fd)
    def is_closing(self): return self._closed

    def _check_running(self):
        if self._returncode is not None: raise ProcessLookupError()

    def send_signal(self, signal):
        self._check_running()
        self._proc.send_signal(signal)

    def terminate(self):
        self._check_running()
        self._proc.terminate()

    def kill(self):
        self._check_running()
        self._proc.kill()

    def close(self):
        if self._closed: return
        self._closed = True
        for tr in self._pipes.values():
            if tr is not None: tr.close()
        if self._returncode is None and self._proc.poll() is None: self._proc.kill()
