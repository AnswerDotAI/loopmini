"A nonblocking process spawn feeding asyncio's standard subprocess transport."
import threading
from asyncio import base_subprocess

__all__ = ["SubprocessTransport"]

class SubprocessTransport(base_subprocess.BaseSubprocessTransport):
    def __init__(self, loop, protocol, args, shell, stdin, stdout, stderr, bufsize, proc, waiter=None, extra=None, **kwargs):
        self._prepared_proc = proc
        super().__init__(loop, protocol, args, shell, stdin, stdout, stderr, bufsize, waiter, extra, **kwargs)
        threading.Thread(target=self._reap, daemon=True).start()

    def _start(self, **kwargs):
        self._proc = self._prepared_proc
        del self._prepared_proc

    def _reap(self):
        returncode = self._proc.wait()
        try: self._loop.call_soon_threadsafe(self._process_exited, returncode)
        except RuntimeError: pass
