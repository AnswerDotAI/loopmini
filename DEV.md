# Developer guide

## Why this exists

Kernmini's Rust engine (see kernmini `meta/ROUGH.md`, "Separate project idea: a
Tokio-backed asyncio loop" and the 2026-08-28 review) needs Python kernels to
run arbitrary user asyncio code. loopmini tests whether a Rust reactor can host
that loop with full asyncio compatibility. The compatibility bar is the full
public asyncio surface, because solveit users run arbitrary packages; the
strategy for meeting it is maximal reuse of CPython's own asyncio machinery,
with Rust owning only what Python cannot express well.

## Crate/Python split

A key goal is that Rust crates and PyO3 wrappers share code: the same reactor
must be drivable by a pure-Rust kernel (as kernmini's native crate will be).
So the crate has three layers:

- `src/reactor.rs`: `Reactor<H>`, a PyO3-free, Tokio-free core generic over
  the handle type. Ready queue, timer map, fd interest map (oneshot `polling`
  sources, re-armed on delivery), thread-safe `schedule_ts` + notify, and the
  turn phases: `next_timeout` / `poll` / `process` / `take_batch` /
  `requeue_front`. `poll` holds no locks, so a driver may block in it with the
  GIL released. The crate builds as an rlib, so a Rust consumer instantiates
  `Reactor<RustHandle>` directly.
- `src/tokio_core.rs`: the reactor hosted on a Tokio current-thread runtime.
  Tokio waits on the kqueue's own fd (a kqueue is pollable) and a zero-timeout
  drain then collects events, so the kqueue-native level/oneshot semantics
  that asyncio's `add_reader` contract needs survive Tokio's edge-triggered
  driver. Rust futures spawned on the runtime advance during every blocking
  poll, with the GIL released: the shared-reactor path for kernmini's engine.
  Zero-timeout turns skip the runtime entirely, because entering it rounds the
  wait up to the timer driver's ~1ms tick.
- `src/pyreactor.rs`: `PyReactor`, the Python-facing pyclass: the scheduling
  and readiness methods plus the canonical dispatch loop (`check_signals` each
  turn, `py.detach` around `poll`, EINTR retry, and the injected-exception
  requeue rule, which must exist exactly once). `loopmini._core` registers it
  on an owned runtime; kernmini's Python feature compiles the same struct and constructs
  it with `PyReactor::with_handle` on kernmini's runtime. Rust has no stable
  dylib ABI, so runtimes and reactors never cross extension boundaries: each
  extension compiles the crate in, and the Python-visible reactor methods are
  the only shared surface. `Loop(reactor=...)` accepts such a foreign reactor.
- `src/lib.rs`: module registration and the public re-exports (`ReactorCore`,
  `TokioCore`, `PyReactor`) for embedding crates.

Python (`python/loopmini/loop.py`) implements `asyncio.AbstractEventLoop` by
delegating scheduling to the reactor and reusing stock `Handle`, `TimerHandle`,
`Task`, `Future`, and `wrap_future`. That reuse is a design decision, not a
shortcut: it buys exact contextvars, cancellation, and introspection semantics
and removes the most version-sensitive surface. `run_forever` on the main
thread routes signals through `signal.set_wakeup_fd` into an fd the reactor
watches, matching `BaseSelectorEventLoop`.

## Implemented surface

Beyond the scheduling core: TCP transports (`transports.py`: `SockTransport`
speaking both `Protocol` and `BufferedProtocol`, with `TCP_NODELAY` set as the
standard loop does, and `MiniServer` with the accept loop),
`create_connection`/`create_server` and their Unix-socket twins,
`connect_accepted_socket`, UDP via `DatagramTransport` and
`create_datagram_endpoint`, `start_tls` (including the gh-142352
buffered-StreamReader move), TLS client and server via stock `asyncio.sslproto`
over our transports, `getaddrinfo`/`getnameinfo` through the executor, and
`add_signal_handler`/`remove_signal_handler` dispatched from the
`set_wakeup_fd` socketpair (main-thread loops only, as in the standard loop),
pipe transports (`connect_read_pipe`/`connect_write_pipe`), and
`subprocess_exec`/`subprocess_shell` (`subproc.py`: Popen spawned in the
executor since fork/exec blocks, one reaper thread per child as in asyncio's
default ThreadedChildWatcher, and pipe transports over the child's fds).

Contracts learned from the oracles, kept working by them: `remove_reader`
cancels the stored `Handle`, so an already-queued readiness callback for a
removed fd never fires (anyio's futures assume it); `connection_lost` is
scheduled exactly once however `close()`, `abort()`, and fatal errors overlap
(anyio's `aclose` does close-then-abort, and a missed schedule leaks the
socket); a cancelled connect attempt closes its socket; and a raised
`create_connection` error must not keep a referrer to the attempt-error list.
`MiniServer` mirrors 3.13 `Server` exactly (test_server pins it):
`wait_closed()` returns only once the server is closed *and* the last client
transport is gone, `close_clients()`/`abort_clients()` act on a `WeakSet` of
attached transports, a cancelled `serve_forever` closes the server and its
clients before re-raising, and a Unix socket path is unlinked at close only
when its inode still matches the one bound (`cleanup_socket=False` disables).
A cancelled timer leaves the reactor's timer map at once: `schedule_at`
returns a `(deadline µs, seq)` key, the `TimerHandle` subclass carries it, and
`_timer_handle_cancelled` removes the entry. Retention until the original
deadline would grow memory for an hour per cancelled `wait_for(..., 3600)`.
From the aiohttp suite: `loop.time()` must be anchored to `time.monotonic`
(connectors compare the two directly), so the reactor clock carries a
constant offset captured at loop creation; and a transport must not hold an
explicit `memoryview` export of its write buffer across a `send` that can
raise, because the raised exception's traceback keeps the export alive and
the buffer can then never be resized (send the bytearray itself, as the
standard loop does).

## Deliberately not implemented yet

Sendfile and Windows.
`AbstractEventLoop` raises `NotImplementedError` for these, which is the
honest signal while the compatibility ladder is climbed. Next rungs and their
oracles (anyio/aiohttp/httpx/websockets test suites) are listed in kernmini
`meta/ROUGH.md`.

## Tests

The tiers, so the inner loop stays seconds:

- `pytest -q` per change (~2s).
- `pytest -m oracle -n auto` when a feature lands, not per edit.
- `pytest -m bench -s` and `pytest -m soak -s` only when performance or
  stability is the question.
- `chkstyle` once, at the final PR stage, never per edit.

`pytest -q`. Ten integration stories, deliberately few: scheduling/tasks/
threads (timers, contextvars, gather, TaskGroup, timeout, cancellation,
cross-thread wakeup, to_thread), socket I/O through the reactor (accept/
connect/backpressure on a 5MB payload), asyncio streams echo with drain
backpressure, a background task surviving between `run_until_complete` calls
(the kernel-persistence story), KeyboardInterrupt injection with the loop
reused afterwards, uvicorn serving an ASGI app fetched by urllib and by
httpx-over-anyio, TLS echo against a throwaway openssl cert, a subprocess
round-trip (exec and shell, streams and communicate), and the
interrupt-torture story: window-scoped `PyThreadState_SetAsyncExc` injection
under stream/timer/cross-thread load, mirroring kernmini's
`sync_execution_context` contract (0.5s by default; `LOOPMINI_TORTURE_SECONDS`
extends it), and the KI-at-`_run`-entry orphan repro that pins the
traceback-depth requeue rule. Rust-side unit tests should exist only for
reactor invariants Python stories cannot reach.

Two CPython 3.13 facts the torture test depends on, discovered the hard way:
an async-injected exception raised at an eval-breaker check escapes the
raising frame even past a same-frame `try/except` (open regression
[gh-139622](https://github.com/python/cpython/issues/139622); 3.12 and 3.14
unaffected), so the guard must sit in a parent frame; and a busy window
shorter than the GIL switch interval (5ms) is invisible to a sampling thread,
because the GIL is only ever released while the window is closed.

## The compatibility oracles

Four external suites are the conformance measure, run in-repo through
`tests/test_oracle.py` (marked `oracle`, deselected by default):

```bash
pytest -m oracle -n auto          # all four suites, one worker per module
pytest -m oracle -k uvloop        # one suite; add "and test_tcp" etc. for one module
```

`tests/oracle_util.py` fetches and caches what each suite needs under
`~/.cache/loopmini-oracle` (override with `LOOPMINI_ORACLE_CACHE`):

- CPython's own test_asyncio (uvloop's strategy). This uv-managed Python ships
  without the stdlib `test` package, so the matching source tarball is fetched
  and its functional modules run under a loopmini event-loop policy. Suites
  asserting standard-loop internals (base_events, selector_events,
  unix_events) are not fair oracles and are excluded.
- anyio's test suite, via its sanctioned alternative-loop mechanism: the sdist
  matching the installed version is unpacked and a loopmini entry added to
  `asyncio_params` in its conftest. Runs as a subprocess; requires the anyio
  test deps (`trustme`, `blockbuster`) in the venv. blockbuster's
  blocking-call detector allowlists Popen's blocking os.read only under
  `asyncio/base_events.py`, which is one reason the Popen spawn goes through
  the executor.
- uvloop's test suite, from a source clone (`LOOPMINI_UVLOOP_REPO`, default
  `~/aai-ws/links/uvloop`): its loop-parameterized test classes are cloned
  onto loopmini with `implementation='asyncio'`, so branchy tests take the
  standard-loop expectation paths. `test_tcp` needs pyOpenSSL and is skipped
  without it.
- aiohttp's test suite: the sdist is unpacked, uvloop is stubbed to loopmini in
  its conftest so `--aiohttp-loop=uvloop` selects it, and its blockbuster
  fixture gains an allowlist entry for `MiniServer.close` (the same
  unlink-if-unchanged stat asyncio's `_stop_serving` is allowlisted for).
  Runs the pure-Python aiohttp (`AIOHTTP_NO_EXTENSIONS=1`), with a private
  `--basetemp` because its permission tests leave chmod-000 dirs that
  pytest's numbered-dir sweeper cannot remove, fatal under aiohttp's
  `filterwarnings = error`. Deselected, with reasons in `test_oracle.py`:
  tests that fail identically on the standard loop in this venv, and one
  test that mock-patches `loop.time()`, which only steers loops whose timer
  arithmetic reads Python-level time each turn (real uvloop fails it too).

Status on 2026-08-28: all green; 25 oracle tests, the first three suites in
~17s under xdist and aiohttp's ~4,200 tests in a further ~2 minutes.

## The soak gate

`pytest -m soak -s` runs a kernel-shaped workload on one loopmini loop for
`LOOPMINI_SOAK_SECONDS` (default 30): a fasthtml app on uvicorn under
threaded httpx load, a websocket echo server with a bot-style client, a
housekeeping tick, and periodic cells submitted from another thread, as a
kernel would. It asserts zero errors, bounded fd growth, and that every
component made progress. It watches stability, not speed.

## Benchmarks

`pytest -m bench -s` prints loopmini vs the standard loop on loop-bound
microbenchmarks; informational, never gating. Numbers on 2026-08-28 (M-series
macOS): call_soon 1.10x, sleep0 1.12x, tcp_echo 1.10x, task spawn 2.48x
(stdlib=1.0). The spawn gap is per-task PyO3 boundary crossings (two to three
`schedule` calls per task lifecycle at ~0.5µs each); loop startup is at parity.
Closing it would need handles represented Rust-side, which is later-rung work.

## Style and releases

fastai style (`chkstyle` before committing). Releases via fastship; the tree
carries the next version.
