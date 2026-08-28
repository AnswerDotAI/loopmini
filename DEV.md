# Developer guide

## Why this exists

Kernmini's Rust engine (see kernmini `meta/ROUGH.md`, "Separate project idea: a Rust-backed asyncio loop" and the 2026-08-28 review) needs Python kernels to run arbitrary user asyncio code. loopmini tests whether a Rust reactor can host that loop with full asyncio compatibility. The compatibility bar is the full public asyncio surface, because solveit users run arbitrary packages; the strategy is maximal reuse of CPython's own asyncio machinery, with Rust owning only what Python cannot express well.

## Crate/Python split

A key goal is that Rust crates and PyO3 wrappers share code: the same reactor must be drivable by a pure-Rust kernel (as kernmini's native crate will be). The implementation has two layers plus module registration:

- `src/reactor.rs`: `Reactor<H>`, a PyO3-free, Tokio-free core generic over the handle type. It owns the ready queue, timer map, fd interest map (oneshot `polling` sources, re-armed on delivery), thread-safe `schedule_ts` + notify, and the turn phases: `next_timeout` / `poll` / `process` / `take_batch` / `requeue_front`. `poll` holds no locks, so a driver may block in it with the GIL released. The crate builds as an rlib, so a Rust consumer instantiates `Reactor<RustHandle>` directly.
- `src/pyreactor.rs`: `PyReactor`, the Python-facing pyclass: the scheduling and readiness methods plus the canonical dispatch loop (`check_signals` each turn, `py.detach` around `poll`, EINTR retry, and the injected-exception requeue rule, which must exist exactly once). The driver blocks directly in the core poller with the GIL released. Rust runtimes keep their futures on worker threads and wake the loop with its thread-safe scheduling path.
- `src/lib.rs`: module registration and the public re-exports (`ReactorCore`, `PyReactor`) for embedding crates.

Python (`python/loopmini/loop.py`) subclasses `asyncio.BaseEventLoop`, delegating scheduling to the reactor while inheriting task/future creation, executors, exception handling, high-level networking, TLS orchestration, servers, subprocess entry points, and buffered sendfile. Reactor-neutral implementations of socket operations and accepting connections are borrowed from `BaseSelectorEventLoop`; Unix connection/server methods and pipe transports are borrowed from `_UnixSelectorEventLoop`. Loopmini supplies the hooks those implementations call. This reuse buys exact CPython contextvars, cancellation, introspection, lifecycle, and version-specific semantics. `run_forever` on the main thread routes signals through `signal.set_wakeup_fd` into an fd watched by the reactor.

## Implemented surface

Beyond the scheduling core, loopmini implements TCP transports (`SockTransport`, speaking both `Protocol` and `BufferedProtocol`, with `TCP_NODELAY`), UDP (`DatagramTransport`), signal dispatch through the `set_wakeup_fd` socketpair, and the reactor's reader/writer registrations. CPython supplies the high-level TCP/UDP/TLS/Unix/server operations, Unix pipe transports, flow control, and buffered sendfile. `subproc.py` prepares `Popen` in the executor because fork/exec blocks, then gives it to CPython's `BaseSubprocessTransport`; one reaper thread per child reports exit through the loop's thread-safe scheduling path.

Contracts learned from the oracles, kept working by them: `remove_reader` cancels the stored `Handle`, so an already-queued readiness callback for a removed fd never fires; `connection_lost` is scheduled exactly once however `close()`, `abort()`, and fatal errors overlap; and a Unix socket path is unlinked at close only when its inode still matches the one bound. A cancelled timer leaves the reactor's timer map at once: `schedule_at` returns a `(deadline µs, seq)` key, the `TimerHandle` subclass carries it, and `_timer_handle_cancelled` removes the entry. Retention until the original deadline would grow memory for an hour per cancelled `wait_for(..., 3600)`. The reactor clock carries a constant offset captured at loop creation because aiohttp compares `loop.time()` with `time.monotonic`. A socket transport sends its bytearray directly rather than holding an explicit `memoryview` export across a potentially raising `send`, whose traceback could otherwise keep the buffer unresizable.

## Deliberately not implemented yet

Native sendfile and Windows. `BaseEventLoop` provides the portable buffered sendfile fallback; the socket transport declares that capability rather than pretending to support the platform-native fast path.

## Tests

The tiers, so the inner loop stays seconds:

- `pytest -q` per change (~2s).
- `pytest -m oracle -n auto` when a feature lands, not per edit.
- `pytest -m bench -s` and `pytest -m soak -s` only when performance or stability is the question.
- `chkstyle` once, at the final PR stage, never per edit.

To test against another Python version locally (the workspace venv is pinned, so
plain `uv run --python` refuses): `uv run --no-project --python 3.14 --with
'.[dev]' pytest -q` from the repo root. uv builds against its managed
interpreter into its cache, so repeat runs are fast and there is no venv to
maintain. CI runs the same suite on every supported version.

`pytest -q`. Eleven integration stories, deliberately few: scheduling/tasks/threads (timers, contextvars, gather, TaskGroup, timeout, cancellation, cross-thread wakeup, to_thread), socket I/O through the reactor (accept/connect/backpressure on a 5MB payload), asyncio streams echo with drain backpressure, a background task surviving between `run_until_complete` calls (the kernel-persistence story), KeyboardInterrupt injection with the loop reused afterwards, uvicorn serving an ASGI app fetched by urllib and by httpx-over-anyio, TLS echo against a throwaway openssl cert, a subprocess round-trip (exec and shell, streams and communicate), and the interrupt-torture story: window-scoped `PyThreadState_SetAsyncExc` injection under stream/timer/cross-thread load, mirroring kernmini's `sync_execution_context` contract (0.5s by default; `LOOPMINI_TORTURE_SECONDS` extends it), and the KI-at-`_run`-entry orphan repro that pins the traceback-depth requeue rule. Rust-side unit tests should exist only for reactor invariants Python stories cannot reach.

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

`tests/oracle_util.py` fetches and caches what each suite needs under `~/.cache/loopmini-oracle` (override with `LOOPMINI_ORACLE_CACHE`). Modified external source trees are content-addressed by the source of their adapter function, so changing an adapter creates a fresh fixture while unchanged fixtures retain their cache:

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
- aiohttp's test suite: the sdist is unpacked, uvloop is stubbed to loopmini in its conftest so `--aiohttp-loop=uvloop` selects it, and its blockbuster fixture gains an allowlist entry for the same unlink-if-unchanged stat used by asyncio's Unix loop. It runs pure-Python aiohttp (`AIOHTTP_NO_EXTENSIONS=1`) with a private `--basetemp`, because permission tests leave chmod-000 directories that pytest's numbered-directory sweeper cannot remove under aiohttp's `filterwarnings = error`. Deselections and their reasons live in `test_oracle.py`.

Status on 2026-08-28: all green; 26 oracle stories, including about 4,200 aiohttp tests, complete in about 26 seconds under xdist on the development Mac.

## The soak gate

`pytest -m soak -s` runs a kernel-shaped workload on one loopmini loop for
`LOOPMINI_SOAK_SECONDS` (default 30): a fasthtml app on uvicorn under
threaded httpx load, a websocket echo server with a bot-style client, a
housekeeping tick, and periodic cells submitted from another thread, as a
kernel would. It asserts zero errors, bounded fd growth, and that every
component made progress. It watches stability, not speed.

## Benchmarks

`pytest -m bench -s` prints loopmini vs the standard loop on loop-bound microbenchmarks; informational, never gating. Numbers on 2026-08-28 (M-series macOS): call_soon 1.04x, sleep0 1.03x, tcp_echo 1.02x, task spawn 1.17x (stdlib=1.0); the corresponding extra cost is approximately 0.5µs per scheduled callback, 0.5µs per `sleep(0)` suspend/resume, 1.1µs per TCP echo, and 0.3µs per spawned task. Median 1ms-timer overshoot is 0.188ms versus 0.156ms.

## Style and releases

fastai style (`chkstyle` before committing). Releases via fastship; the tree
carries the next version.
