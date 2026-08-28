"""Conformance oracles: external suites run against loopmini.

Deselected by default; run with `pytest -m oracle` (add -k to pick one suite).
Each suite must pass everything the standard loop passes; a deliberate gap
must be excluded here with a comment, never left as an expected failure.
"""
import asyncio, importlib.util, os, subprocess, sys
import pytest, oracle_util

pytestmark = pytest.mark.oracle  # per-test timeout comes from the ini default (60s)

CPYTHON_MODULES = ['futures', 'tasks', 'timeouts', 'taskgroups', 'locks', 'queues', 'sock_lowlevel', 'server', 'streams', 'subprocess']

# Tests asserting standard-loop internals loopmini deliberately lacks, with the reason
CPYTHON_EXCLUDE = {'test_create_subprocess_fails_with_inactive_watcher'}  # child watchers: 3.13-deprecated, removed in 3.14

@pytest.mark.parametrize('module', CPYTHON_MODULES)
def test_cpython_asyncio(module):
    lib = oracle_util.cpython_test_dir()
    sys.path.insert(0, str(lib))
    asyncio.set_event_loop_policy(oracle_util.LoopminiPolicy())
    try: res = oracle_util.run_cases([f'test.test_asyncio.test_{module}'], exclude=CPYTHON_EXCLUDE)
    finally:
        asyncio.set_event_loop_policy(None)
        sys.path.remove(str(lib))
    assert not res.failures and not res.errors

def test_anyio():
    pytest.importorskip('trustme')
    pytest.importorskip('blockbuster')
    root = oracle_util.anyio_test_dir()
    cmd = [sys.executable, '-m', 'pytest', 'tests', '-q', '-p', 'no:cacheprovider', '--timeout', '120', '-k', 'loopmini']
    assert subprocess.run(cmd, cwd=root).returncode == 0

# Fail identically on the standard loop in this venv (missing C extensions,
# pytester machinery vs newer pytest), so they measure the environment, not the loop
AIOHTTP_ENV_DESELECT = ['tests/test_circular_imports.py', 'tests/test_imports.py', 'tests/test_pytest_plugin.py',
    'tests/test_test_utils.py::test_testcase_no_app', 'tests/test_client_session.py::test_build_url_returns_expected_url']

@pytest.mark.timeout(600)
def test_aiohttp():
    root = oracle_util.aiohttp_test_dir()
    # test_keepalive_expires_on_time mock-patches loop.time(), which only steers loops whose
    # timer arithmetic consults Python-level time() each turn; real uvloop fails it too
    deselect = AIOHTTP_ENV_DESELECT + ['tests/test_web_functional.py::test_keepalive_expires_on_time']
    # A private basetemp sidesteps pytest's numbered-dir sweeper: aiohttp's permission
    # tests leave chmod-000 dirs it cannot remove, fatal under `filterwarnings = error`
    basetemp = root/'basetemp'
    if basetemp.exists(): oracle_util.rmtree_force(basetemp)
    cmd = [sys.executable, '-m', 'pytest', 'tests', '-q', '-p', 'no:cacheprovider', '--timeout', '120',
        '--aiohttp-loop=uvloop', '-n', 'auto', '--tb=line', f'--basetemp={basetemp}'] + [f'--deselect={d}' for d in deselect]
    env = dict(os.environ, AIOHTTP_NO_EXTENSIONS='1')
    assert subprocess.run(cmd, cwd=root, env=env).returncode == 0

UVLOOP_MODULES = ['test_base', 'test_udp', 'test_unix', 'test_pipes', 'test_process', 'test_process_spawning',
    'test_signals', 'test_sockets', 'test_dns', 'test_executors', 'test_context', 'test_runner', 'test_regr1']
if importlib.util.find_spec('OpenSSL'): UVLOOP_MODULES.append('test_tcp')  # imports pyOpenSSL

@pytest.mark.parametrize('module', UVLOOP_MODULES)
def test_uvloop(module, capfd):
    repo = oracle_util.uvloop_repo()
    if repo is None: pytest.skip('no uvloop clone (set LOOPMINI_UVLOOP_REPO)')
    # Capture off: uvloop's fd-redirection tests assume sys.stdout.fileno() is the real fd 1
    with capfd.disabled(): res = oracle_util.run_cases(oracle_util.uvloop_cases(repo, module))
    assert not res.failures and not res.errors
