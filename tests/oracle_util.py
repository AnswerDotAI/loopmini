"Fetch, cache, and adapt external event-loop conformance suites (CPython, anyio, uvloop)."
import asyncio, contextlib, fcntl, hashlib, importlib.metadata, importlib.util, inspect, os, platform, shutil, sys, tarfile, types, unittest, urllib.request
from pathlib import Path
import loopmini

CACHE = Path(os.environ.get('LOOPMINI_ORACLE_CACHE', Path.home()/'.cache'/'loopmini-oracle'))

class LoopminiPolicy(asyncio.DefaultEventLoopPolicy):
    def new_event_loop(self): return loopmini.new_event_loop()

def _fetch_tar(url, dest_dir):
    dest_dir.mkdir(parents=True, exist_ok=True)
    tar_path = dest_dir/'src.tgz'
    urllib.request.urlretrieve(url, tar_path)
    with tarfile.open(tar_path) as tf: tf.extractall(dest_dir, filter='data')
    tar_path.unlink()

@contextlib.contextmanager
def _cache_lock(path):
    "Serialize the first fetch/adaptation of one cache entry across xdist workers."
    path.mkdir(parents=True, exist_ok=True)
    with open(path/'.lock', 'w') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield

def rmtree_force(p):
    "Remove tree `p` even where permission-test leftovers are chmod-000."
    os.chmod(p, 0o700)
    # chmod children from the parent: os.walk never yields an unreadable dir itself
    for dirpath,dirnames,_ in os.walk(p):
        for d in dirnames: os.chmod(os.path.join(dirpath, d), 0o700)
    shutil.rmtree(p)

def cpython_test_dir():
    "The Lib/ directory of the CPython source matching this interpreter, holding the `test` package."
    ver = platform.python_version()
    cache = CACHE/f'cpython-{ver}'
    lib = cache/f'Python-{ver}'/'Lib'
    if not (lib/'test').exists():
        with _cache_lock(cache):
            if not (lib/'test').exists(): _fetch_tar(f'https://www.python.org/ftp/python/{ver}/Python-{ver}.tgz', cache)
    return lib

def _adapted_sdist(name, ver, adapt):
    "Fetch an sdist into a cache keyed by the source of its local adapter."
    key = hashlib.sha256(inspect.getsource(adapt).encode()).hexdigest()[:12]
    cache = CACHE/f'{name}-{ver}-{key}'
    root,ready = cache/f'{name}-{ver}',cache/'.ready'
    if not ready.exists():
        with _cache_lock(cache):
            if not ready.exists():
                _fetch_tar(f'https://files.pythonhosted.org/packages/source/{name[0]}/{name}/{name}-{ver}.tar.gz', cache)
                adapt(root)
                ready.touch()
    return root

def _adapt_anyio(root):
    conftest = root/'tests'/'conftest.py'
    src = conftest.read_text()
    anchor = 'backend_params = asyncio_params.copy()'
    assert anchor in src, 'anyio conftest layout changed; adjust oracle_util.anyio_test_dir'
    param = '''import loopmini

asyncio_params.append(
    pytest.param(
        ("asyncio", {"debug": True, "loop_factory": loopmini.new_event_loop}),
        id="asyncio+loopmini",
    ),
)

'''
    conftest.write_text(src.replace(anchor, param + anchor))

def anyio_test_dir():
    "The sdist of the installed anyio version, with a loopmini entry added to its backend params."
    ver = importlib.metadata.version('anyio')
    return _adapted_sdist('anyio', ver, _adapt_anyio)

def _adapt_aiohttp(root):
    conftest = root/'tests'/'conftest.py'
    src = conftest.read_text()
    anchor = 'try:\n    if sys.platform == "win32":'
    assert anchor in src, 'aiohttp conftest layout changed; adjust oracle_util.aiohttp_test_dir'
    stub = """import sys, types, loopmini
uvloop = types.ModuleType("uvloop")
uvloop.new_event_loop = loopmini.new_event_loop
sys.modules["uvloop"] = uvloop
"""
    src = src.replace(anchor, stub + anchor)
    anchor = 'bb.functions["threading.Lock.acquire"].deactivate()'
    assert anchor in src, 'aiohttp blockbuster fixture changed; adjust oracle_util.aiohttp_test_dir'
    patch = '''# loopmini does the same unlink-if-unchanged close as asyncio's unix loop
        for func in ("os.stat", "os.unlink"):
            bb.functions[func].can_block_in("loopmini/loop.py", "_stop_serving")
        '''
    conftest.write_text(src.replace(anchor, patch + anchor))
    cfg = root/'setup.cfg'
    cfg.write_text('\n'.join(l for l in cfg.read_text().splitlines() if 'CoverageWarning' not in l) + '\n')

def aiohttp_test_dir():
    "The sdist of the installed aiohttp version, with uvloop stubbed to loopmini so `--aiohttp-loop=uvloop` selects it."
    ver = importlib.metadata.version('aiohttp')
    return _adapted_sdist('aiohttp', ver, _adapt_aiohttp)

def uvloop_repo():
    "A uvloop source clone (its test suite is loop-parameterized by design); None if not present."
    p = Path(os.environ.get('LOOPMINI_UVLOOP_REPO', Path.home()/'aai-ws'/'links'/'uvloop'))
    return p if (p/'tests').is_dir() else None

def _uvloop_stub(repo):
    "A stand-in uvloop package: `_testbase` imports from the real clone, no built extension needed."
    stub = sys.modules.get('uvloop')
    if stub is None or not getattr(stub, '_loopmini_stub', False):
        stub = types.ModuleType('uvloop')
        stub._loopmini_stub = True
        stub.__path__ = [str(repo/'uvloop')]
        stub.new_event_loop = loopmini.new_event_loop
        stub.EventLoopPolicy = LoopminiPolicy
        sys.modules['uvloop'] = stub
    return stub

def uvloop_cases(repo, module_name):
    "Import one uvloop test module and clone its asyncio-loop test classes to run on loopmini."
    _uvloop_stub(repo)
    from uvloop import _testbase as tb
    spec = importlib.util.spec_from_file_location(f'uvloop_oracle.{module_name}', repo/'tests'/f'{module_name}.py')
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    # implementation='asyncio' and is_asyncio_loop=True: branchy tests must take the
    # standard-loop expectation paths, which are the contract loopmini implements
    ns = dict(implementation='asyncio', is_asyncio_loop=lambda self: True,
        new_loop=lambda self: loopmini.new_event_loop(), new_policy=lambda self: LoopminiPolicy())
    return [type(name.replace('AIO', 'LM'), (cls,), dict(ns)) for name, cls in vars(mod).copy().items()
        if isinstance(cls, type) and issubclass(cls, tb.AIOTestCase) and name.startswith('Test')]

def run_cases(cases, exclude=frozenset()):
    "Run unittest cases/classes (minus excluded method names); output goes to stderr for pytest to show on failure."
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    for c in cases:
        for t in loader.loadTestsFromTestCase(c) if isinstance(c, type) else loader.loadTestsFromName(c):
            for case in t if isinstance(t, unittest.TestSuite) else [t]:
                if case._testMethodName not in exclude: suite.addTest(case)
    return unittest.TextTestRunner(stream=sys.stderr, verbosity=1).run(suite)
