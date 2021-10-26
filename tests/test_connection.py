from unittest import mock
import types
import pytest

from redis.exceptions import InvalidResponse, ModuleError
from redis.utils import HIREDIS_AVAILABLE
from .conftest import skip_if_server_version_lt


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason='PythonParser only')
def test_invalid_response(r):
    raw = b'x'
    parser = r.connection._parser
    with mock.patch.object(parser._buffer, 'readline', return_value=raw):
        with pytest.raises(InvalidResponse) as cm:
            parser.read_response()
    assert str(cm.value) == 'Protocol Error: %r' % raw


@skip_if_server_version_lt('4.0.0')
def test_loaded_modules(r, modclient):
    assert r.loaded_modules == []
    assert 'rejson' in modclient.loaded_modules.keys()


@skip_if_server_version_lt('4.0.0')
def test_loading_external_modules(r, modclient):
    def inner():
        pass

    with pytest.raises(ModuleError):
        r.load_external_module('rejson', 'myfuncname', inner)

    modclient.load_external_module('rejson', 'myfuncname', inner)
    assert getattr(modclient, 'myfuncname') == inner
    assert isinstance(getattr(modclient, 'myfuncname'), types.FunctionType)

    # and call it
    from redis.commands import RedisModuleCommands
    j = RedisModuleCommands.json
    modclient.load_external_module('rejson', 'sometestfuncname', j)

    d = {'hello': 'world!'}
    mod = j(modclient)
    mod.set("fookey", ".", d)
    assert mod.get('fookey') == d
