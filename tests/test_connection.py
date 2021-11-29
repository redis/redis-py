from unittest import mock
import types
import pytest

from redis.exceptions import InvalidResponse
from redis.utils import HIREDIS_AVAILABLE
from .conftest import skip_if_server_version_lt


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason='PythonParser only')
@pytest.mark.onlynoncluster
def test_invalid_response(r):
    raw = b'x'
    parser = r.connection._parser
    with mock.patch.object(parser._buffer, 'readline', return_value=raw):
        with pytest.raises(InvalidResponse) as cm:
            parser.read_response()
    assert str(cm.value) == 'Protocol Error: %r' % raw


@skip_if_server_version_lt('4.0.0')
@pytest.mark.redismod
def test_loading_external_modules(modclient):
    def inner():
        pass

    modclient.load_external_module('myfuncname', inner)
    assert getattr(modclient, 'myfuncname') == inner
    assert isinstance(getattr(modclient, 'myfuncname'), types.FunctionType)

    # and call it
    from redis.commands import RedisModuleCommands
    j = RedisModuleCommands.json
    modclient.load_external_module('sometestfuncname', j)

    # d = {'hello': 'world!'}
    # mod = j(modclient)
    # mod.set("fookey", ".", d)
    # assert mod.get('fookey') == d
