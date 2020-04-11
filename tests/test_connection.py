import mock
import pytest

from redis.exceptions import InvalidResponse
from redis.utils import HIREDIS_AVAILABLE


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason='PythonParser only')
def test_invalid_response(r):
    raw = b'x'
    parser = r.connection._parser
    with mock.patch.object(parser._buffer, 'readline', return_value=raw):
        with pytest.raises(InvalidResponse) as cm:
            parser.read_response()
    assert str(cm.value) == 'Protocol Error: %r' % raw
