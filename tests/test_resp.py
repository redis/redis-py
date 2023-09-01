from .resp import encode

import pytest


@pytest.fixture(params=[2, 3])
def resp_version(request):
    return request.param


class TestEncoder:
    def test_simple_str(self):
        assert encode("foo") == b"+foo\r\n"

    def test_long_str(self):
        text = "fooling around with the sword in the mud"
        assert len(text) == 40
        assert encode(text) == b"$40\r\n" + text.encode() + b"\r\n"

    # test strings with control characters
    def test_str_with_ctrl_chars(self):
        text = "foo\r\nbar"
        assert encode(text) == b"$8\r\nfoo\r\nbar\r\n"

    def test_bytes(self):
        assert encode(b"foo") == b"$3\r\nfoo\r\n"

    def test_int(self):
        assert encode(123) == b":123\r\n"

    def test_float(self, resp_version):
        data = encode(1.23, protocol=resp_version)
        if resp_version == 2:
            assert data == b"+1.23\r\n"
        else:
            assert data == b",1.23\r\n"

    def test_large_int(self, resp_version):
        data = encode(2**63, protocol=resp_version)
        if resp_version == 2:
            assert data == b"+9223372036854775808\r\n"
        else:
            assert data == b"(9223372036854775808\r\n"

    def test_array(self):
        assert encode([1, 2, 3]) == b"*3\r\n:1\r\n:2\r\n:3\r\n"

    def test_set(self, resp_version):
        data = encode({1, 2, 3}, protocol=resp_version)
        if resp_version == 2:
            assert data == b"*3\r\n:1\r\n:2\r\n:3\r\n"
        else:
            assert data == b"~3\r\n:1\r\n:2\r\n:3\r\n"

    def test_map(self, resp_version):
        data = encode({1: 2, 3: 4}, protocol=resp_version)
        if resp_version == 2:
            assert data == b"*4\r\n:1\r\n:2\r\n:3\r\n:4\r\n"
        else:
            assert data == b"%2\r\n:1\r\n:2\r\n:3\r\n:4\r\n"

    def test_nested_array(self):
        assert encode([1, [2, 3]]) == b"*2\r\n:1\r\n*2\r\n:2\r\n:3\r\n"

    def test_nested_map(self, resp_version):
        data = encode({1: {2: 3}}, protocol=resp_version)
        if resp_version == 2:
            assert data == b"*2\r\n:1\r\n*2\r\n:2\r\n:3\r\n"
        else:
            assert data == b"%1\r\n:1\r\n%1\r\n:2\r\n:3\r\n"

    def test_null(self, resp_version):
        data = encode(None, protocol=resp_version)
        if resp_version == 2:
            assert data == b"$-1\r\n"
        else:
            assert data == b"_\r\n"

    def test_mixed_array(self, resp_version):
        data = encode([1, "foo", 2.3, None, True], protocol=resp_version)
        if resp_version == 2:
            assert data == b"*5\r\n:1\r\n+foo\r\n+2.3\r\n$-1\r\n:1\r\n"
        else:
            assert data == b"*5\r\n:1\r\n+foo\r\n,2.3\r\n_\r\nt\r\n"

    def test_bool(self, resp_version):
        data = encode(True, protocol=resp_version)
        if resp_version == 2:
            assert data == b":1\r\n"
        else:
            assert data == b"t\r\n"

        data = encode(False, resp_version)
        if resp_version == 2:
            assert data == b":0\r\n"
        else:
            assert data == b"f\r\n"
