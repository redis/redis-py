import pytest

from .resp import PushData, VerbatimString, encode, parse_all, parse_chunks


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

    def test_push_data(self, resp_version):
        data = encode(PushData([1, 2, 3]), protocol=resp_version)
        if resp_version == 2:
            assert data == b"*3\r\n:1\r\n:2\r\n:3\r\n"
        else:
            assert data == b">3\r\n:1\r\n:2\r\n:3\r\n"

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


@pytest.mark.parametrize("chunk_size", [0, 1, 2, -2])
class TestParser:
    def breakup_bytes(self, data, chunk_size=2):
        insert_empty = False
        if chunk_size < 0:
            insert_empty = True
            chunk_size = -chunk_size
        chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
        if insert_empty:
            empty = len(chunks) * [b""]
            chunks = [item for pair in zip(chunks, empty) for item in pair]
        return chunks

    def parse_data(self, chunk_size, data):
        """helper to parse either a single blob, or a list of chunks"""
        if chunk_size == 0:
            return parse_all(data)
        else:
            return parse_chunks(self.breakup_bytes(data, chunk_size))

    def test_int(self, chunk_size):
        parsed = self.parse_data(chunk_size, b":123\r\n")
        assert parsed == ([123], b"")

        parsed = self.parse_data(chunk_size, b":123\r\nfoo")
        assert parsed == ([123], b"foo")

    def test_double(self, chunk_size):
        parsed = self.parse_data(chunk_size, b",1.23\r\njunk")
        assert parsed == ([1.23], b"junk")

    def test_array(self, chunk_size):
        parsed = self.parse_data(chunk_size, b"*3\r\n:1\r\n:2\r\n:3\r\n")
        assert parsed == ([[1, 2, 3]], b"")

        parsed = self.parse_data(chunk_size, b"*3\r\n:1\r\n:2\r\n:3\r\nfoo")
        assert parsed == ([[1, 2, 3]], b"foo")

    def test_push_data(self, chunk_size):
        parsed = self.parse_data(chunk_size, b">3\r\n:1\r\n:2\r\n:3\r\n")
        assert isinstance(parsed[0][0], PushData)
        assert parsed == ([[1, 2, 3]], b"")

    def test_incomplete_list(self, chunk_size):
        parsed = self.parse_data(chunk_size, b"*3\r\n:1\r\n:2\r\n")
        assert parsed == ([], b"*3\r\n:1\r\n:2\r\n")

    def test_invalid_token(self, chunk_size):
        with pytest.raises(ValueError):
            self.parse_data(chunk_size, b")foo\r\n")
        with pytest.raises(NotImplementedError):
            self.parse_data(chunk_size, b"!foo\r\n")

    def test_multiple_ints(self, chunk_size):
        parsed = self.parse_data(chunk_size, b":1\r\n:2\r\n:3\r\n")
        assert parsed == ([1, 2, 3], b"")

    def test_multiple_ints_and_junk(self, chunk_size):
        parsed = self.parse_data(chunk_size, b":1\r\n:2\r\n:3\r\n*3\r\n:1\r\n:2\r\n")
        assert parsed == ([1, 2, 3], b"*3\r\n:1\r\n:2\r\n")

    def test_set(self, chunk_size):
        parsed = self.parse_data(chunk_size, b"~3\r\n:1\r\n:2\r\n:3\r\n")
        assert parsed == ([{1, 2, 3}], b"")

    def test_list_of_sets(self, chunk_size):
        parsed = self.parse_data(
            chunk_size, b"*2\r\n~3\r\n:1\r\n:2\r\n:3\r\n~2\r\n:4\r\n:5\r\n"
        )
        assert parsed == ([[{1, 2, 3}, {4, 5}]], b"")

    def test_map(self, chunk_size):
        parsed = self.parse_data(chunk_size, b"%2\r\n:1\r\n:2\r\n:3\r\n:4\r\n")
        assert parsed == ([{1: 2, 3: 4}], b"")

    def test_simple_string(self, chunk_size):
        parsed = self.parse_data(chunk_size, b"+foo\r\n")
        assert parsed == (["foo"], b"")

    def test_bulk_string(self, chunk_size):
        parsed = parse_all(b"$3\r\nfoo\r\nbar")
        assert parsed == ([b"foo"], b"bar")

    def test_bulk_string_with_ctrl_chars(self, chunk_size):
        parsed = self.parse_data(chunk_size, b"$8\r\nfoo\r\nbar\r\n")
        assert parsed == ([b"foo\r\nbar"], b"")

    def test_verbatim_string(self, chunk_size):
        parsed = self.parse_data(chunk_size, b"=3\r\ntxt:foo\r\nbar")
        assert parsed == ([VerbatimString(b"foo", "txt")], b"bar")
