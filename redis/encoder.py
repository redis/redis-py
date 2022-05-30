from redis.exceptions import DataError
from redis.typing import EncodableT, EncodedT

__all__ = ["Encoder"]


class Encoder:
    __slots__ = "encoding", "encoding_errors", "decode_responses"

    def __init__(self, encoding: str, encoding_errors: str, decode_responses: bool):
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses

    def encode(self, value: EncodableT) -> EncodedT:
        if isinstance(value, str):
            return value.encode(self.encoding, self.encoding_errors)
        if isinstance(value, (bytes, memoryview)):
            return value
        if isinstance(value, (int, float)):
            if isinstance(value, bool):
                # special case bool since it is a subclass of int
                raise DataError(
                    "Invalid input of type: 'bool'. "
                    "Convert to a bytes, string, int or float first."
                )
            return repr(value).encode()
        # a value we don't know how to deal with. throw an error
        typename = value.__class__.__name__
        raise DataError(
            f"Invalid input of type: {typename!r}. "
            "Convert to a bytes, string, int or float first."
        )

    def decode(self, value: EncodedT, force: bool = False) -> EncodableT:
        if self.decode_responses or force:
            if isinstance(value, bytes):
                return value.decode(self.encoding, self.encoding_errors)
            if isinstance(value, memoryview):
                return value.tobytes().decode(self.encoding, self.encoding_errors)
        return value
