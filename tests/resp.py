import itertools
from types import NoneType
from typing import Any, Optional


class RespEncoder:
    """
    A class for simple RESP protocol encodign for unit tests
    """

    def __init__(self, protocol: int = 2, encoding: str = "utf-8") -> None:
        self.protocol = protocol
        self.encoding = encoding

    def encode(self, data: Any, hint: Optional[str] = None) -> bytes:
        if isinstance(data, dict):
            if self.protocol > 2:
                result = f"%{len(data)}\r\n".encode()
                for key, val in data.items():
                    result += self.encode(key) + self.encode(val)
                return result
            else:
                # Automatically encode dicts as flattened key, value arrays
                mylist = list(
                    itertools.chain(*((key, val) for (key, val) in data.items()))
                )
                return self.encode(mylist)

        elif isinstance(data, list):
            result = f"*{len(data)}\r\n".encode()
            for val in data:
                result += self.encode(val)
            return result

        elif isinstance(data, set):
            if self.protocol > 2:
                result = f"~{len(data)}\r\n".encode()
                for val in data:
                    result += self.encode(val)
                return result
            else:
                return self.encode(list(data))

        elif isinstance(data, str):
            enc = data.encode(self.encoding)
            # long strings or strings with control characters must be encoded as bulk
            # strings
            if hint or len(enc) > 20 or b"\r" in enc or b"\n" in enc:
                return self.encode_bulkstr(enc, hint)
            return b"+" + enc + b"\r\n"

        elif isinstance(data, bytes):
            return self.encode_bulkstr(data, hint)

        elif isinstance(data, bool):
            if self.protocol == 2:
                return b":1\r\n" if data else b":0\r\n"
            else:
                return b"t\r\n" if data else b"f\r\n"

        elif isinstance(data, int):
            if (data > 2**63 - 1) or (data < -(2**63)):
                if self.protocol > 2:
                    return f"({data}\r\n".encode()  # resp3 big int
                else:
                    return f"+{data}\r\n".encode()  # force to simple string
            return f":{data}\r\n".encode()
        elif isinstance(data, float):
            if self.protocol > 2:
                return f",{data}\r\n".encode()  # resp3 double
            else:
                return f"+{data}\r\n".encode()  # simple string

        elif isinstance(data, NoneType):
            if self.protocol > 2:
                return b"_\r\n"  # resp3 null
            else:
                return b"$-1\r\n"  # Null bulk string
                # some commands return null array: b"*-1\r\n"

        else:
            raise NotImplementedError

    def encode_bulkstr(self, bstr: bytes, hint: Optional[str]) -> bytes:
        if self.protocol > 2 and hint is not None:
            # a resp3 verbatim string
            return f"={len(bstr)}\r\n{hint}:".encode() + bstr + b"\r\n"
        else:
            return f"${len(bstr)}\r\n".encode() + bstr + b"\r\n"


def encode(value: Any, protocol: int = 2, hint: Optional[str] = None) -> bytes:
    return RespEncoder(protocol).encode(value, hint)
