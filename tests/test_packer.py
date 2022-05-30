import sys

import pytest

from redis.commands.packer import SPEEDUPS, CommandPacker
from redis.encoder import Encoder
from redis.exceptions import DataError

ENCODINGS = ("utf-8", "utf-16", "utf-32")
ERRORS = ("strict", "replace", "ignore")


@pytest.mark.skipif(not SPEEDUPS, reason="CommandPacker speedups are not installed")
class TestCCommandPacker:
    @pytest.fixture(
        params=[(encoding, errors) for encoding in ENCODINGS for errors in ERRORS],
        ids=[f"{encoding}-{errors}" for encoding in ENCODINGS for errors in ERRORS],
    )
    def cp(self, request):
        encoding, errors = request.param

        def _cp():
            encoder = Encoder(
                encoding=encoding, encoding_errors=errors, decode_responses=True
            )
            pycp = CommandPacker(encoder=encoder, speedups=False)
            assert not pycp.speedups
            ccp = CommandPacker(encoder=encoder, speedups=True)
            assert ccp.speedups
            return pycp, ccp

        return _cp

    def test_ccommand_packer(self, cp):
        pycp, ccp = cp()

        commands = [
            ("INFO",),
            (b"INFO",),
            ("CLUSTER SLOTS",),
            (b"CLUSTER SLOTS",),
            ("CONFIG GET", "key"),
            (b"CONFIG GET", "key"),
            ("CONFIG GET", b"key"),
            (b"CONFIG GET", b"key"),
            ("CONFIG GET", b"key"),
            (b"HGET", b"key", "field", "value"),
            (b"HGET", "key", b"field", "value"),
            (b"HGET", "key", "field", b"value"),
            ("HSET", "key", "field", "value"),
            ("HSET", "key", b"field", "value"),
            ("HSET", "key", "field" * 1500, "value"),
            ("HSET", "key", b"field" * 1500, "value"),
            ("HSET", "key", 1, "value"),
            ("HSET", "key", int("1" * 10000), "value"),
            ("HSET", "key", sys.maxsize, "value"),
            ("HSET", "key", 1.1, "value"),
            ("HSET", "key", 1 / 3, "value"),
            ("HSET", "key", 0.2 + 0.1, "value"),
            ("HSET", "key", float("1" * 10000), "value"),
            ("HSET", "key", sys.float_info.max, "value"),
            ("HSET", "key", memoryview(b"field"), "value"),
            ("HSET", "key", memoryview(b"some_arg"), "value"),
        ]
        for cmd in commands:
            assert b"".join(pycp.pack_command(*cmd)) == b"".join(ccp.pack_command(*cmd))

        assert b"".join(pycp.pack_commands(cmd for cmd in commands)) == b"".join(
            ccp.pack_commands(cmd for cmd in commands)
        )

    def test_ccommand_packer_seg_fault(self, cp):
        pycp, ccp = cp()

        args = [b"HGET", "key", "field", b"value"]
        for i in range(1, 10000):
            commands = []
            args[2] = "s" * i
            commands.append(tuple(args))
            assert b"".join(pycp.pack_command(*args)) == b"".join(
                ccp.pack_command(*args)
            )
            args[2] = b"b" * i
            commands.append(tuple(args))
            assert b"".join(pycp.pack_command(*args)) == b"".join(
                ccp.pack_command(*args)
            )
            args[3] = min(int("1" * i), sys.maxsize)
            commands.append(tuple(args))
            assert b"".join(pycp.pack_command(*args)) == b"".join(
                ccp.pack_command(*args)
            )
            args[3] = min(float("1" * i), sys.float_info.max)
            commands.append(tuple(args))
            assert b"".join(pycp.pack_command(*args)) == b"".join(
                ccp.pack_command(*args)
            )
            args[3] = memoryview(b"m" * i)
            commands.append(tuple(args))
            assert b"".join(pycp.pack_command(*args)) == b"".join(
                ccp.pack_command(*args)
            )

            assert b"".join(pycp.pack_commands(cmd for cmd in commands)) == b"".join(
                ccp.pack_commands(cmd for cmd in commands)
            )

    def test_ccommand_packer_invalid_arg(self, cp):
        pycp, ccp = cp()

        args = (b"HGET", "key", {"field"}, b"value")

        with pytest.raises(DataError):
            pycp.pack_command(*args)
        with pytest.raises(DataError):
            pycp.pack_commands(cmd for cmd in [args, args, args])

        with pytest.raises(DataError):
            ccp.pack_command(*args)
        with pytest.raises(DataError):
            ccp.pack_commands(cmd for cmd in [args, args, args])
