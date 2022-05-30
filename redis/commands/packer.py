from typing import Iterable, List, Tuple

from redis.encoder import Encoder
from redis.typing import EncodableT, EncodedT

try:
    from redis.speedups import CommandPacker as _CommandPacker

    SPEEDUPS = True
except Exception:
    SPEEDUPS = False


__all__ = ["CommandPacker", "SPEEDUPS", "SYM_CRLF"]


SYM_STAR = b"*"
SYM_DOLLAR = b"$"
SYM_CRLF = b"\r\n"
SYM_EMPTY = b""


class CommandPacker:
    __slots__ = (
        "_command_packer",
        "buffer_cutoff",
        "encoder",
        "encoding",
        "errors",
        "speedups",
    )

    def __init__(self, encoder: Encoder, speedups: bool = True) -> None:
        self.buffer_cutoff = 6000
        self.encoder = encoder
        self.encoding = encoder.encoding
        self.errors = encoder.encoding_errors
        self.speedups = SPEEDUPS and speedups
        if self.speedups:
            self._command_packer = _CommandPacker(self.encoding, self.errors)

    def pack_command(self, *args: EncodableT) -> List[EncodedT]:
        if self.speedups:
            try:
                return self._command_packer.pack_command(args)
            except Exception:
                ...

        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. These arguments should be bytestrings so that they are
        # not encoded.
        if isinstance(args[0], str):
            args = tuple(args[0].encode().split()) + args[1:]
        elif b" " in args[0]:
            args = tuple(args[0].split()) + args[1:]

        output = []
        buff = SYM_EMPTY.join((SYM_STAR, str(len(args)).encode(), SYM_CRLF))

        buffer_cutoff = self.buffer_cutoff
        for arg in map(self.encoder.encode, args):
            # to avoid large string mallocs, chunk the command into the
            # output list if we're sending large values or memoryviews
            arg_length = len(arg)
            if (
                len(buff) > buffer_cutoff
                or arg_length > buffer_cutoff
                or isinstance(arg, memoryview)
            ):
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, str(arg_length).encode(), SYM_CRLF)
                )
                output.append(buff)
                output.append(arg)
                buff = SYM_CRLF
            else:
                buff = SYM_EMPTY.join(
                    (
                        buff,
                        SYM_DOLLAR,
                        str(arg_length).encode(),
                        SYM_CRLF,
                        arg,
                        SYM_CRLF,
                    )
                )
        output.append(buff)
        return output

    def pack_commands(
        self, commands: Iterable[Tuple[EncodableT, ...]]
    ) -> List[EncodedT]:
        if self.speedups:
            try:
                return self._command_packer.pack_commands(commands)
            except Exception:
                ...

        output: List[bytes] = []
        pieces: List[bytes] = []
        buffer_length = 0
        buffer_cutoff = self.buffer_cutoff

        for cmd in commands:
            for chunk in self.pack_command(*cmd):
                chunklen = len(chunk)
                if (
                    buffer_length > buffer_cutoff
                    or chunklen > buffer_cutoff
                    or isinstance(chunk, memoryview)
                ):
                    output.append(SYM_EMPTY.join(pieces))
                    buffer_length = 0
                    pieces = []

                if chunklen > buffer_cutoff or isinstance(chunk, memoryview):
                    output.append(chunk)
                else:
                    pieces.append(chunk)
                    buffer_length += chunklen

        if pieces:
            output.append(SYM_EMPTY.join(pieces))
        return output
