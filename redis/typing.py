from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Type,
    TypedDict,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    from redis._parsers import Encoder


Number = Union[int, float]
EncodedT = Union[bytes, bytearray, memoryview]
DecodedT = Union[str, int, float]
EncodableT = Union[EncodedT, DecodedT]
AbsExpiryT = Union[int, datetime]
ExpiryT = Union[int, timedelta]
ZScoreBoundT = Union[float, str]  # str allows for the [ or ( prefix
BitfieldOffsetT = Union[int, str]  # str allows for #x syntax
_StringLikeT = Union[bytes, str, memoryview]
KeyT = _StringLikeT  # Main redis key space
PatternT = _StringLikeT  # Patterns matched against keys, fields etc
FieldT = EncodableT  # Fields within hash tables, streams and geo commands
KeysT = Union[KeyT, Iterable[KeyT]]
ResponseT = Union[Awaitable[Any], Any]
ChannelT = _StringLikeT
GroupT = _StringLikeT  # Consumer group
ConsumerT = _StringLikeT  # Consumer name
StreamIdT = Union[int, _StringLikeT]
ScriptTextT = _StringLikeT
TimeoutSecT = Union[int, float, _StringLikeT]
# Mapping is not covariant in the key type, which prevents
# Mapping[_StringLikeT, X] from accepting arguments of type Dict[str, X]. Using
# a TypeVar instead of a Union allows mappings with any of the permitted types
# to be passed. Care is needed if there is more than one such mapping in a
# type signature because they will all be required to be the same key type.
AnyKeyT = TypeVar("AnyKeyT", bytes, str, memoryview)
AnyFieldT = TypeVar("AnyFieldT", bytes, str, memoryview)
AnyChannelT = TypeVar("AnyChannelT", bytes, str, memoryview)

ExceptionMappingT = Mapping[str, Union[Type[Exception], Mapping[str, Type[Exception]]]]

BooleanType = bool
IntegerType = int
OptionalIntegerType = Optional[IntegerType]
FloatType = float

DecodedStringType = str
EncodedStringType = bytes
AnyStringType = Union[DecodedStringType, EncodedStringType]
OptionalDecodedStringType = Optional[DecodedStringType]
OptionalEncodedStringType = Optional[EncodedStringType]
OptionalAnyStringType = Union[OptionalDecodedStringType, OptionalEncodedStringType]

ListOfDecodedStringsType = list[DecodedStringType]
ListOfEncodedStringsType = list[EncodedStringType]
OptionalListOfDecodedStringsType = Optional[ListOfDecodedStringsType]
OptionalListOfEncodedStringsType = Optional[ListOfEncodedStringsType]
ListOfAnyStringsType = Union[ListOfDecodedStringsType, ListOfEncodedStringsType]
OptionalListOfAnyStringsType = Union[
    OptionalListOfDecodedStringsType,
    OptionalListOfEncodedStringsType,
]

ListOfOptionalDecodedStringsType = list[OptionalDecodedStringType]
ListOfOptionalEncodedStringsType = list[OptionalEncodedStringType]
ListOfAnyOptionalStringsType = Union[
    ListOfOptionalDecodedStringsType,
    ListOfOptionalEncodedStringsType,
]

LPopRPopDecodedReturnType = Union[
    DecodedStringType,  # Single value when count not specified
    ListOfDecodedStringsType,  # List when count is specified
    None,  # None when list is empty
]
LPopRPopEncodedReturnType = Union[
    EncodedStringType,  # Single value when count not specified
    ListOfEncodedStringsType,  # List when count is specified
    None,  # None when list is empty
]

# lpop / rpop can return single value, list, or None
LPopRPopAnyReturnType = Union[
    LPopRPopDecodedReturnType,
    LPopRPopEncodedReturnType,
]

# blmpop / lmpop return types
# Returns a list containing [key_name, [values...]] or None
# PyCharm doesn't like use of Optional here
LMPopDecodedReturnType = Union[
    list[
        Union[
            DecodedStringType,  # key_name
            list[DecodedStringType],  # [values, ...]
        ],
    ],
    None,  # or None
]
LMPopEncodedReturnType = Union[
    list[
        Union[
            EncodedStringType,  # key_name
            list[EncodedStringType],  # [values, ...]
        ],
    ],
    None,  # or None
]
LMPopAnyReturnType = Union[LMPopDecodedReturnType, LMPopEncodedReturnType]

# STRALGO return types
# Represents the ranges, e.g., (4, 7)
PositionRange = tuple[IntegerType, IntegerType]

# Represents a match entry when WITHMATCHLEN is False
# Example: [(4, 7), (5, 8)]
MatchSequence = list[PositionRange]

# Represents a match entry when WITHMATCHLEN is True
# Example: [4, (4, 7), (5, 8)]  <-- First item is int (len), rest are ranges
MatchSequenceWithLen = list[Union[IntegerType, PositionRange]]


class StrAlgoIdxResponse(TypedDict):
    """
    Return type when IDX=True (No WITHMATCHLEN)
    Example: {'matches': [[(4, 7), (5, 8)]], 'len': 6}
    """

    matches: list[MatchSequence]
    len: IntegerType


class StrAlgoIdxWithLenResponse(TypedDict):
    """
    Return type when IDX=True AND WITHMATCHLEN=True
    Example: {'matches': [[4, (4, 7), (5, 8)]], 'len': 6}
    """

    matches: list[MatchSequenceWithLen]
    len: IntegerType


StrAlgoResultType = Union[
    DecodedStringType,  # str (default)
    IntegerType,  # int (LEN argument)
    StrAlgoIdxResponse,  # dict (IDX argument)
    StrAlgoIdxWithLenResponse,  # dict (IDX + WITHMATCHLEN argument)
]


ResponseTypeBoolean = TypeVar(
    "ResponseTypeBoolean",
    bound=Awaitable[BooleanType] | BooleanType,
)
ResponseTypeInteger = TypeVar(
    "ResponseTypeInteger",
    bound=Awaitable[IntegerType] | IntegerType,
)
ResponseTypeFloat = TypeVar(
    "ResponseTypeFloat",
    bound=Awaitable[FloatType] | FloatType,
)
ResponseTypeAnyString = TypeVar(
    "ResponseTypeAnyString",
    bound=Awaitable[AnyStringType] | AnyStringType,
)
ResponseTypeOptionalEncodedString = TypeVar(
    "ResponseTypeOptionalEncodedString",
    bound=Awaitable[OptionalEncodedStringType] | OptionalEncodedStringType,
)
ResponseTypeOptionalAnyString = TypeVar(
    "ResponseTypeOptionalAnyString",
    bound=Awaitable[OptionalAnyStringType] | OptionalAnyStringType,
)
ResponseTypeListOfAnyStrings = TypeVar(
    "ResponseTypeListOfAnyStrings",
    bound=Awaitable[ListOfAnyStringsType] | ListOfAnyStringsType,
)
ResponseTypeListOfAnyOptionalStrings = TypeVar(
    "ResponseTypeListOfAnyOptionalStrings",
    bound=Awaitable[ListOfAnyOptionalStringsType] | ListOfAnyOptionalStringsType,
)
ResponseTypeOptionalInteger = TypeVar(
    "ResponseTypeOptionalInteger",
    bound=Awaitable[OptionalIntegerType] | OptionalIntegerType,
)
ResponseTypeOptionalListOfAnyStrings = TypeVar(
    "ResponseTypeOptionalListOfAnyStrings",
    bound=Awaitable[OptionalListOfAnyStringsType] | OptionalListOfAnyStringsType,
)
ResponseTypeLPopRPop = TypeVar(
    "ResponseTypeLPopRPop",
    bound=Awaitable[LPopRPopAnyReturnType] | LPopRPopAnyReturnType,
)
ResponseTypeOptionalLMPop = TypeVar(
    "ResponseTypeOptionalLMPop",
    bound=Awaitable[LMPopAnyReturnType] | LMPopAnyReturnType,
)
ResponseTypeStrAlgoResult = TypeVar(
    "ResponseTypeStrAlgoResult",
    bound=Awaitable[StrAlgoResultType] | StrAlgoResultType,
)


class CommandsProtocol(Protocol):
    def execute_command(self, *args, **options) -> ResponseT: ...


class ClusterCommandsProtocol(CommandsProtocol):
    encoder: "Encoder"


if TYPE_CHECKING:
    from redis.client import Redis

    RedisEncoded = Redis[
        EncodedStringType,
        OptionalEncodedStringType,
        ListOfEncodedStringsType,
        ListOfOptionalEncodedStringsType,
        OptionalListOfEncodedStringsType,
        LMPopEncodedReturnType,
        LPopRPopEncodedReturnType,
    ]
    RedisDecoded = Redis[
        DecodedStringType,
        OptionalDecodedStringType,
        ListOfDecodedStringsType,
        ListOfOptionalDecodedStringsType,
        OptionalListOfDecodedStringsType,
        LMPopDecodedReturnType,
        LPopRPopDecodedReturnType,
    ]
    RedisEncodedOrDecoded = Redis[
        AnyStringType,
        OptionalAnyStringType,
        ListOfAnyStringsType,
        ListOfAnyOptionalStringsType,
        OptionalListOfAnyStringsType,
        LMPopAnyReturnType,
        LPopRPopAnyReturnType,
    ]
