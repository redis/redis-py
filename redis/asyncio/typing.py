from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio.client import Redis
    from redis.typing import (
        AnyStringType,
        DecodedStringType,
        EncodedStringType,
        ListOfAnyOptionalStringsType,
        ListOfAnyStrings,
        ListOfDecodedStringsType,
        ListOfEncodedStringsType,
        ListOfOptionalDecodedStringsType,
        ListOfOptionalEncodedStringsType,
        OptionalAnyString,
        OptionalDecodedStringType,
        OptionalEncodedStringType,
    )

    RedisEncoded = Redis[
        EncodedStringType,
        OptionalEncodedStringType,
        ListOfEncodedStringsType,
        ListOfOptionalEncodedStringsType,
    ]
    RedisDecoded = Redis[
        DecodedStringType,
        OptionalDecodedStringType,
        ListOfDecodedStringsType,
        ListOfOptionalDecodedStringsType,
    ]
    RedisEncodedOrDecoded = Redis[
        AnyStringType,
        OptionalAnyString,
        ListOfAnyStrings,
        ListOfAnyOptionalStringsType,
    ]
