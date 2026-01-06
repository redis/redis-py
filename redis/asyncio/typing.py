from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio.client import Redis
    from redis.typing import (
        AnyStringType,
        DecodedStringType,
        EncodedStringType,
        ListOfAnyOptionalStringsType,
        ListOfAnyStringsType,
        ListOfDecodedStringsType,
        ListOfEncodedStringsType,
        ListOfOptionalDecodedStringsType,
        ListOfOptionalEncodedStringsType,
        LMPopAnyReturnType,
        LMPopDecodedReturnType,
        LMPopEncodedReturnType,
        LPopRPopAnyReturnType,
        LPopRPopDecodedReturnType,
        LPopRPopEncodedReturnType,
        OptionalAnyStringType,
        OptionalDecodedStringType,
        OptionalEncodedStringType,
        OptionalListOfAnyStringsType,
        OptionalListOfDecodedStringsType,
        OptionalListOfEncodedStringsType,
    )

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
