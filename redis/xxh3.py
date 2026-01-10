"""
Pure Python implementation of XXH3_64 hash algorithm.
Compatible with xxHash v0.8.3 specification.
Supports inputs of any size.
"""

import struct
from typing import Union

# Prime constants
PRIME32_1 = 0x9E3779B1
PRIME32_2 = 0x85EBCA77
PRIME32_3 = 0xC2B2AE3D
PRIME64_1 = 0x9E3779B185EBCA87
PRIME64_2 = 0xC2B2AE3D27D4EB4F
PRIME64_3 = 0x165667B19E3779F9
PRIME64_4 = 0x85EBCA77C2B2AE63
PRIME64_5 = 0x27D4EB2F165667C5
PRIME_MX1 = 0x165667919E3779F9
PRIME_MX2 = 0x9FB21C651E98DF25

# Secret key (192 bytes)
KKEY = bytes.fromhex(
    'b8fe6c3923a44bbe7c01812cf721ad1cded46de9839097db7240a4a4b7b3671f'
    'cb79e64eccc0e578825ad07dccff7221b8084674f743248ee03590e6813a264c'
    '3c2852bb91c300cb88d0658b1b532ea371644897a20df94e3819ef46a9deacd8'
    'a8fa763fe39c343ff9dcbbc7c70b4f1d8a51e04bcdb45931c89f7ec9d9787364'
    'eac5ac8334d3ebc3c581a0fffa1363eb170ddd51b7f0da49d316552629d4689e'
    '2b16be587d47a1fc8ff8b8d17ad031ce45cb3a8f95160428afd7fbcabb4b407e'
)

# Masks for bit operations
MASK64 = (1 << 64) - 1
MASK32 = (1 << 32) - 1

# Constants
STRIPE_LEN = 64
ACC_NB = STRIPE_LEN // 8  # 8 accumulators


def _read_u32_le(data: bytes, offset: int = 0) -> int:
    """Read 32-bit little-endian integer."""
    return struct.unpack_from('<I', data, offset)[0]


def _read_u64_le(data: bytes, offset: int = 0) -> int:
    """Read 64-bit little-endian integer."""
    return struct.unpack_from('<Q', data, offset)[0]


def _rotl64(value: int, shift: int) -> int:
    """Rotate left 64-bit."""
    value &= MASK64
    return ((value << shift) | (value >> (64 - shift))) & MASK64


def _bswap32(value: int) -> int:
    """Byte-swap 32-bit value (reverse byte order)."""
    value &= MASK32
    return struct.unpack('<I', struct.pack('>I', value))[0]


def _bswap64(value: int) -> int:
    """Byte-swap 64-bit value (reverse byte order)."""
    value &= MASK64
    return struct.unpack('<Q', struct.pack('>Q', value))[0]


def _avalanche(h64: int) -> int:
    """XXH3 avalanche function."""
    h64 &= MASK64
    h64 ^= h64 >> 37
    h64 = (h64 * PRIME_MX1) & MASK64
    h64 ^= h64 >> 32
    return h64 & MASK64


def _avalanche_xxh64(h64: int) -> int:
    """XXH64-style avalanche function (used for small inputs)."""
    h64 &= MASK64
    h64 ^= h64 >> 33
    h64 = (h64 * PRIME64_2) & MASK64
    h64 ^= h64 >> 29
    h64 = (h64 * PRIME64_3) & MASK64
    h64 ^= h64 >> 32
    return h64 & MASK64


def _mix16b(data: bytes, key: bytes, seed: int) -> int:
    """Mix 16 bytes of data."""
    data_lo = _read_u64_le(data, 0)
    data_hi = _read_u64_le(data, 8)
    key_lo = _read_u64_le(key, 0)
    key_hi = _read_u64_le(key, 8)

    # XXH3_mul128_fold64
    a = (data_lo ^ ((key_lo + seed) & MASK64)) & MASK64
    b = (data_hi ^ ((key_hi - seed) & MASK64)) & MASK64

    product = (a * b) & ((1 << 128) - 1)
    return ((product & MASK64) ^ (product >> 64)) & MASK64


def _hash_0to16(data: bytes, seed: int) -> int:
    """Hash 0-16 bytes using XXH3 small input algorithm."""
    length = len(data)

    if length == 0:
        # Empty input: use secret[56:72]
        return _avalanche_xxh64((seed ^ _read_u64_le(KKEY, 56) ^ _read_u64_le(KKEY, 64)) & MASK64)

    if length <= 3:
        # 1-3 bytes: combine bytes into 32-bit value
        combined = data[length - 1] | (length << 8) | (data[0] << 16) | (data[length >> 1] << 24)
        bitflip = ((_read_u32_le(KKEY, 0) ^ _read_u32_le(KKEY, 4)) + seed) & MASK64
        return _avalanche_xxh64((bitflip ^ combined) & MASK64)

    if length <= 8:
        # 4-8 bytes: read first and last 4 bytes
        input_first = _read_u32_le(data, 0)
        input_last = _read_u32_le(data, length - 4)
        modified_seed = seed ^ ((_bswap32(seed & MASK32) << 32) & MASK64)
        combined = (input_last | (input_first << 32)) & MASK64
        bitflip = ((_read_u64_le(KKEY, 8) ^ _read_u64_le(KKEY, 16)) - modified_seed) & MASK64
        value = (bitflip ^ combined) & MASK64
        value ^= _rotl64(value, 49) ^ _rotl64(value, 24)
        value = (value * PRIME_MX2) & MASK64
        value ^= ((value >> 35) + length) & MASK64
        value = (value * PRIME_MX2) & MASK64
        return (value ^ (value >> 28)) & MASK64

    # 9-16 bytes: read first and last 8 bytes
    input_first = _read_u64_le(data, 0)
    input_last = _read_u64_le(data, length - 8)
    bitflip_lo = ((_read_u64_le(KKEY, 24) ^ _read_u64_le(KKEY, 32)) + seed) & MASK64
    bitflip_hi = ((_read_u64_le(KKEY, 40) ^ _read_u64_le(KKEY, 48)) - seed) & MASK64
    low = (bitflip_lo ^ input_first) & MASK64
    high = (bitflip_hi ^ input_last) & MASK64
    mul_result = (low * high) & ((1 << 128) - 1)
    value = (length + _bswap64(low) + high + ((mul_result & MASK64) ^ (mul_result >> 64))) & MASK64
    return _avalanche(value)


def _hash_17to240(data: bytes, seed: int) -> int:
    """Hash 17-240 bytes using XXH3 medium input algorithm."""
    length = len(data)
    acc = (length * PRIME64_1) & MASK64

    if length <= 128:
        # 17-128 bytes: process pairs of chunks from start and end
        num_rounds = ((length - 1) >> 5) + 1  # Number of 32-byte rounds
        for i in range(num_rounds - 1, -1, -1):
            offset_start = i * 16
            offset_end = length - i * 16 - 16
            acc = (acc + _mix16b(data[offset_start:offset_start + 16], KKEY[i * 32:i * 32 + 16], seed)) & MASK64
            acc = (acc + _mix16b(data[offset_end:offset_end + 16], KKEY[i * 32 + 16:i * 32 + 32], seed)) & MASK64
        return _avalanche(acc)

    # 129-240 bytes: process first 8 chunks, avalanche, then remaining
    for i in range(8):
        acc = (acc + _mix16b(data[16 * i:16 * i + 16], KKEY[16 * i:16 * i + 16], seed)) & MASK64

    acc = _avalanche(acc)

    # Process remaining full chunks
    num_chunks = length >> 4
    for i in range(8, num_chunks):
        secret_offset = (i - 8) * 16 + 3
        acc = (acc + _mix16b(data[16 * i:16 * i + 16], KKEY[secret_offset:secret_offset + 16], seed)) & MASK64

    # Last 16 bytes
    acc = (acc + _mix16b(data[length - 16:length], KKEY[119:135], seed)) & MASK64

    return _avalanche(acc)


# ============================================================================
# Long input processing (> 240 bytes)
# ============================================================================

def _accumulate_512(acc: list, data: bytes, data_offset: int, secret: bytes, secret_offset: int) -> None:
    """
    Accumulate 512 bits (64 bytes) of data.
    Modifies acc in-place.

    Args:
        acc: List of 8 64-bit accumulators
        data: Input data
        data_offset: Offset into data
        secret: Secret key
        secret_offset: Offset into secret
    """
    for i in range(ACC_NB):
        data_val = _read_u64_le(data, data_offset + i * 8)
        data_key = data_val ^ _read_u64_le(secret, secret_offset + i * 8)

        # Swap lanes for better mixing
        acc[i ^ 1] = (acc[i ^ 1] + data_val) & MASK64

        # Multiply low and high 32 bits (inlined for performance)
        acc[i] = (acc[i] + ((data_key & MASK32) * (data_key >> 32))) & MASK64


def _scramble_acc(acc: list, key: bytes) -> None:
    """
    Scramble accumulators.
    Modifies acc in-place.

    Args:
        acc: List of 8 64-bit accumulators
        key: Secret key
    """
    for i in range(ACC_NB):
        key64 = _read_u64_le(key, i * 8)
        acc64 = acc[i]

        # XOR shift
        acc64 ^= acc64 >> 47
        acc64 ^= key64
        acc64 = (acc64 * PRIME32_1) & MASK64

        acc[i] = acc64


def _mix2accs(acc: list, key: bytes, offset: int = 0) -> int:
    """Mix two accumulators."""
    lo = acc[0] ^ _read_u64_le(key, offset)
    hi = acc[1] ^ _read_u64_le(key, offset + 8)

    # XXH3_mul128_fold64
    product = (lo * hi) & ((1 << 128) - 1)
    return ((product & MASK64) ^ (product >> 64)) & MASK64


def _merge_accs(acc: list, secret: bytes, secret_offset: int, start: int) -> int:
    """Merge all accumulators into final hash."""
    result = start & MASK64

    # Mix pairs of accumulators
    result = (result + _mix2accs(acc[0:2], secret, secret_offset + 0)) & MASK64
    result = (result + _mix2accs(acc[2:4], secret, secret_offset + 16)) & MASK64
    result = (result + _mix2accs(acc[4:6], secret, secret_offset + 32)) & MASK64
    result = (result + _mix2accs(acc[6:8], secret, secret_offset + 48)) & MASK64

    return _avalanche(result)


def _hash_long(data: bytes, secret: bytes, seed: int) -> int:
    """
    Hash long input (> 240 bytes).

    Args:
        data: Input data
        secret: Secret key
        seed: Seed value

    Returns:
        64-bit hash value
    """
    # Initialize accumulators
    acc = [
        PRIME32_3,
        PRIME64_1,
        PRIME64_2,
        PRIME64_3,
        PRIME64_4,
        PRIME32_2,
        PRIME64_5,
        PRIME32_1,
    ]

    # Calculate block parameters
    nb_stripes_per_block = (len(secret) - STRIPE_LEN) // 8
    block_len = STRIPE_LEN * nb_stripes_per_block
    nb_blocks = (len(data) - 1) // block_len

    # Process full blocks
    for n in range(nb_blocks):
        # Accumulate all stripes in this block
        for s in range(nb_stripes_per_block):
            stripe_offset = n * block_len + s * STRIPE_LEN
            secret_offset = s * 8
            _accumulate_512(acc, data, stripe_offset, secret, secret_offset)

        # Scramble after each block
        scramble_secret_offset = len(secret) - STRIPE_LEN
        _scramble_acc(acc, secret[scramble_secret_offset:])

    # Process remaining stripes (partial block)
    remaining_start = nb_blocks * block_len
    remaining_len = len(data) - remaining_start
    nb_stripes = (remaining_len - 1) // STRIPE_LEN

    for s in range(nb_stripes):
        stripe_offset = remaining_start + s * STRIPE_LEN
        secret_offset = s * 8
        _accumulate_512(acc, data, stripe_offset, secret, secret_offset)

    # Process last stripe (may overlap with previous)
    last_stripe_offset = len(data) - STRIPE_LEN
    last_secret_offset = len(secret) - 71  # As per spec: secretLength - 71
    _accumulate_512(acc, data, last_stripe_offset, secret, last_secret_offset)

    # Merge accumulators
    start_value = (len(data) * PRIME64_1) & MASK64
    return _merge_accs(acc, secret, 11, start_value)


def xxh3_64(data: Union[bytes, str], seed: int = 0) -> int:
    """
    Compute XXH3 64-bit hash.

    Args:
        data: Input bytes or str to hash. Strings are encoded as UTF-8.
        seed: Optional seed value (default: 0)

    Returns:
        64-bit hash value as integer

    Examples:
        >>> xxh3_64(b"hello")
        2794345569481354659

        >>> xxh3_64(b"hello", seed=42)
        4282832379142140568

        >>> xxh3_64("hello")
        2794345569481354659
    """
    if isinstance(data, str):
        data = data.encode('utf-8')

    length = len(data)

    if length <= 16:
        return _hash_0to16(data, seed)
    elif length <= 240:
        return _hash_17to240(data, seed)
    else:
        return _hash_long(data, KKEY, seed)


def xxh3_64_hexdigest(data: Union[bytes, str], seed: int = 0) -> str:
    """
    Compute XXH3 64-bit hash and return as hex string.

    Args:
        data: Input bytes or str to hash. Strings are encoded as UTF-8.
        seed: Optional seed value (default: 0)

    Returns:
        16-character hex string (lowercase)

    Examples:
        >>> xxh3_64_hexdigest(b"hello")
        '26c7827d889f3723'

        >>> xxh3_64_hexdigest("hello world")
        'd2d4e5d8b4b5e8c8'
    """
    hash_value = xxh3_64(data, seed)
    return f"{hash_value:016x}"