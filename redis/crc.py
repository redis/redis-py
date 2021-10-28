from binascii import crc_hqx

# Redis Cluster's key space is divided into 16384 slots.
# For more information see: https://github.com/redis/redis/issues/2576
REDIS_CLUSTER_HASH_SLOTS = 16384

__all__ = [
    "crc16",
    "key_slot",
    "REDIS_CLUSTER_HASH_SLOTS"
]


def crc16(data):
    return crc_hqx(data, 0)


def key_slot(key, bucket=REDIS_CLUSTER_HASH_SLOTS):
    """Calculate key slot for a given key.
    :param key - bytes
    :param bucket - int
    """
    start = key.find(b"{")
    if start > -1:
        end = key.find(b"}", start + 1)
        if end > -1 and end != start + 1:
            key = key[start + 1: end]
    return crc16(key) % bucket
