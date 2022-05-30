from binascii import crc_hqx

from redis.encoder import Encoder
from redis.typing import EncodableT

try:
    from redis.speedups import KeySlotter as _KeySlotter

    SPEEDUPS = True
except Exception:
    SPEEDUPS = False


__all__ = ["KeySlotter", "REDIS_CLUSTER_HASH_SLOTS", "SPEEDUPS"]


# Redis Cluster's key space is divided into 16384 slots.
# See: https://redis.io/docs/reference/cluster-spec/#key-distribution-model
REDIS_CLUSTER_HASH_SLOTS = 16384


class KeySlotter:
    __slots__ = "_key_slotter", "encoder", "encoding", "errors", "speedups"

    def __init__(self, encoder: Encoder, speedups: bool = True) -> None:
        self.encoder = encoder
        self.encoding = encoder.encoding
        self.errors = encoder.encoding_errors
        self.speedups = SPEEDUPS and speedups
        if self.speedups:
            self._key_slotter = _KeySlotter(self.encoding, self.errors)

    def key_slot(self, key: EncodableT) -> int:
        if self.speedups:
            try:
                return self._key_slotter.key_slot(key)
            except Exception:
                ...

        k = self.encoder.encode(key)
        start = k.find(b"{")
        if start > -1:
            end = k.find(b"}", start + 1)
            if end > -1 and end != start + 1:
                k = k[start + 1 : end]
        return crc_hqx(k, 0) % REDIS_CLUSTER_HASH_SLOTS
