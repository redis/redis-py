"""Pluggable cluster topology discovery.

``NodesManager`` uses a topology provider to learn which nodes own which slots.
A provider names the command to issue and knows how to read its reply; issuing
the command and building the node caches stay with ``NodesManager``.
"""

from abc import ABC, abstractmethod
from typing import Any

from redis.utils import str_if_bytes

SlotOwner = tuple[str, int]
SlotOwners = tuple[int, int, SlotOwner, list[SlotOwner]]

_MASTER_ROLE = "master"
_ONLINE_HEALTH = "online"


def _as_str(value: Any) -> str:
    return str_if_bytes(value) if isinstance(value, bytes) else value


def _as_mapping(entry: Any) -> dict[str, Any]:
    """Normalize a RESP3 map or a RESP2 flat ``[key, value, ...]`` array."""
    if isinstance(entry, dict):
        return {_as_str(key): value for key, value in entry.items()}
    return {_as_str(entry[i]): entry[i + 1] for i in range(0, len(entry) - 1, 2)}


def _slot_ranges(slots: Any) -> list[tuple[int, int]]:
    """Read a shard's slot ranges, which arrive either flat or already paired."""
    if not slots:
        return []
    if isinstance(slots[0], (list, tuple)):
        return [(int(start), int(end)) for start, end in slots]
    return [(int(slots[i]), int(slots[i + 1])) for i in range(0, len(slots) - 1, 2)]


def _node_address(node: dict[str, Any], prefer_tls_port: bool) -> SlotOwner:
    # A null or empty endpoint means the node's address is unknown to itself
    # (typically because it sits behind a load balancer); the caller must reach
    # it at the host the topology command was sent to, so leave the host empty
    # rather than substituting ``ip``, which is the unreachable internal address.
    endpoint = node.get("endpoint")
    host = "" if endpoint is None else _as_str(endpoint)

    port_keys = ("tls-port", "port") if prefer_tls_port else ("port", "tls-port")
    port = next(
        (node[key] for key in port_keys if node.get(key) is not None),
        None,
    )
    return host, int(port)


def parse_cluster_slots_topology(response: Any) -> list[SlotOwners]:
    """Read a ``CLUSTER SLOTS`` reply into per-range slot ownership."""
    topology = []
    for slot in response:
        start, end = int(slot[0]), int(slot[1])
        primary = (_as_str(slot[2][0]), int(slot[2][1]))
        replicas = [(_as_str(node[0]), int(node[1])) for node in slot[3:]]
        topology.append((start, end, primary, replicas))
    return topology


def parse_cluster_shards_topology(
    response: Any, prefer_tls_port: bool = False
) -> list[SlotOwners]:
    """Read a ``CLUSTER SHARDS`` reply into per-range slot ownership.

    A shard covers any number of slot ranges and lists its nodes in no
    guaranteed order, so the primary is selected by role rather than position.
    """
    topology = []
    for shard_entry in response:
        shard = _as_mapping(shard_entry)
        ranges = _slot_ranges(shard.get("slots"))
        if not ranges:
            continue

        primary = None
        replicas = []
        for node_entry in shard.get("nodes", []):
            node = _as_mapping(node_entry)
            role = _as_str(node.get("role", ""))
            if role == _MASTER_ROLE:
                if primary is None:
                    primary = _node_address(node, prefer_tls_port)
            # An unhealthy primary still owns its slots, so only replicas are
            # dropped on health; dropping the primary would leave them uncovered.
            elif _as_str(node.get("health", _ONLINE_HEALTH)) == _ONLINE_HEALTH:
                replicas.append(_node_address(node, prefer_tls_port))

        if primary is None:
            continue

        topology.extend((start, end, primary, replicas) for start, end in ranges)
    return topology


class ClusterTopologyProvider(ABC):
    """Names a topology command and reads its reply."""

    command: tuple[str, ...]

    @abstractmethod
    def parse(self, response: Any) -> list[SlotOwners]:
        """
        Reads a topology command reply into per-range slot ownership.

        Args:
            response: The reply to the provider's ``command``.

        Returns:
            One ``(start, end, primary, replicas)`` tuple per slot range.
        """
        pass


class AsyncClusterTopologyProvider(ABC):
    """Names a topology command and reads its reply."""

    command: tuple[str, ...]

    @abstractmethod
    def parse(self, response: Any) -> list[SlotOwners]:
        """
        Reads a topology command reply into per-range slot ownership.

        Args:
            response: The reply to the provider's ``command``.

        Returns:
            One ``(start, end, primary, replicas)`` tuple per slot range.
        """
        pass


class ClusterSlotsTopologyProvider(ClusterTopologyProvider):
    """Discovers topology with ``CLUSTER SLOTS``."""

    command = ("CLUSTER SLOTS",)

    def parse(self, response: Any) -> list[SlotOwners]:
        return parse_cluster_slots_topology(response)


class ClusterShardsTopologyProvider(ClusterTopologyProvider):
    """Discovers topology with ``CLUSTER SHARDS``, which requires Redis 7.0+.

    One reply entry per shard rather than per slot range, so replies stay small
    on clusters whose slot ranges have become fragmented.

    Args:
        prefer_tls_port: Take each node's ``tls-port`` in preference to ``port``.
    """

    command = ("CLUSTER SHARDS",)

    def __init__(self, prefer_tls_port: bool = False) -> None:
        self.prefer_tls_port = prefer_tls_port

    def parse(self, response: Any) -> list[SlotOwners]:
        return parse_cluster_shards_topology(response, self.prefer_tls_port)


class AsyncClusterSlotsTopologyProvider(
    ClusterSlotsTopologyProvider, AsyncClusterTopologyProvider
):
    """Discovers topology with ``CLUSTER SLOTS``."""


class AsyncClusterShardsTopologyProvider(
    ClusterShardsTopologyProvider, AsyncClusterTopologyProvider
):
    """Discovers topology with ``CLUSTER SHARDS``, which requires Redis 7.0+."""
