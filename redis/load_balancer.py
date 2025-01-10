from typing import TYPE_CHECKING, List, Union

if TYPE_CHECKING:
    from redis.asyncio.cluster import ClusterNode as AsyncioClusterNode  # noqa: F401
    from redis.cluster import ClusterNode


class LoadBalancer:
    """
    Round-Robin Load Balancing
    """

    def __init__(self) -> None:
        self.primary_name_to_last_used_index: dict[str, int] = {}

    def get_node_from_slot(
        self,
        slot_nodes: Union[List["ClusterNode"], List["AsyncioClusterNode"]],
        read_from_replicas: bool,
    ) -> Union["ClusterNode", "AsyncioClusterNode"]:
        assert len(slot_nodes) > 0
        if not read_from_replicas:
            return slot_nodes[0]

        primary_name = slot_nodes[0].name
        node_idx = self.get_server_index(primary_name, len(slot_nodes))
        return slot_nodes[node_idx]

    def get_server_index(self, primary: str, list_size: int) -> int:
        # default to -1 if not found, so after incrementing it will be 0
        server_index = (
            self.primary_name_to_last_used_index.get(primary, -1) + 1
        ) % list_size
        # Update the index
        self.primary_name_to_last_used_index[primary] = server_index
        return server_index

    def reset(self) -> None:
        self.primary_name_to_last_used_index.clear()
