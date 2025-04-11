from enum import Enum


class LoadBalancingStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    ROUND_ROBIN_REPLICAS = "round_robin_replicas"
    RANDOM_REPLICA = "random_replica"


class LoadBalancer:
    """
    Round-Robin Load Balancing
    """

    def __init__(self, start_index: int = 0) -> None:
        self.primary_to_idx = {}
        self.start_index = start_index

    def get_server_index(
        self,
        primary: str,
        list_size: int,
        load_balancing_strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN,
    ) -> int:
        if load_balancing_strategy == LoadBalancingStrategy.RANDOM_REPLICA:
            return self._get_random_replica_index(list_size)
        else:
            return self._get_round_robin_index(
                primary,
                list_size,
                load_balancing_strategy == LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
            )

    def reset(self) -> None:
        self.primary_to_idx.clear()

    def _get_random_replica_index(self, list_size: int) -> int:
        return random.randint(1, list_size - 1)

    def _get_round_robin_index(
        self, primary: str, list_size: int, replicas_only: bool
    ) -> int:
        server_index = self.primary_to_idx.setdefault(primary, self.start_index)
        if replicas_only and server_index == 0:
            # skip the primary node index
            server_index = 1
        # Update the index for the next round
        self.primary_to_idx[primary] = (server_index + 1) % list_size
        return server_index