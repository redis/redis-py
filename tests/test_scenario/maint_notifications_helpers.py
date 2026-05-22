import logging
import time
from typing import Optional, Union

import pytest
from redis import RedisCluster

from redis.client import Redis
from redis.connection import Connection


class ClientValidations:
    @staticmethod
    def get_default_connection(redis_client: Union[Redis, RedisCluster]) -> Connection:
        """Get a random connection from the pool."""
        if isinstance(redis_client, RedisCluster):
            return redis_client.get_default_node().redis_connection.connection_pool.get_connection()
        if isinstance(redis_client, Redis):
            return redis_client.connection_pool.get_connection()
        raise ValueError(f"Unsupported redis client type: {type(redis_client)}")

    @staticmethod
    def release_connection(
        redis_client: Union[Redis, RedisCluster], connection: Connection
    ):
        """Release a connection back to the pool."""
        if isinstance(redis_client, RedisCluster):
            node_address = connection.host + ":" + str(connection.port)
            node = redis_client.get_node(node_address)
            if node is None:
                raise ValueError(
                    f"Node not found in cluster for address: {node_address}"
                )
            node.redis_connection.connection_pool.release(connection)
        elif isinstance(redis_client, Redis):
            redis_client.connection_pool.release(connection)
        else:
            raise ValueError(f"Unsupported redis client type: {type(redis_client)}")

    @staticmethod
    def wait_push_notification(
        redis_client: Union[Redis, RedisCluster],
        timeout: float = 120,
        fail_on_timeout: bool = True,
        connection: Optional[Connection] = None,
    ):
        """Wait for a push notification to be received."""
        start_time = time.time()  # returns the time in seconds
        check_interval = 0.2  # Check more frequently during operations
        test_conn = (
            connection
            if connection
            else ClientValidations.get_default_connection(redis_client)
        )
        logging.info(
            f"Waiting for push notification on connection: {test_conn}, "
            f"local socket port: {test_conn._sock.getsockname()[1] if test_conn._sock else None}"
        )

        try:
            while time.time() - start_time < timeout:
                try:
                    if test_conn.can_read(timeout=0.2):
                        # reading is important, it triggers the push notification
                        push_response = test_conn.read_response(push_request=True)
                        logging.debug(
                            f"Push notification has been received. Response: {push_response}"
                        )
                        if test_conn.should_reconnect():
                            logging.debug("Connection is marked for reconnect")
                        return
                except Exception as e:
                    logging.error(f"Error reading push notification: {e}")
                    break
                time.sleep(check_interval)
            if fail_on_timeout:
                pytest.fail(
                    f"Timeout waiting for push notification: waiting > {time.time() - start_time} seconds."
                )
        finally:
            # Release the connection back to the pool
            try:
                if not connection:
                    ClientValidations.release_connection(redis_client, test_conn)
            except Exception as e:
                logging.error(f"Error releasing connection: {e}")
