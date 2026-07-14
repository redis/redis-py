import asyncio
import logging
import time
from typing import Optional

import pytest

from redis.asyncio import Redis
from redis.maint_notifications import MaintenanceState


class AsyncClientValidations:
    @staticmethod
    async def get_default_connection(redis_client: Redis):
        return await redis_client.connection_pool.get_connection()

    @staticmethod
    async def release_connection(redis_client: Redis, connection):
        await redis_client.connection_pool.release(connection)

    @staticmethod
    async def wait_push_notification(
        redis_client: Redis,
        timeout: float = 120,
        fail_on_timeout: bool = True,
        connection=None,
        expected_state: Optional[MaintenanceState] = None,
    ):
        """Wait for a push notification to be received."""
        start_time = time.time()
        check_interval = 0.2
        test_conn = (
            connection
            if connection
            else await AsyncClientValidations.get_default_connection(redis_client)
        )
        logging.info(
            f"Waiting for push notification on connection: {test_conn}, "
            f"peer: {test_conn.getpeername()}"
        )

        try:
            if (
                expected_state is not None
                and test_conn.maintenance_state == expected_state
            ):
                logging.debug(
                    f"Connection already in expected state {expected_state}, "
                    f"returning immediately"
                )
                return

            while time.time() - start_time < timeout:
                try:
                    if await test_conn.can_read():
                        push_response = await test_conn.read_response(push_request=True)
                        logging.debug(
                            f"Push notification received. Response: {push_response}"
                        )
                        if test_conn.should_reconnect():
                            logging.debug("Connection is marked for reconnect")
                        if (
                            expected_state is None
                            or test_conn.maintenance_state == expected_state
                        ):
                            return
                except Exception as e:
                    logging.error(f"Error reading push notification: {e}")
                    raise
                await asyncio.sleep(check_interval)
            if fail_on_timeout:
                pytest.fail(
                    f"Timeout waiting for push notification: "
                    f"waiting > {time.time() - start_time} seconds."
                )
        finally:
            try:
                if not connection:
                    await AsyncClientValidations.release_connection(
                        redis_client, test_conn
                    )
            except Exception as e:
                logging.error(f"Error releasing connection: {e}")
