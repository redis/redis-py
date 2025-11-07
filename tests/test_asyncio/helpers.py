import asyncio
import logging
from typing import Callable


async def wait_for_condition(
    predicate: Callable[[], bool],
    timeout: float = 0.2,
    check_interval: float = 0.01,
    error_message: str = "Timeout waiting for condition",
) -> None:
    """
    Poll a condition until it becomes True or timeout is reached.

    Args:
        predicate: A callable that returns True when the condition is met
        timeout: Maximum time to wait in seconds (default: 0.2s = 20 * 0.01s)
        check_interval: Time to sleep between checks in seconds (default: 0.01s)
        error_message: Error message to raise if timeout occurs

    Raises:
        AssertionError: If the condition is not met within the timeout period

    Example:
        # Wait for circuit breaker to open
        await wait_for_condition(
            lambda: cb2.state == CBState.OPEN,
            timeout=0.2,
            error_message="Timeout waiting for cb2 to open"
        )

        # Wait for failover strategy to select a specific database
        await wait_for_condition(
            lambda: client.command_executor.active_database is mock_db,
            timeout=0.2,
            error_message="Timeout waiting for active database to change"
        )
    """
    max_retries = int(timeout / check_interval)

    for attempt in range(max_retries):
        if predicate():
            logging.debug(f"Condition met after {attempt} attempts")
            return
        await asyncio.sleep(check_interval)

    raise AssertionError(error_message)
