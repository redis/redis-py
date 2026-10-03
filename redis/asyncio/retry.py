import inspect
import warnings
from asyncio import sleep
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from redis.exceptions import ConnectionError, TimeoutError
from redis.retry import AbstractRetry

T = TypeVar("T")

if TYPE_CHECKING:
    from redis.backoff import AbstractBackoff


class Retry(AbstractRetry[Exception]):
    __hash__ = AbstractRetry.__hash__

    def __init__(
        self,
        backoff: "AbstractBackoff",
        retries: int,
        supported_errors: Tuple[Type[Exception], ...] = (
            ConnectionError,
            TimeoutError,
        ),
    ):
        super().__init__(backoff, retries, supported_errors)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Retry):
            return NotImplemented

        return (
            self._backoff == other._backoff
            and self._retries == other._retries
            and set(self._supported_errors) == set(other._supported_errors)
        )

    async def call_with_retry(
        self,
        do: Callable[[], Awaitable[T]],
        fail: Union[
            Callable[[Exception], Any],
            Callable[[Exception, int], Any],
        ],
        is_retryable: Optional[Callable[[Exception], bool]] = None,
        with_failure_count: bool = False,
    ) -> T:
        """
        Execute an operation that might fail and returns its result, or
        raise the exception that was thrown depending on the `Backoff` object.
        `do`: the operation to call. Expects no argument.
        `fail`: the failure handler, expects the last error that was thrown
        ``is_retryable``: optional function to determine if an error is retryable
        ``with_failure_count``: if True, the failure count is passed to the failure handler
        """
        self._backoff.reset()
        failures = 0
        while True:
            try:
                return await do()
            except self._supported_errors as error:
                if is_retryable and not is_retryable(error):
                    raise
                failures += 1

                if with_failure_count:
                    await fail(error, failures)
                else:
                    await fail(error)

                if self._retries >= 0 and failures > self._retries:
                    raise error
                backoff = self._backoff.compute(failures)
                if backoff > 0:
                    await sleep(backoff)


def ensure_async_retry(
    retry: AbstractRetry | Retry | None,
) -> AbstractRetry | Retry | None:
    """Return an async ``Retry`` for use with ``redis.asyncio`` clients.

    Passing the sync ``redis.retry.Retry`` to an asyncio client is an easy
    mistake (same class name) and degrades every retry seam to a single
    attempt with no failure callback, because the sync ``call_with_retry``
    is a plain ``def`` that returns the coroutine without awaiting it.
    Rebuild such objects as ``redis.asyncio.retry.Retry`` so the caller's
    backoff / retries / supported errors are preserved.
    """
    if retry is None or isinstance(retry, Retry):
        return retry
    if isinstance(retry, AbstractRetry) and not inspect.iscoroutinefunction(
        retry.call_with_retry
    ):
        warnings.warn(
            "Passing redis.retry.Retry to redis.asyncio clients is deprecated "
            "and was converted to redis.asyncio.retry.Retry. "
            "Use redis.asyncio.retry.Retry with redis.asyncio clients.",
            UserWarning,
            stacklevel=3,
        )
        return Retry(
            backoff=retry._backoff,
            retries=retry._retries,
            supported_errors=tuple(retry._supported_errors),
        )
    return retry
