from __future__ import annotations

import asyncio
import inspect
import re
import warnings
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
    TYPE_CHECKING,
)

from redis.asyncio.connection import (
    Connection,
    ConnectionPool,
    SSLConnection,
    UnixDomainSocketConnection,
)
from redis.client import AbstractRedis, bool_ok, MonitorCommandInfo
from redis.commands import (
    CoreCommands,
    RedisModuleCommands,
    SentinelCommands,
    list_or_args,
)
from redis.compat import Protocol
from redis.exceptions import (
    ConnectionError,
    ExecAbortError,
    PubSubError,
    RedisError,
    ResponseError,
    TimeoutError,
    WatchError,
)
from redis.lock import Lock
from redis.typing import ChannelT, EncodableT, KeyT
from redis.utils import safe_str, str_if_bytes

PubSubHandler = Callable[[Dict[str, str]], None]
_KeyT = TypeVar("_KeyT", bound=KeyT)
_ArgT = TypeVar("_ArgT", KeyT, EncodableT)
_RedisT = TypeVar("_RedisT", bound="Redis")
_NormalizeKeysT = TypeVar("_NormalizeKeysT", bound=Mapping[ChannelT, object])
if TYPE_CHECKING:
    from redis.commands.core import AsyncScript

SYM_EMPTY = b""
EMPTY_RESPONSE = "EMPTY_RESPONSE"

# some responses (ie. dump) are binary, and just meant to never be decoded
NEVER_DECODE = "NEVER_DECODE"


class ResponseCallbackProtocol(Protocol):
    def __call__(self, response: Any, **kwargs):
        ...


class AsyncResponseCallbackProtocol(Protocol):
    async def __call__(self, response: Any, **kwargs):
        ...


ResponseCallbackT = Union[ResponseCallbackProtocol, AsyncResponseCallbackProtocol]


class Redis(AbstractRedis, RedisModuleCommands, CoreCommands, SentinelCommands):
    """
    Implementation of the Redis protocol.

    This abstract class provides a Python interface to all Redis commands
    and an implementation of the Redis protocol.

    Pipelines derive from this, implementing how
    the commands are sent and received to the Redis server. Based on
    configuration, an instance will either use a ConnectionPool, or
    Connection object to talk to redis.
    """

    connection_pool_cls: type[ConnectionPool] = ConnectionPool
    connection_pool: ConnectionPool
    unix_domain_socket_connection_cls: type[UnixDomainSocketConnection] = UnixDomainSocketConnection
    ssl_connection_cls: type[SSLConnection] = SSLConnection
    pipeline_cls: type[Pipeline]
    connection: Connection | None

    def _init(self, single_connection_client: bool) -> None:
        self.pipeline_cls = Pipeline
        self.single_connection_client = single_connection_client

    def __await__(self):
        return self.initialize().__await__()

    async def initialize(self: _RedisT) -> _RedisT:
        if self.single_connection_client and self.connection is None:
            self.connection = await self.connection_pool.get_connection("_")
        return self

    async def transaction(
        self,
        func: Callable[["Pipeline"], Union[Any, Awaitable[Any]]],
        *watches: KeyT,
        shard_hint: Optional[str] = None,
        value_from_callable: bool = False,
        watch_delay: Optional[float] = None,
    ):
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single argument which is a Pipeline object.
        """
        pipe: Pipeline
        async with self.pipeline(True, shard_hint) as pipe:
            while True:
                try:
                    if watches:
                        await pipe.watch(*watches)
                    func_value = func(pipe)
                    if inspect.isawaitable(func_value):
                        func_value = await func_value
                    exec_value = await pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        await asyncio.sleep(watch_delay)
                    continue

    def lock(
        self,
        name: KeyT,
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking_timeout: float | None = None,
        lock_class: type[Lock] | None = None,
        thread_local: bool = True,
    ) -> Lock:
        """
        Return a new Lock object using key ``name`` that mimics
        the behavior of threading.Lock.

        TODO - asyncio Lock needs re-implementation

        If specified, ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``lock_class`` forces the specified lock implementation.

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. Consider the following timeline:

            time: 0, thread-1 acquires `my-lock`, with a timeout of 5 seconds.
                     thread-1 sets the token to "abc"
            time: 1, thread-2 blocks trying to acquire `my-lock` using the
                     Lock instance.
            time: 5, thread-1 has not yet completed. redis expires the lock
                     key.
            time: 5, thread-2 acquired `my-lock` now that it's available.
                     thread-2 sets the token to "xyz"
            time: 6, thread-1 finishes its work and calls release(). if the
                     token is *not* stored in thread local storage, then
                     thread-1 would see the token value as "xyz" and would be
                     able to successfully release the thread-2's lock.

        In some use cases it's necessary to disable thread local storage. For
        example, if you have code where one thread acquires a lock and passes
        that lock instance to a worker thread to release later. If thread
        local storage isn't disabled in this case, the worker thread won't see
        the token set by the thread that acquired the lock. Our assumption
        is that these cases aren't common and as such default to using
        thread local storage."""
        if lock_class is None:
            lock_class = Lock
        return lock_class(
            self,
            name,
            timeout=timeout,
            sleep=sleep,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    def pubsub(self, **kwargs) -> "PubSub":
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        them.
        """
        return PubSub(self.connection_pool, **kwargs)

    def monitor(self) -> "Monitor":
        return Monitor(self.connection_pool)

    def client(self) -> "Redis":
        return self.__class__(
            connection_pool=self.connection_pool, single_connection_client=True
        )

    async def __aenter__(self: _RedisT) -> _RedisT:
        return await self.initialize()

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    _DEL_MESSAGE = "Unclosed Redis client"

    def __del__(self, _warnings: Any = warnings) -> None:
        if self.connection is not None:
            _warnings.warn(
                f"Unclosed client session {self!r}",
                ResourceWarning,
                source=self,
            )
            context = {"client": self, "message": self._DEL_MESSAGE}
            asyncio.get_event_loop().call_exception_handler(context)

    async def close(self):
        conn = self.connection
        if conn:
            self.connection = None
            await self.connection_pool.release(conn)

    async def _send_command_parse_response(self, conn, command_name, *args, **options):
        """
        Send a command and parse the response
        """
        await conn.send_command(*args)
        return await self.parse_response(conn, command_name, **options)

    async def _disconnect_raise(self, conn: Connection, error: Exception):
        """
        Close the connection and raise an exception
        if retry_on_timeout is not set or the error
        is not a TimeoutError
        """
        await conn.disconnect()
        if not (conn.retry_on_timeout and isinstance(error, TimeoutError)):
            raise error

    # COMMAND EXECUTION AND PROTOCOL PARSING
    async def execute_command(self, *args, **options):
        """Execute a command and return a parsed response"""
        await self.initialize()
        pool = self.connection_pool
        command_name = args[0]
        conn = self.connection or await pool.get_connection(command_name, **options)

        try:
            return await conn.retry.call_with_retry(
                lambda: self._send_command_parse_response(
                    conn, command_name, *args, **options
                ),
                lambda error: self._disconnect_raise(conn, error),
            )
        finally:
            if not self.connection:
                await pool.release(conn)

    async def parse_response(
        self, connection: Connection, command_name: Union[str, bytes], **options
    ):
        """Parses a response from the Redis server"""
        try:
            if NEVER_DECODE in options:
                response = await connection.read_response(disable_encoding=True)
            else:
                response = await connection.read_response()
        except ResponseError:
            if EMPTY_RESPONSE in options:
                return options[EMPTY_RESPONSE]
            raise
        if command_name in self.response_callbacks:
            # Mypy bug: https://github.com/python/mypy/issues/10977
            command_name = cast(str, command_name)
            retval = self.response_callbacks[command_name](response, **options)
            return await retval if inspect.isawaitable(retval) else retval
        return response


StrictRedis = Redis


class Monitor:
    """
    Monitor is useful for handling the MONITOR command to the redis server.
    next_command() method returns one command from monitor
    listen() method yields commands from monitor.
    """

    monitor_re = re.compile(r"\[(\d+) (.*)\] (.*)")
    command_re = re.compile(r'"(.*?)(?<!\\)"')

    def __init__(self, connection_pool: ConnectionPool):
        self.connection_pool = connection_pool
        self.connection: Optional[Connection] = None

    async def connect(self):
        if self.connection is None:
            self.connection = await self.connection_pool.get_connection("MONITOR")

    async def __aenter__(self):
        await self.connect()
        await self.connection.send_command("MONITOR")
        # check that monitor returns 'OK', but don't return it to user
        response = await self.connection.read_response()
        if not bool_ok(response):
            raise RedisError(f"MONITOR failed: {response}")
        return self

    async def __aexit__(self, *args):
        await self.connection.disconnect()
        await self.connection_pool.release(self.connection)

    async def next_command(self) -> MonitorCommandInfo:
        """Parse the response from a monitor command"""
        await self.connect()
        response = await self.connection.read_response()
        if isinstance(response, bytes):
            response = self.connection.encoder.decode(response, force=True)
        command_time, command_data = response.split(" ", 1)
        m = self.monitor_re.match(command_data)
        db_id, client_info, command = m.groups()
        command = " ".join(self.command_re.findall(command))
        # Redis escapes double quotes because each piece of the command
        # string is surrounded by double quotes. We don't have that
        # requirement so remove the escaping and leave the quote.
        command = command.replace('\\"', '"')

        if client_info == "lua":
            client_address = "lua"
            client_port = ""
            client_type = "lua"
        elif client_info.startswith("unix"):
            client_address = "unix"
            client_port = client_info[5:]
            client_type = "unix"
        else:
            # use rsplit as ipv6 addresses contain colons
            client_address, client_port = client_info.rsplit(":", 1)
            client_type = "tcp"
        return {
            "time": float(command_time),
            "db": int(db_id),
            "client_address": client_address,
            "client_port": client_port,
            "client_type": client_type,
            "command": command,
        }

    async def listen(self) -> AsyncIterator[MonitorCommandInfo]:
        """Listen for commands coming to the server."""
        while True:
            yield await self.next_command()


class PubSub:
    """
    PubSub provides publish, subscribe and listen support to Redis channels.

    After subscribing to one or more channels, the listen() method will block
    until a message arrives on one of the subscribed channels. That message
    will be returned and it's safe to start listening again.
    """

    PUBLISH_MESSAGE_TYPES = ("message", "pmessage")
    UNSUBSCRIBE_MESSAGE_TYPES = ("unsubscribe", "punsubscribe")
    HEALTH_CHECK_MESSAGE = "redis-py-health-check"

    def __init__(
        self,
        connection_pool: ConnectionPool,
        shard_hint: Optional[str] = None,
        ignore_subscribe_messages: bool = False,
        encoder=None,
    ):
        self.connection_pool = connection_pool
        self.shard_hint = shard_hint
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.connection = None
        # we need to know the encoding options for this connection in order
        # to lookup channel and pattern names for callback handlers.
        self.encoder = encoder
        if self.encoder is None:
            self.encoder = self.connection_pool.get_encoder()
        if self.encoder.decode_responses:
            self.health_check_response: Iterable[Union[str, bytes]] = [
                "pong",
                self.HEALTH_CHECK_MESSAGE,
            ]
        else:
            self.health_check_response = [
                b"pong",
                self.encoder.encode(self.HEALTH_CHECK_MESSAGE),
            ]
        self.channels = {}
        self.pending_unsubscribe_channels = set()
        self.patterns = {}
        self.pending_unsubscribe_patterns = set()
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.reset()

    def __del__(self):
        if self.connection:
            self.connection.clear_connect_callbacks()

    async def reset(self):
        async with self._lock:
            if self.connection:
                await self.connection.disconnect()
                self.connection.clear_connect_callbacks()
                await self.connection_pool.release(self.connection)
                self.connection = None
            self.channels = {}
            self.pending_unsubscribe_channels = set()
            self.patterns = {}
            self.pending_unsubscribe_patterns = set()

    def close(self) -> Awaitable[NoReturn]:
        return self.reset()

    async def on_connect(self, connection: Connection):
        """Re-subscribe to any channels and patterns previously subscribed to"""
        # NOTE: for python3, we can't pass bytestrings as keyword arguments
        # so we need to decode channel/pattern names back to unicode strings
        # before passing them to [p]subscribe.
        self.pending_unsubscribe_channels.clear()
        self.pending_unsubscribe_patterns.clear()
        if self.channels:
            channels = {}
            for k, v in self.channels.items():
                channels[self.encoder.decode(k, force=True)] = v
            await self.subscribe(**channels)
        if self.patterns:
            patterns = {}
            for k, v in self.patterns.items():
                patterns[self.encoder.decode(k, force=True)] = v
            await self.psubscribe(**patterns)

    @property
    def subscribed(self):
        """Indicates if there are subscriptions to any channels or patterns"""
        return bool(self.channels or self.patterns)

    async def execute_command(self, *args: EncodableT):
        """Execute a publish/subscribe command"""

        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        if self.connection is None:
            self.connection = await self.connection_pool.get_connection(
                "pubsub", self.shard_hint
            )
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            self.connection.register_connect_callback(self.on_connect)
        connection = self.connection
        kwargs = {"check_health": not self.subscribed}
        await self._execute(connection, connection.send_command, *args, **kwargs)

    async def _disconnect_raise_connect(self, conn, error):
        """
        Close the connection and raise an exception
        if retry_on_timeout is not set or the error
        is not a TimeoutError. Otherwise, try to reconnect
        """
        await conn.disconnect()
        if not (conn.retry_on_timeout and isinstance(error, TimeoutError)):
            raise error
        await conn.connect()

    async def _execute(self, conn, command, *args, **kwargs):
        """
        Connect manually upon disconnection. If the Redis server is down,
        this will fail and raise a ConnectionError as desired.
        After reconnection, the ``on_connect`` callback should have been
        called by the # connection to resubscribe us to any channels and
        patterns we were previously listening to
        """
        return await conn.retry.call_with_retry(
            lambda: command(*args, **kwargs),
            lambda error: self._disconnect_raise_connect(conn, error),
        )

    async def parse_response(self, block: bool = True, timeout: float = 0):
        """Parse the response from a publish/subscribe command"""
        conn = self.connection
        if conn is None:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
            )

        await self.check_health()

        if not block and not await self._execute(conn, conn.can_read, timeout=timeout):
            return None
        response = await self._execute(conn, conn.read_response)

        if conn.health_check_interval and response == self.health_check_response:
            # ignore the health check message as user might not expect it
            return None
        return response

    async def check_health(self):
        conn = self.connection
        if conn is None:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
            )

        if (
            conn.health_check_interval
            and asyncio.get_event_loop().time() > conn.next_health_check
        ):
            await conn.send_command(
                "PING", self.HEALTH_CHECK_MESSAGE, check_health=False
            )

    def _normalize_keys(self, data: _NormalizeKeysT) -> _NormalizeKeysT:
        """
        normalize channel/pattern names to be either bytes or strings
        based on whether responses are automatically decoded. this saves us
        from coercing the value for each message coming in.
        """
        encode = self.encoder.encode
        decode = self.encoder.decode
        return {decode(encode(k)): v for k, v in data.items()}  # type: ignore[return-value]

    async def psubscribe(self, *args: ChannelT, **kwargs: PubSubHandler):
        """
        Subscribe to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern rather than producing a message via
        ``listen()``.
        """
        parsed_args = list_or_args((args[0],), args[1:]) if args else args
        new_patterns: Dict[ChannelT, PubSubHandler] = dict.fromkeys(parsed_args)
        # Mypy bug: https://github.com/python/mypy/issues/10970
        new_patterns.update(kwargs)  # type: ignore[arg-type]
        ret_val = await self.execute_command("PSUBSCRIBE", *new_patterns.keys())
        # update the patterns dict AFTER we send the command. we don't want to
        # subscribe twice to these patterns, once for the command and again
        # for the reconnection.
        new_patterns = self._normalize_keys(new_patterns)
        self.patterns.update(new_patterns)
        self.pending_unsubscribe_patterns.difference_update(new_patterns)
        return ret_val

    def punsubscribe(self, *args: ChannelT) -> Awaitable:
        """
        Unsubscribe from the supplied patterns. If empty, unsubscribe from
        all patterns.
        """
        patterns: Iterable[ChannelT]
        if args:
            parsed_args = list_or_args((args[0],), args[1:])
            patterns = self._normalize_keys(dict.fromkeys(parsed_args)).keys()
        else:
            parsed_args = []
            patterns = self.patterns
        self.pending_unsubscribe_patterns.update(patterns)
        return self.execute_command("PUNSUBSCRIBE", *parsed_args)

    async def subscribe(self, *args: ChannelT, **kwargs: Callable):
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """
        parsed_args = list_or_args((args[0],), args[1:]) if args else ()
        new_channels = dict.fromkeys(parsed_args)
        # Mypy bug: https://github.com/python/mypy/issues/10970
        new_channels.update(kwargs)  # type: ignore[arg-type]
        ret_val = await self.execute_command("SUBSCRIBE", *new_channels.keys())
        # update the channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        new_channels = self._normalize_keys(new_channels)
        self.channels.update(new_channels)
        self.pending_unsubscribe_channels.difference_update(new_channels)
        return ret_val

    def unsubscribe(self, *args) -> Awaitable:
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        if args:
            parsed_args = list_or_args(args[0], args[1:])
            channels = self._normalize_keys(dict.fromkeys(parsed_args))
        else:
            parsed_args = []
            channels = self.channels
        self.pending_unsubscribe_channels.update(channels)
        return self.execute_command("UNSUBSCRIBE", *parsed_args)

    async def listen(self) -> AsyncIterator:
        """Listen for messages on channels this client has been subscribed to"""
        while self.subscribed:
            response = self.handle_message(await self.parse_response(block=True))
            if response is not None:
                yield response

    async def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float = 0.0
    ):
        """
        Get the next message if one is available, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number.
        """
        response = await self.parse_response(block=False, timeout=timeout)
        if response:
            return self.handle_message(response, ignore_subscribe_messages)
        return None

    def ping(self, message=None) -> Awaitable:
        """
        Ping the Redis server
        """
        message = "" if message is None else message
        return self.execute_command("PING", message)

    def handle_message(self, response, ignore_subscribe_messages=False):
        """
        Parses a pub/sub message. If the channel or pattern was subscribed to
        with a message handler, the handler is invoked instead of a parsed
        message being returned.
        """
        message_type = str_if_bytes(response[0])
        if message_type == "pmessage":
            message = {
                "type": message_type,
                "pattern": response[1],
                "channel": response[2],
                "data": response[3],
            }
        elif message_type == "pong":
            message = {
                "type": message_type,
                "pattern": None,
                "channel": None,
                "data": response[1],
            }
        else:
            message = {
                "type": message_type,
                "pattern": None,
                "channel": response[1],
                "data": response[2],
            }

        # if this is an unsubscribe message, remove it from memory
        if message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            if message_type == "punsubscribe":
                pattern = response[1]
                if pattern in self.pending_unsubscribe_patterns:
                    self.pending_unsubscribe_patterns.remove(pattern)
                    self.patterns.pop(pattern, None)
            else:
                channel = response[1]
                if channel in self.pending_unsubscribe_channels:
                    self.pending_unsubscribe_channels.remove(channel)
                    self.channels.pop(channel, None)

        if message_type in self.PUBLISH_MESSAGE_TYPES:
            # if there's a message handler, invoke it
            if message_type == "pmessage":
                handler = self.patterns.get(message["pattern"], None)
            else:
                handler = self.channels.get(message["channel"], None)
            if handler:
                handler(message)
                return None
        elif message_type != "pong":
            # this is a subscribe/unsubscribe message. ignore if we don't
            # want them
            if ignore_subscribe_messages or self.ignore_subscribe_messages:
                return None

        return message

    async def run(
        self,
        *,
        exception_handler: Optional["PSWorkerThreadExcHandlerT"] = None,
        poll_timeout: float = 1.0,
    ) -> None:
        """Process pub/sub messages using registered callbacks.

        This is the equivalent of :py:meth:`redis.PubSub.run_in_thread` in
        redis-py, but it is a coroutine. To launch it as a separate task, use
        ``asyncio.create_task``:

            >>> task = asyncio.create_task(pubsub.run())

        To shut it down, use asyncio cancellation:

            >>> task.cancel()
            >>> await task
        """
        for channel, handler in self.channels.items():
            if handler is None:
                raise PubSubError(f"Channel: '{channel}' has no handler registered")
        for pattern, handler in self.patterns.items():
            if handler is None:
                raise PubSubError(f"Pattern: '{pattern}' has no handler registered")

        while True:
            try:
                await self.get_message(
                    ignore_subscribe_messages=True, timeout=poll_timeout
                )
            except asyncio.CancelledError:
                raise
            except BaseException as e:
                if exception_handler is None:
                    raise
                res = exception_handler(e, self)
                if inspect.isawaitable(res):
                    await res
            # Ensure that other tasks on the event loop get a chance to run
            # if we didn't have to block for I/O anywhere.
            await asyncio.sleep(0)


class PubsubWorkerExceptionHandler(Protocol):
    def __call__(self, e: BaseException, pubsub: PubSub):
        ...


class AsyncPubsubWorkerExceptionHandler(Protocol):
    async def __call__(self, e: BaseException, pubsub: PubSub):
        ...


PSWorkerThreadExcHandlerT = Union[
    PubsubWorkerExceptionHandler, AsyncPubsubWorkerExceptionHandler
]


CommandT = Tuple[Tuple[Union[str, bytes], ...], Mapping[str, Any]]
CommandStackT = List[CommandT]


class Pipeline(Redis):  # lgtm [py/init-calls-subclass]
    """
    Pipelines provide a way to transmit multiple commands to the Redis server
    in one transmission.  This is convenient for batch processing, such as
    saving all the values in a list to Redis.

    All commands executed within a pipeline are wrapped with MULTI and EXEC
    calls. This guarantees all commands executed in the pipeline will be
    executed atomically.

    Any command raising an exception does *not* halt the execution of
    subsequent commands in the pipeline. Instead, the exception is caught
    and its instance is placed into the response list returned by execute().
    Code iterating over the response list should be able to deal with an
    instance of an exception as a potential value. In general, these will be
    ResponseError exceptions, such as those raised when issuing a command
    on a key of a different datatype.
    """

    UNWATCH_COMMANDS = {"DISCARD", "EXEC", "UNWATCH"}

    def __init__(
        self,
        connection_pool: ConnectionPool,
        response_callbacks: MutableMapping[Union[str, bytes], ResponseCallbackT],
        transaction: bool,
        shard_hint: Optional[str],
    ):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        self.is_transaction = transaction
        self.shard_hint = shard_hint
        self.watching = False
        self.command_stack: CommandStackT = []
        self.scripts: Set["AsyncScript"] = set()
        self.explicit_transaction = False

    async def __aenter__(self: _RedisT) -> _RedisT:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.reset()

    def __await__(self):
        return self._async_self().__await__()

    _DEL_MESSAGE = "Unclosed Pipeline client"

    def __len__(self):
        return len(self.command_stack)

    def __bool__(self):
        """Pipeline instances should always evaluate to True"""
        return True

    async def _async_self(self):
        return self

    async def reset(self):
        self.command_stack = []
        self.scripts = set()
        # make sure to reset the connection state in the event that we were
        # watching something
        if self.watching and self.connection:
            try:
                # call this manually since our unwatch or
                # immediate_execute_command methods can call reset()
                await self.connection.send_command("UNWATCH")
                await self.connection.read_response()
            except ConnectionError:
                # disconnect will also remove any previous WATCHes
                if self.connection:
                    await self.connection.disconnect()
        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False
        # we can safely return the connection to the pool here since we're
        # sure we're no longer WATCHing anything
        if self.connection:
            await self.connection_pool.release(self.connection)
            self.connection = None

    def multi(self):
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        if self.explicit_transaction:
            raise RedisError("Cannot issue nested calls to MULTI")
        if self.command_stack:
            raise RedisError(
                "Commands without an initial WATCH have already " "been issued"
            )
        self.explicit_transaction = True

    def execute_command(
        self, *args, **kwargs
    ) -> Union["Pipeline", Awaitable["Pipeline"]]:
        if (self.watching or args[0] == "WATCH") and not self.explicit_transaction:
            return self.immediate_execute_command(*args, **kwargs)
        return self.pipeline_execute_command(*args, **kwargs)

    async def _disconnect_reset_raise(self, conn, error):
        """
        Close the connection, reset watching state and
        raise an exception if we were watching,
        retry_on_timeout is not set,
        or the error is not a TimeoutError
        """
        await conn.disconnect()
        # if we were already watching a variable, the watch is no longer
        # valid since this connection has died. raise a WatchError, which
        # indicates the user should retry this transaction.
        if self.watching:
            await self.reset()
            raise WatchError(
                "A ConnectionError occurred on while " "watching one or more keys"
            )
        # if retry_on_timeout is not set, or the error is not
        # a TimeoutError, raise it
        if not (conn.retry_on_timeout and isinstance(error, TimeoutError)):
            await self.reset()
            raise

    async def immediate_execute_command(self, *args, **options):
        """
        Execute a command immediately, but don't auto-retry on a
        ConnectionError if we're already WATCHing a variable. Used when
        issuing WATCH or subsequent commands retrieving their values but before
        MULTI is called.
        """
        command_name = args[0]
        conn = self.connection
        # if this is the first call, we need a connection
        if not conn:
            conn = await self.connection_pool.get_connection(
                command_name, self.shard_hint
            )
            self.connection = conn

        return await conn.retry.call_with_retry(
            lambda: self._send_command_parse_response(
                conn, command_name, *args, **options
            ),
            lambda error: self._disconnect_reset_raise(conn, error),
        )

    def pipeline_execute_command(self, *args, **options):
        """
        Stage a command to be executed when execute() is next called

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.
        """
        self.command_stack.append((args, options))
        return self

    async def _execute_transaction(  # noqa: C901
        self, connection: Connection, commands: CommandStackT, raise_on_error
    ):
        pre: CommandT = (("MULTI",), {})
        post: CommandT = (("EXEC",), {})
        cmds = (pre, *commands, post)
        all_cmds = connection.pack_commands(
            args for args, options in cmds if EMPTY_RESPONSE not in options
        )
        await connection.send_packed_command(all_cmds)
        errors = []

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            await self.parse_response(connection, "_")
        except ResponseError as err:
            errors.append((0, err))

        # and all the other commands
        for i, command in enumerate(commands):
            if EMPTY_RESPONSE in command[1]:
                errors.append((i, command[1][EMPTY_RESPONSE]))
            else:
                try:
                    await self.parse_response(connection, "_")
                except ResponseError as err:
                    self.annotate_exception(err, i + 1, command[0])
                    errors.append((i, err))

        # parse the EXEC.
        try:
            response = await self.parse_response(connection, "_")
        except ExecAbortError as err:
            if errors:
                raise errors[0][1] from err
            raise

        # EXEC clears any watched keys
        self.watching = False

        if response is None:
            raise WatchError("Watched variable changed.") from None

        # put any parse errors into the response
        for i, e in errors:
            response.insert(i, e)

        if len(response) != len(commands):
            if self.connection:
                await self.connection.disconnect()
            raise ResponseError(
                "Wrong number of response items from pipeline execution"
            ) from None

        # find any errors in the response and raise if necessary
        if raise_on_error:
            self.raise_first_error(commands, response)

        # We have to run response callbacks manually
        data = []
        for r, cmd in zip(response, commands):
            if not isinstance(r, Exception):
                args, options = cmd
                command_name = args[0]
                if command_name in self.response_callbacks:
                    r = self.response_callbacks[command_name](r, **options)
                    if inspect.isawaitable(r):
                        r = await r
            data.append(r)
        return data

    async def _execute_pipeline(
        self, connection: Connection, commands: CommandStackT, raise_on_error: bool
    ):
        # build up all commands into a single request to increase network perf
        all_cmds = connection.pack_commands([args for args, _ in commands])
        await connection.send_packed_command(all_cmds)

        response = []
        for args, options in commands:
            try:
                response.append(
                    await self.parse_response(connection, args[0], **options)
                )
            except ResponseError as e:
                response.append(e)

        if raise_on_error:
            self.raise_first_error(commands, response)
        return response

    def raise_first_error(self, commands: CommandStackT, response: Iterable[Any]):
        for i, r in enumerate(response):
            if isinstance(r, ResponseError):
                self.annotate_exception(r, i + 1, commands[i][0])
                raise r

    def annotate_exception(
        self, exception: Exception, number: int, command: Iterable[object]
    ) -> None:
        cmd = " ".join(map(safe_str, command))
        msg = f"Command # {number} ({cmd}) of pipeline caused error: {exception.args}"
        exception.args = (msg,) + exception.args[1:]

    async def parse_response(
        self, connection: Connection, command_name: Union[str, bytes], **options
    ):
        result = await super().parse_response(connection, command_name, **options)
        if command_name in self.UNWATCH_COMMANDS:
            self.watching = False
        elif command_name == "WATCH":
            self.watching = True
        return result

    async def load_scripts(self):
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        # we can't use the normal script_* methods because they would just
        # get buffered in the pipeline.
        exists = await immediate("SCRIPT EXISTS", *shas)
        if not all(exists):
            for s, exist in zip(scripts, exists):
                if not exist:
                    s.sha = await immediate("SCRIPT LOAD", s.script)

    async def _disconnect_raise_reset(self, conn: Connection, error: Exception):
        """
        Close the connection, raise an exception if we were watching,
        and raise an exception if retry_on_timeout is not set,
        or the error is not a TimeoutError
        """
        await conn.disconnect()
        # if we were watching a variable, the watch is no longer valid
        # since this connection has died. raise a WatchError, which
        # indicates the user should retry this transaction.
        if self.watching:
            raise WatchError(
                "A ConnectionError occurred on while " "watching one or more keys"
            )
        # if retry_on_timeout is not set, or the error is not
        # a TimeoutError, raise it
        if not (conn.retry_on_timeout and isinstance(error, TimeoutError)):
            await self.reset()
            raise

    async def execute(self, raise_on_error: bool = True):
        """Execute all the commands in the current pipeline"""
        stack = self.command_stack
        if not stack and not self.watching:
            return []
        if self.scripts:
            await self.load_scripts()
        if self.is_transaction or self.explicit_transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline

        conn = self.connection
        if not conn:
            conn = await self.connection_pool.get_connection("MULTI", self.shard_hint)
            # assign to self.connection so reset() releases the connection
            # back to the pool after we're done
            self.connection = conn
        conn = cast(Connection, conn)

        try:
            return await conn.retry.call_with_retry(
                lambda: execute(conn, stack, raise_on_error),
                lambda error: self._disconnect_raise_reset(conn, error),
            )
        finally:
            await self.reset()

    async def discard(self):
        """Flushes all previously queued commands
        See: https://redis.io/commands/DISCARD
        """
        await self.execute_command("DISCARD")

    async def watch(self, *names: KeyT):
        """Watches the values at keys ``names``"""
        if self.explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")
        return await self.execute_command("WATCH", *names)

    async def unwatch(self):
        """Unwatches all previously specified keys"""
        return self.watching and await self.execute_command("UNWATCH") or True
