import copy
import re
import threading
import time
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Type,
    Union,
)

from redis._parsers.encoders import Encoder
from redis._parsers.helpers import (
    _RedisCallbacks,
    _RedisCallbacksRESP2,
    _RedisCallbacksRESP3,
    bool_ok,
)
from redis.backoff import ExponentialWithJitterBackoff
from redis.cache import CacheConfig, CacheInterface
from redis.commands import (
    CoreCommands,
    RedisModuleCommands,
    SentinelCommands,
    list_or_args,
)
from redis.commands.core import Script
from redis.connection import (
    AbstractConnection,
    Connection,
    ConnectionPool,
    SSLConnection,
    UnixDomainSocketConnection,
)
from redis.credentials import CredentialProvider
from redis.event import (
    AfterPooledConnectionsInstantiationEvent,
    AfterPubSubConnectionInstantiationEvent,
    AfterSingleConnectionInstantiationEvent,
    ClientType,
    EventDispatcher,
)
from redis.exceptions import (
    ConnectionError,
    ExecAbortError,
    PubSubError,
    RedisError,
    ResponseError,
    WatchError,
)
from redis.lock import Lock
from redis.retry import Retry
from redis.utils import (
    _set_info_logger,
    deprecated_args,
    get_lib_version,
    safe_str,
    str_if_bytes,
    truncate_text,
)

if TYPE_CHECKING:
    import ssl

    import OpenSSL

SYM_EMPTY = b""
EMPTY_RESPONSE = "EMPTY_RESPONSE"

# some responses (ie. dump) are binary, and just meant to never be decoded
NEVER_DECODE = "NEVER_DECODE"


class CaseInsensitiveDict(dict):
    "Case insensitive dict implementation. Assumes string keys only."

    def __init__(self, data: Dict[str, str]) -> None:
        for k, v in data.items():
            self[k.upper()] = v

    def __contains__(self, k):
        return super().__contains__(k.upper())

    def __delitem__(self, k):
        super().__delitem__(k.upper())

    def __getitem__(self, k):
        return super().__getitem__(k.upper())

    def get(self, k, default=None):
        return super().get(k.upper(), default)

    def __setitem__(self, k, v):
        super().__setitem__(k.upper(), v)

    def update(self, data):
        data = CaseInsensitiveDict(data)
        super().update(data)


class AbstractRedis:
    pass


class Redis(RedisModuleCommands, CoreCommands, SentinelCommands):
    """
    Implementation of the Redis protocol.

    This abstract class provides a Python interface to all Redis commands
    and an implementation of the Redis protocol.

    Pipelines derive from this, implementing how
    the commands are sent and received to the Redis server. Based on
    configuration, an instance will either use a ConnectionPool, or
    Connection object to talk to redis.

    It is not safe to pass PubSub or Pipeline objects between threads.
    """

    @classmethod
    def from_url(cls, url: str, **kwargs) -> "Redis":
        """
        Return a Redis client object configured from the given URL

        For example::

            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[username@]/path/to/socket.sock?db=0[&password=password]

        Three URL schemes are supported:

        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>
        - ``unix://``: creates a Unix Domain Socket connection.

        The username, password, hostname, path and all querystring values
        are passed through urllib.parse.unquote in order to replace any
        percent-encoded values with their corresponding characters.

        There are several ways to specify a database number. The first value
        found will be used:

            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// or rediss:// schemes, the path argument
               of the url, e.g. redis://localhost/0
            3. A ``db`` keyword argument to this function.

        If none of these options are specified, the default db=0 is used.

        All querystring options are cast to their appropriate Python types.
        Boolean arguments can be specified with string values "True"/"False"
        or "Yes"/"No". Values that cannot be properly cast cause a
        ``ValueError`` to be raised. Once parsed, the querystring arguments
        and keyword arguments are passed to the ``ConnectionPool``'s
        class initializer. In the case of conflicting arguments, querystring
        arguments always win.

        """
        single_connection_client = kwargs.pop("single_connection_client", False)
        connection_pool = ConnectionPool.from_url(url, **kwargs)
        client = cls(
            connection_pool=connection_pool,
            single_connection_client=single_connection_client,
        )
        client.auto_close_connection_pool = True
        return client

    @classmethod
    def from_pool(
        cls: Type["Redis"],
        connection_pool: ConnectionPool,
    ) -> "Redis":
        """
        Return a Redis client from the given connection pool.
        The Redis client will take ownership of the connection pool and
        close it when the Redis client is closed.
        """
        client = cls(
            connection_pool=connection_pool,
        )
        client.auto_close_connection_pool = True
        return client

    @deprecated_args(
        args_to_warn=["retry_on_timeout"],
        reason="TimeoutError is included by default.",
        version="6.0.0",
    )
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        socket_timeout: Optional[float] = None,
        socket_connect_timeout: Optional[float] = None,
        socket_keepalive: Optional[bool] = None,
        socket_keepalive_options: Optional[Mapping[int, Union[int, bytes]]] = None,
        connection_pool: Optional[ConnectionPool] = None,
        unix_socket_path: Optional[str] = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        retry_on_timeout: bool = False,
        retry: Retry = Retry(
            backoff=ExponentialWithJitterBackoff(base=1, cap=10), retries=3
        ),
        retry_on_error: Optional[List[Type[Exception]]] = None,
        ssl: bool = False,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_cert_reqs: Union[str, "ssl.VerifyMode"] = "required",
        ssl_ca_certs: Optional[str] = None,
        ssl_ca_path: Optional[str] = None,
        ssl_ca_data: Optional[str] = None,
        ssl_check_hostname: bool = True,
        ssl_password: Optional[str] = None,
        ssl_validate_ocsp: bool = False,
        ssl_validate_ocsp_stapled: bool = False,
        ssl_ocsp_context: Optional["OpenSSL.SSL.Context"] = None,
        ssl_ocsp_expected_cert: Optional[str] = None,
        ssl_min_version: Optional["ssl.TLSVersion"] = None,
        ssl_ciphers: Optional[str] = None,
        max_connections: Optional[int] = None,
        single_connection_client: bool = False,
        health_check_interval: int = 0,
        client_name: Optional[str] = None,
        lib_name: Optional[str] = "redis-py",
        lib_version: Optional[str] = get_lib_version(),
        username: Optional[str] = None,
        redis_connect_func: Optional[Callable[[], None]] = None,
        credential_provider: Optional[CredentialProvider] = None,
        protocol: Optional[int] = 2,
        cache: Optional[CacheInterface] = None,
        cache_config: Optional[CacheConfig] = None,
        event_dispatcher: Optional[EventDispatcher] = None,
    ) -> None:
        """
        Initialize a new Redis client.

        PARAMETER DOCUMENTATION
        =======================

        host (str, default="localhost"):
            Main description: Redis server hostname or IP address.
            Recommended values:
                - Production: Use FQDN (e.g., "redis.example.com")
                - Development: "localhost" or "127.0.0.1"
                - Docker: Service name (e.g., "redis-service")
            Related parameters: port, unix_socket_path (mutually exclusive)
            Common issues:
                - IPv6 resolution delays: Use "127.0.0.1" instead of "localhost"
                - Firewall blocks: Ensure Redis port is accessible

        port (int, default=6379):
            Main description: Redis server port number.
            Recommended values:
                - Standard: 6379 (default Redis port)
            Related parameters: host, ssl (for secure connections)
            Common issues:
                - Port conflicts: Check if port is already in use
                - Firewall rules: Ensure port is open in security groups

        db (int, default=0):
            Main description: Redis logical database number.
            Possible values: 0 - 15
            Recommended values:
                - Single app: 0 (default)
                - Namespaced keys: Different db per namespace (0-15)
                - Testing: Separate logical db for tests (e.g., 15)
            Trade-offs:
                - Multiple DBs: Logical separation but shared memory/CPU and configuration
                - Single DB: Better performance but requires key prefixing for namespacing
            Related parameters: None (isolated per database)
            Use cases:
                - Logical separation: db=0 (cache), db=1 (sessions), db=2 (temp data)
                - Testing: Ex. use db=15 for test data isolation
            Common issues:
                - DB limit: Redis supports 16 DBs by default (configurable)
                - Cross-DB operations: FLUSHALL affects ALL databases (https://redis.io/docs/latest/commands/flushall/)
                - MOVE command: Moves keys between databases (https://redis.io/docs/latest/commands/move/)
                - Cluster mode: Only db=0 supported in Redis Cluster
                - NOT true multi-tenancy: All DBs share CPU, memory, ACL, and configuration
            Security considerations:
                - Redis databases provide logical separation only, NOT security isolation
                - All databases share the same authentication and ACL rules
                - Memory and CPU resources are shared across all databases
                - Use separate Redis instances for true tenant isolation

        password (Optional[str], default=None):
            Main description: Redis server authentication password.
            Recommended values:
                - Production: Strong password (32+ chars, mixed case, symbols)
                - Development: Simple password or None for local dev
            Related parameters: username (for ACL), credential_provider
            Security considerations:
                - Never commit passwords to version control
                - Use Redis ACL for fine-grained access control
                - Rotate passwords regularly

        socket_timeout (Optional[float], default=None):
            Main description: Timeout for socket read/write operations in seconds.
            Recommended values:
                - Same datacenter: 0.3-0.5 seconds
                - Same region: 0.5-1.5 seconds
                - Cross-region (same continent): 1.0-2.0 seconds
                - Intercontinental: 1.0-3.0 seconds
                - Slow networks/unreliable connections: 3.0-10.0 seconds
                - Real-time systems: 0.05-0.2 seconds
                - None: No timeout (blocks indefinitely)
            Trade-offs:
                - Low timeout: Fast failure detection but may cause false timeouts
                - High timeout: More resilient but slower error detection
                - None: Never times out but may hang indefinitely
            Related parameters: socket_connect_timeout, retry, health_check_interval, ssl
            Use cases:
                - API endpoints: 0.5-2.0s to prevent request hanging
                - Slow/unreliable networks: 3-10s for poor connectivity
                - Real-time trading: 0.05-0.2s for predictable latency
            Common issues:
                - Timeout too low: Legitimate operations fail
                - No timeout: Application hangs on network issues
                - Inconsistent timeouts: Different behavior across environments
            Performance implications:
                - Affects all Redis operations (GET, SET, etc.)
                - Should be tuned based on network latency and operation complexity
            Distance and latency considerations:
                - Same datacenter: ~0.1-1ms RTT, Redis responds <1ms, use 0.1-0.5s timeout
                - Same region (e.g., us-east-1a to us-east-1b): ~1-5ms RTT, use 0.2-1.0s timeout
                - Cross-region same continent (e.g., us-east-1 to us-west-2): ~70-100ms RTT, use 0.5-2.0s timeout
                - Cross-continent (e.g., us-east-1 to eu-west-1): ~100-150ms RTT, use 1.0-3.0s timeout
                - Intercontinental (e.g., us-east-1 to ap-southeast-1): ~180-250ms RTT, use 1.5-4.0s timeout
                - Note: Redis operations typically complete in <1ms; timeout accounts for network RTT + safety margin
            TLS/SSL impact:
                - TLS handshake adds 1-3 additional round trips during connection establishment
                - Ongoing operations have minimal TLS overhead (~5-10% latency increase)
                - Consider higher socket_timeout when using SSL, especially for distant Redis instances

        socket_connect_timeout (Optional[float], default=None):
            Main description: Timeout for initial socket connection in seconds.
            Recommended values:
                - Local Redis (no SSL): 1.0-3.0 seconds
                - Local Redis (with SSL): 2.0-5.0 seconds
                - Remote Redis (no SSL): 3.0-8.0 seconds
                - Remote Redis (with SSL): 5.0-15.0 seconds
                - Unreliable networks: 10.0-30.0 seconds
                - None: Uses system default (usually 60-120s)
            Trade-offs:
                - Short timeout: Fast failure on connection issues
                - Long timeout: More resilient to network delays
                - None: May wait too long for failed connections
            Related parameters: socket_timeout, ssl, retry
            Common issues:
                - Too short: Fails on slow networks or high load
                - Too long: Slow startup and poor user experience in case of network issues
                - Network partitions: Connection hangs without timeout
            Performance implications:
                - Only affects initial connection establishment
                - Important for application startup time
                - Should account for network latency and server load
            SSL/TLS handshake considerations:
                - TLS handshake requires 1-3 additional round trips (2-6 RTT total)
                - Certificate validation adds processing time
                - Distant Redis with SSL: add 2-3x the base RTT to timeout
                - Example: 100ms RTT + SSL = ~300-500ms handshake time
                - Consider TLS session resumption to reduce reconnection overhead

        socket_keepalive (Optional[bool], default=None):
            Main description: Enable TCP keepalive to detect dead connections.

            What is TCP keepalive:
                TCP keepalive is a mechanism where the operating system periodically sends
                small probe packets on idle connections to verify the remote endpoint is
                still reachable. If the remote side doesn't respond after several probes,
                the connection is considered dead and closed. This happens at the TCP level,
                below the application layer.

            Why keepalive is needed:
                Redis keeps connections open indefinitely by default (if the timeout config is set to 0), but network
                issues, client crashes, or intermediate devices (firewalls, NAT, proxies) can
                cause "half-open" connections where one side thinks the connection is alive
                but the other side is unreachable. Without keepalive, these dead connections
                can accumulate and consume resources until manually detected.

            How keepalive improves reconnection:
                When keepalive detects a dead connection, the socket is closed immediately.
                This means reconnection attempts are much faster because redis-py won't waste
                time retrying operations on a dead connection and waiting for timeouts.
                Instead, it quickly establishes a new connection.

            Recommended values:
                - Production systems: True (recommended for all connections)
                - Connection pools: True (essential - affects all pool connections)
                - Development/testing: False or None (for simplicity)
            Trade-offs:
                - True: Detects dead connections but uses more network resources (only during idle periods)
                - False: Lower network overhead but may not detect connection failures
            Related parameters: socket_keepalive_options, health_check_interval
            Common issues:
                - Firewall interference: Some firewalls drop keepalive packets
                - Resource usage: Keepalive packets consume bandwidth
                - Timing conflicts: May conflict with application-level health checks
                - NAT timeouts: Helps prevent NAT table entry expiration

        socket_keepalive_options (Optional[Mapping[int, Union[int, bytes]]], default=None):
            Main description: Advanced TCP keepalive socket options.

            Available options reference:
                - Python socket module: import socket; help(socket) or dir(socket)
                - Common constants: socket.TCP_KEEPIDLE, socket.TCP_KEEPINTVL, socket.TCP_KEEPCNT
                - Platform-specific: socket.TCP_KEEPALIVE (macOS), socket.TCP_USER_TIMEOUT (Linux)
                - Online reference: https://docs.python.org/3/library/socket.html#socket-families
                - System documentation: man 7 tcp (Linux), man 4 tcp (BSD/macOS)

            Recommended values:
                - Linux: {socket.TCP_KEEPIDLE: 30, socket.TCP_KEEPINTVL: 10, socket.TCP_KEEPCNT: 3}
                - macOS: {socket.TCP_KEEPALIVE: 30, socket.TCP_KEEPINTVL: 10, socket.TCP_KEEPCNT: 3}
                - Windows: {socket.TCP_KEEPIDLE: 30, socket.TCP_KEEPINTVL: 10, socket.TCP_KEEPCNT: 3}
                - Default: None (use system defaults)
                - Custom: Tune based on network characteristics

            How to discover available options:
                ```python
                import socket
                # List all TCP-related constants
                tcp_options = [attr for attr in dir(socket) if attr.startswith('TCP_')]
                print(tcp_options)

                # Check if specific option exists on your platform
                if hasattr(socket, 'TCP_KEEPIDLE'):
                    print(f"TCP_KEEPIDLE = {socket.TCP_KEEPIDLE}")

                # Example configuration for 30-second keepalive
                keepalive_opts = {socket.TCP_KEEPIDLE: 30, socket.TCP_KEEPINTVL: 10, socket.TCP_KEEPCNT: 3}
                ```

            Trade-offs:
                - Custom options: Fine-tuned detection but platform-specific
                - System defaults: Portable but may not be optimal
            Related parameters: socket_keepalive (must be True)
            Use cases:
                - High-availability systems: Aggressive keepalive settings
                - Satellite/slow networks: Longer intervals
                - Container environments: Shorter intervals for faster detection
            Common issues:
                - Platform differences: Options vary between OS (use hasattr() to check)
                - Invalid options: May cause socket creation to fail
                - Firewall interference: Aggressive settings may be blocked
                - Constant availability: Not all TCP options available on all platforms
            Performance implications:
                - More frequent keepalive packets increase network usage
                - Faster dead connection detection improves reliability

        To specify a retry policy for specific errors, you have two options:

        1. Set the `retry_on_error` to a list of the error/s to retry on, and
        you can also set `retry` to a valid `Retry` object(in case the default
        one is not appropriate) - with this approach the retries will be triggered
        on the default errors specified in the Retry object enriched with the
        errors specified in `retry_on_error`.

        2. Define a `Retry` object with configured 'supported_errors' and set
        it to the `retry` parameter - with this approach you completely redefine
        the errors on which retries will happen.

        `retry_on_timeout` is deprecated - please include the TimeoutError
        either in the Retry object or in the `retry_on_error` list.

        When 'connection_pool' is provided - the retry configuration of the
        provided pool will be used.

        Args:

        single_connection_client:
            if `True`, connection pool is not used. In that case `Redis`
            instance use is not thread safe.
        """
        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher
        if not connection_pool:
            if not retry_on_error:
                retry_on_error = []
            kwargs = {
                "db": db,
                "username": username,
                "password": password,
                "socket_timeout": socket_timeout,
                "encoding": encoding,
                "encoding_errors": encoding_errors,
                "decode_responses": decode_responses,
                "retry_on_error": retry_on_error,
                "retry": copy.deepcopy(retry),
                "max_connections": max_connections,
                "health_check_interval": health_check_interval,
                "client_name": client_name,
                "lib_name": lib_name,
                "lib_version": lib_version,
                "redis_connect_func": redis_connect_func,
                "credential_provider": credential_provider,
                "protocol": protocol,
            }
            # based on input, setup appropriate connection args
            if unix_socket_path is not None:
                kwargs.update(
                    {
                        "path": unix_socket_path,
                        "connection_class": UnixDomainSocketConnection,
                    }
                )
            else:
                # TCP specific options
                kwargs.update(
                    {
                        "host": host,
                        "port": port,
                        "socket_connect_timeout": socket_connect_timeout,
                        "socket_keepalive": socket_keepalive,
                        "socket_keepalive_options": socket_keepalive_options,
                    }
                )

                if ssl:
                    kwargs.update(
                        {
                            "connection_class": SSLConnection,
                            "ssl_keyfile": ssl_keyfile,
                            "ssl_certfile": ssl_certfile,
                            "ssl_cert_reqs": ssl_cert_reqs,
                            "ssl_ca_certs": ssl_ca_certs,
                            "ssl_ca_data": ssl_ca_data,
                            "ssl_check_hostname": ssl_check_hostname,
                            "ssl_password": ssl_password,
                            "ssl_ca_path": ssl_ca_path,
                            "ssl_validate_ocsp_stapled": ssl_validate_ocsp_stapled,
                            "ssl_validate_ocsp": ssl_validate_ocsp,
                            "ssl_ocsp_context": ssl_ocsp_context,
                            "ssl_ocsp_expected_cert": ssl_ocsp_expected_cert,
                            "ssl_min_version": ssl_min_version,
                            "ssl_ciphers": ssl_ciphers,
                        }
                    )
                if (cache_config or cache) and protocol in [3, "3"]:
                    kwargs.update(
                        {
                            "cache": cache,
                            "cache_config": cache_config,
                        }
                    )
            connection_pool = ConnectionPool(**kwargs)
            self._event_dispatcher.dispatch(
                AfterPooledConnectionsInstantiationEvent(
                    [connection_pool], ClientType.SYNC, credential_provider
                )
            )
            self.auto_close_connection_pool = True
        else:
            self.auto_close_connection_pool = False
            self._event_dispatcher.dispatch(
                AfterPooledConnectionsInstantiationEvent(
                    [connection_pool], ClientType.SYNC, credential_provider
                )
            )

        self.connection_pool = connection_pool

        if (cache_config or cache) and self.connection_pool.get_protocol() not in [
            3,
            "3",
        ]:
            raise RedisError("Client caching is only supported with RESP version 3")

        self.single_connection_lock = threading.RLock()
        self.connection = None
        self._single_connection_client = single_connection_client
        if self._single_connection_client:
            self.connection = self.connection_pool.get_connection()
            self._event_dispatcher.dispatch(
                AfterSingleConnectionInstantiationEvent(
                    self.connection, ClientType.SYNC, self.single_connection_lock
                )
            )

        self.response_callbacks = CaseInsensitiveDict(_RedisCallbacks)

        if self.connection_pool.connection_kwargs.get("protocol") in ["3", 3]:
            self.response_callbacks.update(_RedisCallbacksRESP3)
        else:
            self.response_callbacks.update(_RedisCallbacksRESP2)

    def __repr__(self) -> str:
        return (
            f"<{type(self).__module__}.{type(self).__name__}"
            f"({repr(self.connection_pool)})>"
        )

    def get_encoder(self) -> "Encoder":
        """Get the connection pool's encoder"""
        return self.connection_pool.get_encoder()

    def get_connection_kwargs(self) -> Dict:
        """Get the connection's key-word arguments"""
        return self.connection_pool.connection_kwargs

    def get_retry(self) -> Optional[Retry]:
        return self.get_connection_kwargs().get("retry")

    def set_retry(self, retry: Retry) -> None:
        self.get_connection_kwargs().update({"retry": retry})
        self.connection_pool.set_retry(retry)

    def set_response_callback(self, command: str, callback: Callable) -> None:
        """Set a custom Response Callback"""
        self.response_callbacks[command] = callback

    def load_external_module(self, funcname, func) -> None:
        """
        This function can be used to add externally defined redis modules,
        and their namespaces to the redis client.

        funcname - A string containing the name of the function to create
        func - The function, being added to this class.

        ex: Assume that one has a custom redis module named foomod that
        creates command named 'foo.dothing' and 'foo.anotherthing' in redis.
        To load function functions into this namespace:

        from redis import Redis
        from foomodule import F
        r = Redis()
        r.load_external_module("foo", F)
        r.foo().dothing('your', 'arguments')

        For a concrete example see the reimport of the redisjson module in
        tests/test_connection.py::test_loading_external_modules
        """
        setattr(self, funcname, func)

    def pipeline(self, transaction=True, shard_hint=None) -> "Pipeline":
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        return Pipeline(
            self.connection_pool, self.response_callbacks, transaction, shard_hint
        )

    def transaction(
        self, func: Callable[["Pipeline"], None], *watches, **kwargs
    ) -> Union[List[Any], Any, None]:
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single argument which is a Pipeline object.
        """
        shard_hint = kwargs.pop("shard_hint", None)
        value_from_callable = kwargs.pop("value_from_callable", False)
        watch_delay = kwargs.pop("watch_delay", None)
        with self.pipeline(True, shard_hint) as pipe:
            while True:
                try:
                    if watches:
                        pipe.watch(*watches)
                    func_value = func(pipe)
                    exec_value = pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        time.sleep(watch_delay)
                    continue

    def lock(
        self,
        name: str,
        timeout: Optional[float] = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: Optional[float] = None,
        lock_class: Union[None, Any] = None,
        thread_local: bool = True,
        raise_on_release_error: bool = True,
    ):
        """
        Return a new Lock object using key ``name`` that mimics
        the behavior of threading.Lock.

        If specified, ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        ``blocking`` indicates whether calling ``acquire`` should block until
        the lock has been acquired or to fail immediately, causing ``acquire``
        to return False and the lock not being acquired. Defaults to True.
        Note this value can be overridden by passing a ``blocking``
        argument to ``acquire``.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``lock_class`` forces the specified lock implementation. Note that as
        of redis-py 3.0, the only lock class we implement is ``Lock`` (which is
        a Lua-based lock). So, it's unlikely you'll need this parameter, unless
        you have created your own custom lock class.

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

        ``raise_on_release_error`` indicates whether to raise an exception when
        the lock is no longer owned when exiting the context manager. By default,
        this is True, meaning an exception will be raised. If False, the warning
        will be logged and the exception will be suppressed.

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
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
            raise_on_release_error=raise_on_release_error,
        )

    def pubsub(self, **kwargs):
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        them.
        """
        return PubSub(
            self.connection_pool, event_dispatcher=self._event_dispatcher, **kwargs
        )

    def monitor(self):
        return Monitor(self.connection_pool)

    def client(self):
        return self.__class__(
            connection_pool=self.connection_pool, single_connection_client=True
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def close(self) -> None:
        # In case a connection property does not yet exist
        # (due to a crash earlier in the Redis() constructor), return
        # immediately as there is nothing to clean-up.
        if not hasattr(self, "connection"):
            return

        conn = self.connection
        if conn:
            self.connection = None
            self.connection_pool.release(conn)

        if self.auto_close_connection_pool:
            self.connection_pool.disconnect()

    def _send_command_parse_response(self, conn, command_name, *args, **options):
        """
        Send a command and parse the response
        """
        conn.send_command(*args, **options)
        return self.parse_response(conn, command_name, **options)

    def _close_connection(self, conn) -> None:
        """
        Close the connection before retrying.

        The supported exceptions are already checked in the
        retry object so we don't need to do it here.

        After we disconnect the connection, it will try to reconnect and
        do a health check as part of the send_command logic(on connection level).
        """

        conn.disconnect()

    # COMMAND EXECUTION AND PROTOCOL PARSING
    def execute_command(self, *args, **options):
        return self._execute_command(*args, **options)

    def _execute_command(self, *args, **options):
        """Execute a command and return a parsed response"""
        pool = self.connection_pool
        command_name = args[0]
        conn = self.connection or pool.get_connection()

        if self._single_connection_client:
            self.single_connection_lock.acquire()
        try:
            return conn.retry.call_with_retry(
                lambda: self._send_command_parse_response(
                    conn, command_name, *args, **options
                ),
                lambda _: self._close_connection(conn),
            )
        finally:
            if self._single_connection_client:
                self.single_connection_lock.release()
            if not self.connection:
                pool.release(conn)

    def parse_response(self, connection, command_name, **options):
        """Parses a response from the Redis server"""
        try:
            if NEVER_DECODE in options:
                response = connection.read_response(disable_decoding=True)
                options.pop(NEVER_DECODE)
            else:
                response = connection.read_response()
        except ResponseError:
            if EMPTY_RESPONSE in options:
                return options[EMPTY_RESPONSE]
            raise

        if EMPTY_RESPONSE in options:
            options.pop(EMPTY_RESPONSE)

        # Remove keys entry, it needs only for cache.
        options.pop("keys", None)

        if command_name in self.response_callbacks:
            return self.response_callbacks[command_name](response, **options)
        return response

    def get_cache(self) -> Optional[CacheInterface]:
        return self.connection_pool.cache


StrictRedis = Redis


class Monitor:
    """
    Monitor is useful for handling the MONITOR command to the redis server.
    next_command() method returns one command from monitor
    listen() method yields commands from monitor.
    """

    monitor_re = re.compile(r"\[(\d+) (.*?)\] (.*)")
    command_re = re.compile(r'"(.*?)(?<!\\)"')

    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.connection = self.connection_pool.get_connection()

    def __enter__(self):
        self.connection.send_command("MONITOR")
        # check that monitor returns 'OK', but don't return it to user
        response = self.connection.read_response()
        if not bool_ok(response):
            raise RedisError(f"MONITOR failed: {response}")
        return self

    def __exit__(self, *args):
        self.connection.disconnect()
        self.connection_pool.release(self.connection)

    def next_command(self):
        """Parse the response from a monitor command"""
        response = self.connection.read_response()
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

    def listen(self):
        """Listen for commands coming to the server."""
        while True:
            yield self.next_command()


class PubSub:
    """
    PubSub provides publish, subscribe and listen support to Redis channels.

    After subscribing to one or more channels, the listen() method will block
    until a message arrives on one of the subscribed channels. That message
    will be returned and it's safe to start listening again.
    """

    PUBLISH_MESSAGE_TYPES = ("message", "pmessage", "smessage")
    UNSUBSCRIBE_MESSAGE_TYPES = ("unsubscribe", "punsubscribe", "sunsubscribe")
    HEALTH_CHECK_MESSAGE = "redis-py-health-check"

    def __init__(
        self,
        connection_pool,
        shard_hint=None,
        ignore_subscribe_messages: bool = False,
        encoder: Optional["Encoder"] = None,
        push_handler_func: Union[None, Callable[[str], None]] = None,
        event_dispatcher: Optional["EventDispatcher"] = None,
    ):
        self.connection_pool = connection_pool
        self.shard_hint = shard_hint
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.connection = None
        self.subscribed_event = threading.Event()
        # we need to know the encoding options for this connection in order
        # to lookup channel and pattern names for callback handlers.
        self.encoder = encoder
        self.push_handler_func = push_handler_func
        if event_dispatcher is None:
            self._event_dispatcher = EventDispatcher()
        else:
            self._event_dispatcher = event_dispatcher

        self._lock = threading.RLock()
        if self.encoder is None:
            self.encoder = self.connection_pool.get_encoder()
        self.health_check_response_b = self.encoder.encode(self.HEALTH_CHECK_MESSAGE)
        if self.encoder.decode_responses:
            self.health_check_response = ["pong", self.HEALTH_CHECK_MESSAGE]
        else:
            self.health_check_response = [b"pong", self.health_check_response_b]
        if self.push_handler_func is None:
            _set_info_logger()
        self.reset()

    def __enter__(self) -> "PubSub":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.reset()

    def __del__(self) -> None:
        try:
            # if this object went out of scope prior to shutting down
            # subscriptions, close the connection manually before
            # returning it to the connection pool
            self.reset()
        except Exception:
            pass

    def reset(self) -> None:
        if self.connection:
            self.connection.disconnect()
            self.connection.deregister_connect_callback(self.on_connect)
            self.connection_pool.release(self.connection)
            self.connection = None
        self.health_check_response_counter = 0
        self.channels = {}
        self.pending_unsubscribe_channels = set()
        self.shard_channels = {}
        self.pending_unsubscribe_shard_channels = set()
        self.patterns = {}
        self.pending_unsubscribe_patterns = set()
        self.subscribed_event.clear()

    def close(self) -> None:
        self.reset()

    def on_connect(self, connection) -> None:
        "Re-subscribe to any channels and patterns previously subscribed to"
        # NOTE: for python3, we can't pass bytestrings as keyword arguments
        # so we need to decode channel/pattern names back to unicode strings
        # before passing them to [p]subscribe.
        self.pending_unsubscribe_channels.clear()
        self.pending_unsubscribe_patterns.clear()
        self.pending_unsubscribe_shard_channels.clear()
        if self.channels:
            channels = {
                self.encoder.decode(k, force=True): v for k, v in self.channels.items()
            }
            self.subscribe(**channels)
        if self.patterns:
            patterns = {
                self.encoder.decode(k, force=True): v for k, v in self.patterns.items()
            }
            self.psubscribe(**patterns)
        if self.shard_channels:
            shard_channels = {
                self.encoder.decode(k, force=True): v
                for k, v in self.shard_channels.items()
            }
            self.ssubscribe(**shard_channels)

    @property
    def subscribed(self) -> bool:
        """Indicates if there are subscriptions to any channels or patterns"""
        return self.subscribed_event.is_set()

    def execute_command(self, *args):
        """Execute a publish/subscribe command"""

        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        if self.connection is None:
            self.connection = self.connection_pool.get_connection()
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            self.connection.register_connect_callback(self.on_connect)
            if self.push_handler_func is not None:
                self.connection._parser.set_pubsub_push_handler(self.push_handler_func)
            self._event_dispatcher.dispatch(
                AfterPubSubConnectionInstantiationEvent(
                    self.connection, self.connection_pool, ClientType.SYNC, self._lock
                )
            )
        connection = self.connection
        kwargs = {"check_health": not self.subscribed}
        if not self.subscribed:
            self.clean_health_check_responses()
        with self._lock:
            self._execute(connection, connection.send_command, *args, **kwargs)

    def clean_health_check_responses(self) -> None:
        """
        If any health check responses are present, clean them
        """
        ttl = 10
        conn = self.connection
        while self.health_check_response_counter > 0 and ttl > 0:
            if self._execute(conn, conn.can_read, timeout=conn.socket_timeout):
                response = self._execute(conn, conn.read_response)
                if self.is_health_check_response(response):
                    self.health_check_response_counter -= 1
                else:
                    raise PubSubError(
                        "A non health check response was cleaned by "
                        "execute_command: {}".format(response)
                    )
            ttl -= 1

    def _reconnect(self, conn) -> None:
        """
        The supported exceptions are already checked in the
        retry object so we don't need to do it here.

        In this error handler we are trying to reconnect to the server.
        """
        conn.disconnect()
        conn.connect()

    def _execute(self, conn, command, *args, **kwargs):
        """
        Connect manually upon disconnection. If the Redis server is down,
        this will fail and raise a ConnectionError as desired.
        After reconnection, the ``on_connect`` callback should have been
        called by the # connection to resubscribe us to any channels and
        patterns we were previously listening to
        """
        return conn.retry.call_with_retry(
            lambda: command(*args, **kwargs),
            lambda _: self._reconnect(conn),
        )

    def parse_response(self, block=True, timeout=0):
        """Parse the response from a publish/subscribe command"""
        conn = self.connection
        if conn is None:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
            )

        self.check_health()

        def try_read():
            if not block:
                if not conn.can_read(timeout=timeout):
                    return None
            else:
                conn.connect()
            return conn.read_response(disconnect_on_error=False, push_request=True)

        response = self._execute(conn, try_read)

        if self.is_health_check_response(response):
            # ignore the health check message as user might not expect it
            self.health_check_response_counter -= 1
            return None
        return response

    def is_health_check_response(self, response) -> bool:
        """
        Check if the response is a health check response.
        If there are no subscriptions redis responds to PING command with a
        bulk response, instead of a multi-bulk with "pong" and the response.
        """
        return response in [
            self.health_check_response,  # If there was a subscription
            self.health_check_response_b,  # If there wasn't
        ]

    def check_health(self) -> None:
        conn = self.connection
        if conn is None:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
            )

        if conn.health_check_interval and time.monotonic() > conn.next_health_check:
            conn.send_command("PING", self.HEALTH_CHECK_MESSAGE, check_health=False)
            self.health_check_response_counter += 1

    def _normalize_keys(self, data) -> Dict:
        """
        normalize channel/pattern names to be either bytes or strings
        based on whether responses are automatically decoded. this saves us
        from coercing the value for each message coming in.
        """
        encode = self.encoder.encode
        decode = self.encoder.decode
        return {decode(encode(k)): v for k, v in data.items()}

    def psubscribe(self, *args, **kwargs):
        """
        Subscribe to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern rather than producing a message via
        ``listen()``.
        """
        if args:
            args = list_or_args(args[0], args[1:])
        new_patterns = dict.fromkeys(args)
        new_patterns.update(kwargs)
        ret_val = self.execute_command("PSUBSCRIBE", *new_patterns.keys())
        # update the patterns dict AFTER we send the command. we don't want to
        # subscribe twice to these patterns, once for the command and again
        # for the reconnection.
        new_patterns = self._normalize_keys(new_patterns)
        self.patterns.update(new_patterns)
        if not self.subscribed:
            # Set the subscribed_event flag to True
            self.subscribed_event.set()
            # Clear the health check counter
            self.health_check_response_counter = 0
        self.pending_unsubscribe_patterns.difference_update(new_patterns)
        return ret_val

    def punsubscribe(self, *args):
        """
        Unsubscribe from the supplied patterns. If empty, unsubscribe from
        all patterns.
        """
        if args:
            args = list_or_args(args[0], args[1:])
            patterns = self._normalize_keys(dict.fromkeys(args))
        else:
            patterns = self.patterns
        self.pending_unsubscribe_patterns.update(patterns)
        return self.execute_command("PUNSUBSCRIBE", *args)

    def subscribe(self, *args, **kwargs):
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """
        if args:
            args = list_or_args(args[0], args[1:])
        new_channels = dict.fromkeys(args)
        new_channels.update(kwargs)
        ret_val = self.execute_command("SUBSCRIBE", *new_channels.keys())
        # update the channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        new_channels = self._normalize_keys(new_channels)
        self.channels.update(new_channels)
        if not self.subscribed:
            # Set the subscribed_event flag to True
            self.subscribed_event.set()
            # Clear the health check counter
            self.health_check_response_counter = 0
        self.pending_unsubscribe_channels.difference_update(new_channels)
        return ret_val

    def unsubscribe(self, *args):
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        if args:
            args = list_or_args(args[0], args[1:])
            channels = self._normalize_keys(dict.fromkeys(args))
        else:
            channels = self.channels
        self.pending_unsubscribe_channels.update(channels)
        return self.execute_command("UNSUBSCRIBE", *args)

    def ssubscribe(self, *args, target_node=None, **kwargs):
        """
        Subscribes the client to the specified shard channels.
        Channels supplied as keyword arguments expect a channel name as the key
        and a callable as the value. A channel's callable will be invoked automatically
        when a message is received on that channel rather than producing a message via
        ``listen()`` or ``get_sharded_message()``.
        """
        if args:
            args = list_or_args(args[0], args[1:])
        new_s_channels = dict.fromkeys(args)
        new_s_channels.update(kwargs)
        ret_val = self.execute_command("SSUBSCRIBE", *new_s_channels.keys())
        # update the s_channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        new_s_channels = self._normalize_keys(new_s_channels)
        self.shard_channels.update(new_s_channels)
        if not self.subscribed:
            # Set the subscribed_event flag to True
            self.subscribed_event.set()
            # Clear the health check counter
            self.health_check_response_counter = 0
        self.pending_unsubscribe_shard_channels.difference_update(new_s_channels)
        return ret_val

    def sunsubscribe(self, *args, target_node=None):
        """
        Unsubscribe from the supplied shard_channels. If empty, unsubscribe from
        all shard_channels
        """
        if args:
            args = list_or_args(args[0], args[1:])
            s_channels = self._normalize_keys(dict.fromkeys(args))
        else:
            s_channels = self.shard_channels
        self.pending_unsubscribe_shard_channels.update(s_channels)
        return self.execute_command("SUNSUBSCRIBE", *args)

    def listen(self):
        "Listen for messages on channels this client has been subscribed to"
        while self.subscribed:
            response = self.handle_message(self.parse_response(block=True))
            if response is not None:
                yield response

    def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float = 0.0
    ):
        """
        Get the next message if one is available, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number, or None, to wait indefinitely.
        """
        if not self.subscribed:
            # Wait for subscription
            start_time = time.monotonic()
            if self.subscribed_event.wait(timeout) is True:
                # The connection was subscribed during the timeout time frame.
                # The timeout should be adjusted based on the time spent
                # waiting for the subscription
                time_spent = time.monotonic() - start_time
                timeout = max(0.0, timeout - time_spent)
            else:
                # The connection isn't subscribed to any channels or patterns,
                # so no messages are available
                return None

        response = self.parse_response(block=(timeout is None), timeout=timeout)
        if response:
            return self.handle_message(response, ignore_subscribe_messages)
        return None

    get_sharded_message = get_message

    def ping(self, message: Union[str, None] = None) -> bool:
        """
        Ping the Redis server
        """
        args = ["PING", message] if message is not None else ["PING"]
        return self.execute_command(*args)

    def handle_message(self, response, ignore_subscribe_messages=False):
        """
        Parses a pub/sub message. If the channel or pattern was subscribed to
        with a message handler, the handler is invoked instead of a parsed
        message being returned.
        """
        if response is None:
            return None
        if isinstance(response, bytes):
            response = [b"pong", response] if response != b"PONG" else [b"pong", b""]
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
            elif message_type == "sunsubscribe":
                s_channel = response[1]
                if s_channel in self.pending_unsubscribe_shard_channels:
                    self.pending_unsubscribe_shard_channels.remove(s_channel)
                    self.shard_channels.pop(s_channel, None)
            else:
                channel = response[1]
                if channel in self.pending_unsubscribe_channels:
                    self.pending_unsubscribe_channels.remove(channel)
                    self.channels.pop(channel, None)
            if not self.channels and not self.patterns and not self.shard_channels:
                # There are no subscriptions anymore, set subscribed_event flag
                # to false
                self.subscribed_event.clear()

        if message_type in self.PUBLISH_MESSAGE_TYPES:
            # if there's a message handler, invoke it
            if message_type == "pmessage":
                handler = self.patterns.get(message["pattern"], None)
            elif message_type == "smessage":
                handler = self.shard_channels.get(message["channel"], None)
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

    def run_in_thread(
        self,
        sleep_time: float = 0.0,
        daemon: bool = False,
        exception_handler: Optional[Callable] = None,
    ) -> "PubSubWorkerThread":
        for channel, handler in self.channels.items():
            if handler is None:
                raise PubSubError(f"Channel: '{channel}' has no handler registered")
        for pattern, handler in self.patterns.items():
            if handler is None:
                raise PubSubError(f"Pattern: '{pattern}' has no handler registered")
        for s_channel, handler in self.shard_channels.items():
            if handler is None:
                raise PubSubError(
                    f"Shard Channel: '{s_channel}' has no handler registered"
                )

        thread = PubSubWorkerThread(
            self, sleep_time, daemon=daemon, exception_handler=exception_handler
        )
        thread.start()
        return thread


class PubSubWorkerThread(threading.Thread):
    def __init__(
        self,
        pubsub,
        sleep_time: float,
        daemon: bool = False,
        exception_handler: Union[
            Callable[[Exception, "PubSub", "PubSubWorkerThread"], None], None
        ] = None,
    ):
        super().__init__()
        self.daemon = daemon
        self.pubsub = pubsub
        self.sleep_time = sleep_time
        self.exception_handler = exception_handler
        self._running = threading.Event()

    def run(self) -> None:
        if self._running.is_set():
            return
        self._running.set()
        pubsub = self.pubsub
        sleep_time = self.sleep_time
        while self._running.is_set():
            try:
                pubsub.get_message(ignore_subscribe_messages=True, timeout=sleep_time)
            except BaseException as e:
                if self.exception_handler is None:
                    raise
                self.exception_handler(e, pubsub, self)
        pubsub.close()

    def stop(self) -> None:
        # trip the flag so the run loop exits. the run loop will
        # close the pubsub connection, which disconnects the socket
        # and returns the connection to the pool.
        self._running.clear()


class Pipeline(Redis):
    """
    Pipelines provide a way to transmit multiple commands to the Redis server
    in one transmission.  This is convenient for batch processing, such as
    saving all the values in a list to Redis.

    All commands executed within a pipeline(when running in transactional mode,
    which is the default behavior) are wrapped with MULTI and EXEC
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
        response_callbacks,
        transaction,
        shard_hint,
    ):
        self.connection_pool = connection_pool
        self.connection: Optional[Connection] = None
        self.response_callbacks = response_callbacks
        self.transaction = transaction
        self.shard_hint = shard_hint
        self.watching = False
        self.command_stack = []
        self.scripts: Set[Script] = set()
        self.explicit_transaction = False

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self) -> int:
        return len(self.command_stack)

    def __bool__(self) -> bool:
        """Pipeline instances should always evaluate to True"""
        return True

    def reset(self) -> None:
        self.command_stack = []
        self.scripts = set()
        # make sure to reset the connection state in the event that we were
        # watching something
        if self.watching and self.connection:
            try:
                # call this manually since our unwatch or
                # immediate_execute_command methods can call reset()
                self.connection.send_command("UNWATCH")
                self.connection.read_response()
            except ConnectionError:
                # disconnect will also remove any previous WATCHes
                self.connection.disconnect()
        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False
        # we can safely return the connection to the pool here since we're
        # sure we're no longer WATCHing anything
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    def close(self) -> None:
        """Close the pipeline"""
        self.reset()

    def multi(self) -> None:
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        if self.explicit_transaction:
            raise RedisError("Cannot issue nested calls to MULTI")
        if self.command_stack:
            raise RedisError(
                "Commands without an initial WATCH have already been issued"
            )
        self.explicit_transaction = True

    def execute_command(self, *args, **kwargs):
        if (self.watching or args[0] == "WATCH") and not self.explicit_transaction:
            return self.immediate_execute_command(*args, **kwargs)
        return self.pipeline_execute_command(*args, **kwargs)

    def _disconnect_reset_raise_on_watching(
        self,
        conn: AbstractConnection,
        error: Exception,
    ) -> None:
        """
        Close the connection reset watching state and
        raise an exception if we were watching.

        The supported exceptions are already checked in the
        retry object so we don't need to do it here.

        After we disconnect the connection, it will try to reconnect and
        do a health check as part of the send_command logic(on connection level).
        """
        conn.disconnect()

        # if we were already watching a variable, the watch is no longer
        # valid since this connection has died. raise a WatchError, which
        # indicates the user should retry this transaction.
        if self.watching:
            self.reset()
            raise WatchError(
                f"A {type(error).__name__} occurred while watching one or more keys"
            )

    def immediate_execute_command(self, *args, **options):
        """
        Execute a command immediately, but don't auto-retry on the supported
        errors for retry if we're already WATCHing a variable.
        Used when issuing WATCH or subsequent commands retrieving their values but before
        MULTI is called.
        """
        command_name = args[0]
        conn = self.connection
        # if this is the first call, we need a connection
        if not conn:
            conn = self.connection_pool.get_connection()
            self.connection = conn

        return conn.retry.call_with_retry(
            lambda: self._send_command_parse_response(
                conn, command_name, *args, **options
            ),
            lambda error: self._disconnect_reset_raise_on_watching(conn, error),
        )

    def pipeline_execute_command(self, *args, **options) -> "Pipeline":
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

    def _execute_transaction(
        self, connection: Connection, commands, raise_on_error
    ) -> List:
        cmds = chain([(("MULTI",), {})], commands, [(("EXEC",), {})])
        all_cmds = connection.pack_commands(
            [args for args, options in cmds if EMPTY_RESPONSE not in options]
        )
        connection.send_packed_command(all_cmds)
        errors = []

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            self.parse_response(connection, "_")
        except ResponseError as e:
            errors.append((0, e))

        # and all the other commands
        for i, command in enumerate(commands):
            if EMPTY_RESPONSE in command[1]:
                errors.append((i, command[1][EMPTY_RESPONSE]))
            else:
                try:
                    self.parse_response(connection, "_")
                except ResponseError as e:
                    self.annotate_exception(e, i + 1, command[0])
                    errors.append((i, e))

        # parse the EXEC.
        try:
            response = self.parse_response(connection, "_")
        except ExecAbortError:
            if errors:
                raise errors[0][1]
            raise

        # EXEC clears any watched keys
        self.watching = False

        if response is None:
            raise WatchError("Watched variable changed.")

        # put any parse errors into the response
        for i, e in errors:
            response.insert(i, e)

        if len(response) != len(commands):
            self.connection.disconnect()
            raise ResponseError(
                "Wrong number of response items from pipeline execution"
            )

        # find any errors in the response and raise if necessary
        if raise_on_error:
            self.raise_first_error(commands, response)

        # We have to run response callbacks manually
        data = []
        for r, cmd in zip(response, commands):
            if not isinstance(r, Exception):
                args, options = cmd
                # Remove keys entry, it needs only for cache.
                options.pop("keys", None)
                command_name = args[0]
                if command_name in self.response_callbacks:
                    r = self.response_callbacks[command_name](r, **options)
            data.append(r)
        return data

    def _execute_pipeline(self, connection, commands, raise_on_error):
        # build up all commands into a single request to increase network perf
        all_cmds = connection.pack_commands([args for args, _ in commands])
        connection.send_packed_command(all_cmds)

        response = []
        for args, options in commands:
            try:
                response.append(self.parse_response(connection, args[0], **options))
            except ResponseError as e:
                response.append(e)

        if raise_on_error:
            self.raise_first_error(commands, response)
        return response

    def raise_first_error(self, commands, response):
        for i, r in enumerate(response):
            if isinstance(r, ResponseError):
                self.annotate_exception(r, i + 1, commands[i][0])
                raise r

    def annotate_exception(self, exception, number, command):
        cmd = " ".join(map(safe_str, command))
        msg = (
            f"Command # {number} ({truncate_text(cmd)}) of pipeline "
            f"caused error: {exception.args[0]}"
        )
        exception.args = (msg,) + exception.args[1:]

    def parse_response(self, connection, command_name, **options):
        result = Redis.parse_response(self, connection, command_name, **options)
        if command_name in self.UNWATCH_COMMANDS:
            self.watching = False
        elif command_name == "WATCH":
            self.watching = True
        return result

    def load_scripts(self):
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        # we can't use the normal script_* methods because they would just
        # get buffered in the pipeline.
        exists = immediate("SCRIPT EXISTS", *shas)
        if not all(exists):
            for s, exist in zip(scripts, exists):
                if not exist:
                    s.sha = immediate("SCRIPT LOAD", s.script)

    def _disconnect_raise_on_watching(
        self,
        conn: AbstractConnection,
        error: Exception,
    ) -> None:
        """
        Close the connection, raise an exception if we were watching.

        The supported exceptions are already checked in the
        retry object so we don't need to do it here.

        After we disconnect the connection, it will try to reconnect and
        do a health check as part of the send_command logic(on connection level).
        """
        conn.disconnect()
        # if we were watching a variable, the watch is no longer valid
        # since this connection has died. raise a WatchError, which
        # indicates the user should retry this transaction.
        if self.watching:
            raise WatchError(
                f"A {type(error).__name__} occurred while watching one or more keys"
            )

    def execute(self, raise_on_error: bool = True) -> List[Any]:
        """Execute all the commands in the current pipeline"""
        stack = self.command_stack
        if not stack and not self.watching:
            return []
        if self.scripts:
            self.load_scripts()
        if self.transaction or self.explicit_transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline

        conn = self.connection
        if not conn:
            conn = self.connection_pool.get_connection()
            # assign to self.connection so reset() releases the connection
            # back to the pool after we're done
            self.connection = conn

        try:
            return conn.retry.call_with_retry(
                lambda: execute(conn, stack, raise_on_error),
                lambda error: self._disconnect_raise_on_watching(conn, error),
            )
        finally:
            self.reset()

    def discard(self):
        """
        Flushes all previously queued commands
        See: https://redis.io/commands/DISCARD
        """
        self.execute_command("DISCARD")

    def watch(self, *names):
        """Watches the values at keys ``names``"""
        if self.explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")
        return self.execute_command("WATCH", *names)

    def unwatch(self) -> bool:
        """Unwatches all previously specified keys"""
        return self.watching and self.execute_command("UNWATCH") or True
