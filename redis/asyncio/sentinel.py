import asyncio
import random
import weakref
from typing import (
    Any,
    AsyncIterator,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
)

from redis.asyncio.client import Redis
from redis.asyncio.connection import (
    Connection,
    ConnectionPool,
    EncodableT,
    SSLConnection,
)
from redis.commands import AsyncSentinelCommands
from redis.exceptions import ConnectionError, ReadOnlyError, ResponseError, TimeoutError
from redis.utils import str_if_bytes


class MasterNotFoundError(ConnectionError):
    pass


class SlaveNotFoundError(ConnectionError):
    pass


class SentinelManagedConnection(Connection):
    def __init__(self, **kwargs):
        self.connection_pool = kwargs.pop("connection_pool")
        super().__init__(**kwargs)

    def __repr__(self):
        pool = self.connection_pool
        s = (
            f"<{self.__class__.__module__}.{self.__class__.__name__}"
            f"(service={pool.service_name}"
        )
        if self.host:
            host_info = f",host={self.host},port={self.port}"
            s += host_info
        return s + ")>"

    async def connect_to(self, address):
        self.host, self.port = address
        await super().connect()
        if self.connection_pool.check_connection:
            await self.send_command("PING")
            if str_if_bytes(await self.read_response()) != "PONG":
                raise ConnectionError("PING failed")

    async def _connect_retry(self):
        if self._reader:
            return  # already connected
        if self.connection_pool.is_master:
            await self.connect_to(await self.connection_pool.get_master_address())
        else:
            async for slave in self.connection_pool.rotate_slaves():
                try:
                    return await self.connect_to(slave)
                except ConnectionError:
                    continue
            raise SlaveNotFoundError  # Never be here

    async def connect(self):
        return await self.retry.call_with_retry(
            self._connect_retry,
            lambda error: asyncio.sleep(0),
        )

    async def _connect_to_address_retry(self, host: str, port: int) -> None:
        if self._reader:
            return  # already connected
        try:
            return await self.connect_to((host, port))
        except ConnectionError:
            raise SlaveNotFoundError

    async def connect_to_address(self, host: str, port: int) -> None:
        # Connect to the specified host and port
        # instead of connecting to the master / rotated slaves
        return await self.retry.call_with_retry(
            lambda: self._connect_to_address_retry(host, port),
            lambda error: asyncio.sleep(0),
        )

    async def read_response(
        self,
        disable_decoding: bool = False,
        timeout: Optional[float] = None,
        *,
        disconnect_on_error: Optional[float] = True,
        push_request: Optional[bool] = False,
    ):
        try:
            return await super().read_response(
                disable_decoding=disable_decoding,
                timeout=timeout,
                disconnect_on_error=disconnect_on_error,
                push_request=push_request,
            )
        except ReadOnlyError:
            if self.connection_pool.is_master:
                # When talking to a master, a ReadOnlyError when likely
                # indicates that the previous master that we're still connected
                # to has been demoted to a slave and there's a new master.
                # calling disconnect will force the connection to re-query
                # sentinel during the next connect() attempt.
                await self.disconnect()
                raise ConnectionError("The previous master is now a slave")
            raise


class SentinelManagedSSLConnection(SentinelManagedConnection, SSLConnection):
    pass


class SentinelConnectionPool(ConnectionPool):
    """
    Sentinel backed connection pool.

    If ``check_connection`` flag is set to True, SentinelManagedConnection
    sends a PING command right after establishing the connection.
    """

    def __init__(self, service_name, sentinel_manager, **kwargs):
        kwargs["connection_class"] = kwargs.get(
            "connection_class",
            (
                SentinelManagedSSLConnection
                if kwargs.pop("ssl", False)
                else SentinelManagedConnection
            ),
        )
        self.is_master = kwargs.pop("is_master", True)
        self.check_connection = kwargs.pop("check_connection", False)
        super().__init__(**kwargs)
        self.connection_kwargs["connection_pool"] = weakref.proxy(self)
        self.service_name = service_name
        self.sentinel_manager = sentinel_manager
        self.master_address = None
        self.slave_rr_counter = None
        self._request_id_to_replica_address = {}

    def __repr__(self):
        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__}"
            f"(service={self.service_name}({self.is_master and 'master' or 'slave'}))>"
        )

    def reset(self):
        super().reset()
        self.master_address = None
        self.slave_rr_counter = None

    def owns_connection(self, connection: Connection):
        check = not self.is_master or (
            self.is_master and self.master_address == (connection.host, connection.port)
        )
        return check and super().owns_connection(connection)

    async def get_master_address(self):
        master_address = await self.sentinel_manager.discover_master(self.service_name)
        if self.is_master:
            if self.master_address != master_address:
                self.master_address = master_address
                # disconnect any idle connections so that they reconnect
                # to the new master the next time that they are used.
                await self.disconnect(inuse_connections=False)
        return master_address

    async def rotate_slaves(self) -> AsyncIterator:
        """Round-robin slave balancer"""
        slaves = await self.sentinel_manager.discover_slaves(self.service_name)
        if slaves:
            if self.slave_rr_counter is None:
                self.slave_rr_counter = random.randint(0, len(slaves) - 1)
            for _ in range(len(slaves)):
                self.slave_rr_counter = (self.slave_rr_counter + 1) % len(slaves)
                slave = slaves[self.slave_rr_counter]
                yield slave
        # Fallback to the master connection
        try:
            yield await self.get_master_address()
        except MasterNotFoundError:
            pass
        raise SlaveNotFoundError(f"No slave found for {self.service_name!r}")

    async def get_connection(
        self, command_name: str, *keys: Any, **options: Any
    ) -> SentinelManagedConnection:
        """
        Get a connection from the pool.
        `xxx_scan_iter` commands needs to be handled specially.
        If the client is created using a connection pool, in replica mode,
        all `scan` command-equivalent of the `xxx_scan_iter` commands needs
        to be issued to the same Redis replica.

        The way each server positions each key is different with one another,
        and the cursor acts as the 'offset' of the scan.
        Hence, all scans coming from a single xxx_scan_iter_channel command
        should go to the same replica.
        """
        # If not an iter command or in master mode, call super()
        # No custom logic for master, because there's only 1 master.
        # The bug is only when Redis has the possibility to connect to multiple replicas
        if not (iter_req_id := options.get("_iter_req_id", None)) or self.is_master:
            return await super().get_connection(command_name, *keys, **options)

        # Check if this iter request has already been directed to a particular server
        # Check if this iter request has already been directed to a particular server
        (
            server_host,
            server_port,
        ) = self._request_id_to_replica_address.get(iter_req_id, (None, None))
        connection = None
        # If this is the first scan request of the iter command,
        # get a connection from the pool
        if server_host is None or server_port is None:
            try:
                connection = self._available_connections.pop()
            except IndexError:
                connection = self.make_connection()
        # If this is not the first scan request of the iter command
        else:
            # Check from the available connections, if any of the connection
            # is connected to the host and port that we want
            # If yes, use that connection
            for available_connection in self._available_connections.copy():
                if (
                    available_connection.host == server_host
                    and available_connection.port == server_port
                ):
                    self._available_connections.remove(available_connection)
                    connection = available_connection
            # If not, make a new dummy connection object, and set its host and port
            # to the one that we want later in the call to ``connect_to_address``
            if not connection:
                connection = self.make_connection()
        assert connection
        self._in_use_connections.add(connection)
        try:
            # ensure this connection is connected to Redis
            # If this is the first scan request,
            # just call the SentinelManagedConnection.connect()
            # This will call rotate_slaves
            # and connect to a random replica
            if server_port is None or server_port is None:
                await connection.connect()
            # If this is not the first scan request,
            # connect to the particular address and port
            else:
                # This will connect to the host and port that we've specified above
                await connection.connect_to_address(server_host, server_port)
            # connections that the pool provides should be ready to send
            # a command. if not, the connection was either returned to the
            # pool before all data has been read or the socket has been
            # closed. either way, reconnect and verify everything is good.
            try:
                if await connection.can_read_destructive():
                    raise ConnectionError("Connection has data") from None
            except (ConnectionError, OSError):
                await connection.disconnect()
                await connection.connect()
                if await connection.can_read_destructive():
                    raise ConnectionError("Connection not ready") from None
        except BaseException:
            # release the connection back to the pool so that we don't
            # leak it
            await self.release(connection)
            raise
        # Store the connection to the dictionary
        self._request_id_to_replica_address[iter_req_id] = (
            connection.host,
            connection.port,
        )

        return connection


class Sentinel(AsyncSentinelCommands):
    """
    Redis Sentinel cluster client

    >>> from redis.sentinel import Sentinel
    >>> sentinel = Sentinel([('localhost', 26379)], socket_timeout=0.1)
    >>> master = sentinel.master_for('mymaster', socket_timeout=0.1)
    >>> await master.set('foo', 'bar')
    >>> slave = sentinel.slave_for('mymaster', socket_timeout=0.1)
    >>> await slave.get('foo')
    b'bar'

    ``sentinels`` is a list of sentinel nodes. Each node is represented by
    a pair (hostname, port).

    ``min_other_sentinels`` defined a minimum number of peers for a sentinel.
    When querying a sentinel, if it doesn't meet this threshold, responses
    from that sentinel won't be considered valid.

    ``sentinel_kwargs`` is a dictionary of connection arguments used when
    connecting to sentinel instances. Any argument that can be passed to
    a normal Redis connection can be specified here. If ``sentinel_kwargs`` is
    not specified, any socket_timeout and socket_keepalive options specified
    in ``connection_kwargs`` will be used.

    ``connection_kwargs`` are keyword arguments that will be used when
    establishing a connection to a Redis server.
    """

    def __init__(
        self,
        sentinels,
        min_other_sentinels=0,
        sentinel_kwargs=None,
        **connection_kwargs,
    ):
        # if sentinel_kwargs isn't defined, use the socket_* options from
        # connection_kwargs
        if sentinel_kwargs is None:
            sentinel_kwargs = {
                k: v for k, v in connection_kwargs.items() if k.startswith("socket_")
            }
        self.sentinel_kwargs = sentinel_kwargs

        self.sentinels = [
            Redis(host=hostname, port=port, **self.sentinel_kwargs)
            for hostname, port in sentinels
        ]
        self.min_other_sentinels = min_other_sentinels
        self.connection_kwargs = connection_kwargs

    async def execute_command(self, *args, **kwargs):
        """
        Execute Sentinel command in sentinel nodes.
        once - If set to True, then execute the resulting command on a single
               node at random, rather than across the entire sentinel cluster.
        """
        kwargs.pop("keys", None)  # the keys are used only for client side caching
        once = bool(kwargs.get("once", False))
        if "once" in kwargs.keys():
            kwargs.pop("once")

        if once:
            await random.choice(self.sentinels).execute_command(*args, **kwargs)
        else:
            tasks = [
                asyncio.Task(sentinel.execute_command(*args, **kwargs))
                for sentinel in self.sentinels
            ]
            await asyncio.gather(*tasks)
        return True

    def __repr__(self):
        sentinel_addresses = []
        for sentinel in self.sentinels:
            sentinel_addresses.append(
                f"{sentinel.connection_pool.connection_kwargs['host']}:"
                f"{sentinel.connection_pool.connection_kwargs['port']}"
            )
        return (
            f"<{self.__class__}.{self.__class__.__name__}"
            f"(sentinels=[{','.join(sentinel_addresses)}])>"
        )

    def check_master_state(self, state: dict, service_name: str) -> bool:
        if not state["is_master"] or state["is_sdown"] or state["is_odown"]:
            return False
        # Check if our sentinel doesn't see other nodes
        if state["num-other-sentinels"] < self.min_other_sentinels:
            return False
        return True

    async def discover_master(self, service_name: str):
        """
        Asks sentinel servers for the Redis master's address corresponding
        to the service labeled ``service_name``.

        Returns a pair (address, port) or raises MasterNotFoundError if no
        master is found.
        """
        collected_errors = list()
        for sentinel_no, sentinel in enumerate(self.sentinels):
            try:
                masters = await sentinel.sentinel_masters()
            except (ConnectionError, TimeoutError) as e:
                collected_errors.append(f"{sentinel} - {e!r}")
                continue
            state = masters.get(service_name)
            if state and self.check_master_state(state, service_name):
                # Put this sentinel at the top of the list
                self.sentinels[0], self.sentinels[sentinel_no] = (
                    sentinel,
                    self.sentinels[0],
                )
                return state["ip"], state["port"]

        error_info = ""
        if len(collected_errors) > 0:
            error_info = f" : {', '.join(collected_errors)}"
        raise MasterNotFoundError(f"No master found for {service_name!r}{error_info}")

    def filter_slaves(
        self, slaves: Iterable[Mapping]
    ) -> Sequence[Tuple[EncodableT, EncodableT]]:
        """Remove slaves that are in an ODOWN or SDOWN state"""
        slaves_alive = []
        for slave in slaves:
            if slave["is_odown"] or slave["is_sdown"]:
                continue
            slaves_alive.append((slave["ip"], slave["port"]))
        return slaves_alive

    async def discover_slaves(
        self, service_name: str
    ) -> Sequence[Tuple[EncodableT, EncodableT]]:
        """Returns a list of alive slaves for service ``service_name``"""
        for sentinel in self.sentinels:
            try:
                slaves = await sentinel.sentinel_slaves(service_name)
            except (ConnectionError, ResponseError, TimeoutError):
                continue
            slaves = self.filter_slaves(slaves)
            if slaves:
                return slaves
        return []

    def master_for(
        self,
        service_name: str,
        redis_class: Type[Redis] = Redis,
        connection_pool_class: Type[SentinelConnectionPool] = SentinelConnectionPool,
        **kwargs,
    ):
        """
        Returns a redis client instance for the ``service_name`` master.

        A :py:class:`~redis.sentinel.SentinelConnectionPool` class is
        used to retrieve the master's address before establishing a new
        connection.

        NOTE: If the master's address has changed, any cached connections to
        the old master are closed.

        By default clients will be a :py:class:`~redis.Redis` instance.
        Specify a different class to the ``redis_class`` argument if you
        desire something different.

        The ``connection_pool_class`` specifies the connection pool to
        use.  The :py:class:`~redis.sentinel.SentinelConnectionPool`
        will be used by default.

        All other keyword arguments are merged with any connection_kwargs
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.
        """
        kwargs["is_master"] = True
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)

        connection_pool = connection_pool_class(service_name, self, **connection_kwargs)
        # The Redis object "owns" the pool
        return redis_class.from_pool(connection_pool)

    def slave_for(
        self,
        service_name: str,
        redis_class: Type[Redis] = Redis,
        connection_pool_class: Type[SentinelConnectionPool] = SentinelConnectionPool,
        **kwargs,
    ):
        """
        Returns redis client instance for the ``service_name`` slave(s).

        A SentinelConnectionPool class is used to retrieve the slave's
        address before establishing a new connection.

        By default clients will be a :py:class:`~redis.Redis` instance.
        Specify a different class to the ``redis_class`` argument if you
        desire something different.

        The ``connection_pool_class`` specifies the connection pool to use.
        The SentinelConnectionPool will be used by default.

        All other keyword arguments are merged with any connection_kwargs
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.
        """
        kwargs["is_master"] = False
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)

        connection_pool = connection_pool_class(service_name, self, **connection_kwargs)
        # The Redis object "owns" the pool
        return redis_class.from_pool(connection_pool)
