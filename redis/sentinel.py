import random
import weakref
from collections import defaultdict
from typing import Any, Iterable, Optional

from redis.client import Redis
from redis.commands import SentinelCommands
from redis.connection import Connection, ConnectionPool, SSLConnection
from redis.exceptions import ConnectionError, ReadOnlyError, ResponseError, TimeoutError
from redis.utils import str_if_bytes


class MasterNotFoundError(ConnectionError):
    pass


class SlaveNotFoundError(ConnectionError):
    pass


class SentinelManagedConnection(Connection):
    def __init__(self, **kwargs):
        self.connection_pool = kwargs.pop("connection_pool")
        # To be set to True if we want to prevent
        # the sentinel managed connection to connect
        # to the most relevant sentinel in the pool and just
        # connect to the current self.host and self.port
        self._is_address_set = False
        super().__init__(**kwargs)

    def __repr__(self):
        pool = self.connection_pool
        s = (
            f"<{type(self).__module__}.{type(self).__name__}"
            f"(service={pool.service_name}%s)>"
        )
        if self.host:
            host_info = f",host={self.host},port={self.port}"
            s = s % host_info
        return s

    def set_address(self, address):
        """
        By setting the address, the connection will just connect
        to the current host and port the next time connect is called.
        """
        self.host, self.port = address
        self._is_address_set = True

    def connect_to(self, address):
        self.host, self.port = address
        super().connect()
        if self.connection_pool.check_connection:
            self.send_command("PING")
            if str_if_bytes(self.read_response()) != "PONG":
                raise ConnectionError("PING failed")

    def _connect_retry(self):
        if self._sock:
            return  # already connected
        # If address is set, it means that the connection
        # will just connect to the current host and port.
        if self._is_address_set:
            self.connect_to((self.host, self.port))
            return
        self._connect_to_sentinel()

    def _connect_to_sentinel(self):
        if self.connection_pool.is_master:
            self.connect_to(self.connection_pool.get_master_address())
        else:
            for slave in self.connection_pool.rotate_slaves():
                try:
                    return self.connect_to(slave)
                except ConnectionError:
                    continue
            raise SlaveNotFoundError  # Never be here

    def connect(self):
        return self.retry.call_with_retry(
            lambda: self._connect_retry(), lambda error: None
        )

    def read_response(
        self,
        disable_decoding=False,
        *,
        disconnect_on_error: Optional[bool] = False,
        push_request: Optional[bool] = False,
    ):
        try:
            return super().read_response(
                disable_decoding=disable_decoding,
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
                self.disconnect()
                raise ConnectionError("The previous master is now a slave")
            raise


class SentinelManagedSSLConnection(SentinelManagedConnection, SSLConnection):
    pass


class SentinelConnectionPoolProxy:
    def __init__(
        self,
        connection_pool,
        is_master,
        check_connection,
        service_name,
        sentinel_manager,
    ):
        self.connection_pool_ref = weakref.ref(connection_pool)
        self.is_master = is_master
        self.check_connection = check_connection
        self.service_name = service_name
        self.sentinel_manager = sentinel_manager
        self.reset()

    def reset(self):
        self.master_address = None
        self.slave_rr_counter = None

    def get_master_address(self):
        master_address = self.sentinel_manager.discover_master(self.service_name)
        if self.is_master and self.master_address != master_address:
            self.master_address = master_address
            # disconnect any idle connections so that they reconnect
            # to the new master the next time that they are used.
            connection_pool = self.connection_pool_ref()
            if connection_pool is not None:
                connection_pool.disconnect(inuse_connections=False)
        return master_address

    def rotate_slaves(self):
        slaves = self.sentinel_manager.discover_slaves(self.service_name)
        if slaves:
            if self.slave_rr_counter is None:
                self.slave_rr_counter = random.randint(0, len(slaves) - 1)
            for _ in range(len(slaves)):
                self.slave_rr_counter = (self.slave_rr_counter + 1) % len(slaves)
                slave = slaves[self.slave_rr_counter]
                yield slave
        # Fallback to the master connection
        try:
            yield self.get_master_address()
        except MasterNotFoundError:
            pass
        raise SlaveNotFoundError(f"No slave found for {self.service_name!r}")


class ConnectionsIndexer(Iterable):
    """
    Data structure that stores available connections in a pool.
    Instead of list, we keep 2 additional DS to support O(1) operations
    on all of the class' methods.
    The first DS is indexed on the connection object's ID.
    The second DS is indexed on the address (ip and port) of the connection.
    """

    def __init__(self):
        # Map the id to the connection object
        self._id_to_connection = {}
        # Map the address to a dictionary of connections
        # The inner dictionary is a map between the object id to the object itself
        # Both of these DS support O(1) operations on all of the class' methods
        self._address_to_connections = defaultdict(dict)

    def pop(self):
        try:
            _, connection = self._id_to_connection.popitem()
            del self._address_to_connections[(connection.host, connection.port)][
                id(connection)
            ]
        except KeyError:
            # We are simulating a list, hence we raise IndexError
            # when there's no item in the dictionary
            raise IndexError()
        return connection

    def append(self, connection: Connection):
        self._id_to_connection[id(connection)] = connection
        self._address_to_connections[(connection.host, connection.port)][
            id(connection)
        ] = connection

    def get_connection(self, host: str, port: int):
        try:
            _, connection = self._address_to_connections[(host, port)].popitem()
            del self._id_to_connection[id(connection)]
        except KeyError:
            return None
        return connection

    def __iter__(self):
        # This is an O(1) operation in python3.7 and later
        return iter(self._id_to_connection.values())

    def __len__(self):
        return len(self._id_to_connection)


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
        self.proxy = SentinelConnectionPoolProxy(
            connection_pool=self,
            is_master=self.is_master,
            check_connection=self.check_connection,
            service_name=service_name,
            sentinel_manager=sentinel_manager,
        )
        super().__init__(**kwargs)
        self.connection_kwargs["connection_pool"] = self.proxy
        self.service_name = service_name
        self.sentinel_manager = sentinel_manager
        self._iter_req_id_to_replica_address = {}

    def __repr__(self):
        role = "master" if self.is_master else "slave"
        return (
            f"<{type(self).__module__}.{type(self).__name__}"
            f"(service={self.service_name}({role}))>"
        )

    def reset(self):
        super().reset()
        self.proxy.reset()

    def reset_available_connections(self):
        return ConnectionsIndexer()

    @property
    def master_address(self):
        return self.proxy.master_address

    def owns_connection(self, connection):
        check = not self.is_master or (
            self.is_master and self.master_address == (connection.host, connection.port)
        )
        parent = super()
        return check and parent.owns_connection(connection)

    def get_master_address(self):
        return self.proxy.get_master_address()

    def rotate_slaves(self):
        "Round-robin slave balancer"
        return self.proxy.rotate_slaves()

    def cleanup(self, **options):
        """
        Remove the SCAN ITER family command's request id from the dictionary
        """
        self._iter_req_id_to_replica_address.pop(options.get("iter_req_id", None), None)

    def get_connection(
        self, command_name: str, *keys: Any, **options: Any
    ) -> SentinelManagedConnection:
        """
        Get a connection from the pool.
        'xxxscan_iter' ('scan_iter', 'hscan_iter', 'sscan_iter', 'zscan_iter')
        commands needs to be handled specially.
        If the client is created using a connection pool, in replica mode,
        all 'scan' command-equivalent of the 'xxx_scan_iter' commands needs
        to be issued to the same Redis replica.

        The way each server positions each key is different with one another,
        and the cursor acts as the offset of the scan.
        Hence, all scans coming from a single 'xxx_scan_iter_channel' command
        should go to the same replica.
        """
        # If not an iter command or in master mode, call superclass' implementation
        if not (iter_req_id := options.get("iter_req_id", None)) or self.is_master:
            return super().get_connection(command_name, *keys, **options)

        # Check if this iter request has already been directed to a particular server
        (
            server_host,
            server_port,
        ) = self._iter_req_id_to_replica_address.get(iter_req_id, (None, None))
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
            # Get the connection that has the same host and port
            connection = self._available_connections.get_connection(
                host=server_host, port=server_port
            )
            # If not, make a new dummy connection object, and set its host and port
            # to the one that we want later in the call to ``set_address``
            if not connection:
                connection = self.make_connection()
        assert connection
        self._in_use_connections.add(connection)
        try:
            # Ensure this connection is connected to Redis
            # If this is the first scan request, it will
            # call rotate_slaves and connect to a random replica
            if server_port is None or server_port is None:
                connection.connect()
            # If this is not the first scan request,
            # connect to the previous replica.
            # This will connect to the host and port of the replica
            else:
                connection.set_address((server_host, server_port))
            self.ensure_connection(connection)
        except BaseException:
            # Release the connection back to the pool so that we don't
            # leak it
            self.release(connection)
            raise
        # Store the connection to the dictionary
        self._iter_req_id_to_replica_address[iter_req_id] = (
            connection.host,
            connection.port,
        )
        return connection


class Sentinel(SentinelCommands):
    """
    Redis Sentinel cluster client

    >>> from redis.sentinel import Sentinel
    >>> sentinel = Sentinel([('localhost', 26379)], socket_timeout=0.1)
    >>> master = sentinel.master_for('mymaster', socket_timeout=0.1)
    >>> master.set('foo', 'bar')
    >>> slave = sentinel.slave_for('mymaster', socket_timeout=0.1)
    >>> slave.get('foo')
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
            Redis(hostname, port, **self.sentinel_kwargs)
            for hostname, port in sentinels
        ]
        self.min_other_sentinels = min_other_sentinels
        self.connection_kwargs = connection_kwargs

    def execute_command(self, *args, **kwargs):
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
            random.choice(self.sentinels).execute_command(*args, **kwargs)
        else:
            for sentinel in self.sentinels:
                sentinel.execute_command(*args, **kwargs)
        return True

    def __repr__(self):
        sentinel_addresses = []
        for sentinel in self.sentinels:
            sentinel_addresses.append(
                "{host}:{port}".format_map(sentinel.connection_pool.connection_kwargs)
            )
        return (
            f"<{type(self).__module__}.{type(self).__name__}"
            f'(sentinels=[{",".join(sentinel_addresses)}])>'
        )

    def check_master_state(self, state, service_name):
        if not state["is_master"] or state["is_sdown"] or state["is_odown"]:
            return False
        # Check if our sentinel doesn't see other nodes
        if state["num-other-sentinels"] < self.min_other_sentinels:
            return False
        return True

    def discover_master(self, service_name):
        """
        Asks sentinel servers for the Redis master's address corresponding
        to the service labeled ``service_name``.

        Returns a pair (address, port) or raises MasterNotFoundError if no
        master is found.
        """
        collected_errors = list()
        for sentinel_no, sentinel in enumerate(self.sentinels):
            try:
                masters = sentinel.sentinel_masters()
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

    def filter_slaves(self, slaves):
        "Remove slaves that are in an ODOWN or SDOWN state"
        slaves_alive = []
        for slave in slaves:
            if slave["is_odown"] or slave["is_sdown"]:
                continue
            slaves_alive.append((slave["ip"], slave["port"]))
        return slaves_alive

    def discover_slaves(self, service_name):
        "Returns a list of alive slaves for service ``service_name``"
        for sentinel in self.sentinels:
            try:
                slaves = sentinel.sentinel_slaves(service_name)
            except (ConnectionError, ResponseError, TimeoutError):
                continue
            slaves = self.filter_slaves(slaves)
            if slaves:
                return slaves
        return []

    def master_for(
        self,
        service_name,
        redis_class=Redis,
        connection_pool_class=SentinelConnectionPool,
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
        return redis_class.from_pool(
            connection_pool_class(service_name, self, **connection_kwargs)
        )

    def slave_for(
        self,
        service_name,
        redis_class=Redis,
        connection_pool_class=SentinelConnectionPool,
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
        return redis_class.from_pool(
            connection_pool_class(service_name, self, **connection_kwargs)
        )
