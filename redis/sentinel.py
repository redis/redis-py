import os
import random
import weakref

from redis.client import StrictRedis
from redis.connection import ConnectionPool, Connection
from redis.exceptions import ConnectionError, ResponseError
from redis._compat import xrange, nativestr


class MasterNotFoundError(ConnectionError):
    pass


class SlaveNotFoundError(ConnectionError):
    pass


class SentinelManagedConnection(Connection):
    def __init__(self, **kwargs):
        self.connection_pool = kwargs.pop('connection_pool')
        super(SentinelManagedConnection, self).__init__(**kwargs)

    def connect_to(self, address):
        self.host, self.port = address
        super(SentinelManagedConnection, self).connect()
        if self.connection_pool.check_connection:
            self.send_command('PING')
            if nativestr(self.read_response()) != 'PONG':
                raise ConnectionError('PING failed')

    def connect(self):
        if self._sock:
            return  # already connected
        if self.connection_pool.is_master:
            self.connect_to(self.connection_pool.get_master_address())
        else:
            for slave in self.connection_pool.rotate_slaves():
                try:
                    return self.connect_to(slave)
                except ConnectionError:
                    continue
            raise SlaveNotFoundError  # Never be here


class SentinelConnectionPool(ConnectionPool):
    """
    Sentinel backed connection pool.

    If ``check_connection`` flag is set to True, SentinelManagedConnection
    sends a PING command right after establishing the connection.
    """

    def __init__(self, service_name, sentinel_manager, **kwargs):
        kwargs['connection_class'] = kwargs.get(
            'connection_class', SentinelManagedConnection)
        self.is_master = kwargs.pop('is_master', True)
        self.check_connection = kwargs.pop('check_connection', False)
        super(SentinelConnectionPool, self).__init__(**kwargs)
        self.connection_kwargs['connection_pool'] = weakref.proxy(self)
        self.service_name = service_name
        self.sentinel_manager = sentinel_manager
        self.master_address = None
        self.slave_rr_counter = None

    def get_master_address(self):
        master_address = self.sentinel_manager.discover_master(
            self.service_name)
        if not self.is_master:
            pass
        elif self.master_address is None:
            self.master_address = master_address
        elif master_address != self.master_address:
            self.disconnect()  # Master address changed
        return master_address

    def rotate_slaves(self):
        "Round-robin slave balancer"
        slaves = self.sentinel_manager.discover_slaves(self.service_name)
        if slaves:
            if self.slave_rr_counter is None:
                self.slave_rr_counter = random.randint(0, len(slaves) - 1)
            for _ in xrange(len(slaves)):
                self.slave_rr_counter = (
                    self.slave_rr_counter + 1) % len(slaves)
                slave = slaves[self.slave_rr_counter]
                yield slave
        # Fallback to the master connection
        try:
            yield self.get_master_address()
        except MasterNotFoundError:
            pass
        raise SlaveNotFoundError('No slave found for %r' % self.service_name)

    def _checkpid(self):
        if self.pid != os.getpid():
            self.disconnect()
            self.__init__(self.service_name, self.sentinel_manager,
                          connection_class=self.connection_class,
                          max_connections=self.max_connections,
                          **self.connection_kwargs)


class Sentinel(object):
    """
    Redis Sentinel cluster client

    >>> from redis.sentinel import Sentinel
    >>> sentinel = Sentinel([('localhost', 26379)], socket_timeout=0.1)
    >>> master = sentinel.master_for('mymaster', socket_timeout=0.1)
    >>> master.set('foo', 'bar')
    >>> slave = sentinel.slave_for('mymaster', socket_timeout=0.1)
    >>> slave.get('foo')
    'bar'

    ``sentinels`` is a list of sentinel nodes. Each node is represented by
    a pair (hostname, port).

    Use ``socket_timeout`` to specify a timeout for sentinel clients.
    It's recommended to use short timeouts.

    Use ``min_other_sentinels`` to filter out sentinels with not enough peers.
    """

    def __init__(self, sentinels, password=None, socket_timeout=None,
                 min_other_sentinels=0):
        self.sentinels = [StrictRedis(hostname, port, password=password,
                                      socket_timeout=socket_timeout)
                          for hostname, port in sentinels]
        self.min_other_sentinels = min_other_sentinels

    def check_master_state(self, state):
        if not state['is_master'] or state['is_sdown'] or state['is_odown']:
            return False
        # Check if our sentinel doesn't see other nodes
        if state['num-other-sentinels'] < self.min_other_sentinels:
            return False
        return True

    def discover_master(self, service_name):
        """
        Asks sentinel servers for the Redis master's address corresponding
        to the service labeled ``service_name``.

        Returns a pair (address, port) or raises MasterNotFoundError if no
        master is found.
        """
        for sentinel_no, sentinel in enumerate(self.sentinels):
            try:
                masters = sentinel.sentinel_masters()
            except ConnectionError:
                continue
            state = masters.get(service_name)
            if state and self.check_master_state(state):
                # Put this sentinel at the top of the list
                self.sentinels[0], self.sentinels[sentinel_no] = (
                    sentinel, self.sentinels[0])
                return state['ip'], state['port']
        raise MasterNotFoundError("No master found for %r" % (service_name,))

    def filter_slaves(self, slaves):
        "Remove slaves that are in an ODOWN or SDOWN state"
        slaves_alive = []
        for slave in slaves:
            if slave['is_odown'] or slave['is_sdown']:
                continue
            slaves_alive.append((slave['ip'], slave['port']))
        return slaves_alive

    def discover_slaves(self, service_name):
        "Returns a list of alive slaves for service ``service_name``"
        for sentinel in self.sentinels:
            try:
                slaves = sentinel.sentinel_slaves(service_name)
            except (ConnectionError, ResponseError):
                continue
            slaves = self.filter_slaves(slaves)
            if slaves:
                return slaves
        return []

    def master_for(self, service_name, redis_class=StrictRedis,
                   connection_pool_class=SentinelConnectionPool, **kwargs):
        """
        Returns a redis client instance for the ``service_name`` master.

        A SentinelConnectionPool class is used to retrive the master's
        address before establishing a new connection.

        NOTE: If the master's address has changed, any cached connections to
        the old master are closed.

        By default clients will be a redis.StrictRedis instance. Specify a
        different class to the ``redis_class`` argument if you desire
        something different.

        The ``connection_pool_class`` specifies the connection pool to use.
        The SentinelConnectionPool will be used by default.

        All other arguments are passed directly to the SentinelConnectionPool.
        """
        kwargs['is_master'] = True
        return redis_class(connection_pool=connection_pool_class(
            service_name, self, **kwargs))

    def slave_for(self, service_name, redis_class=StrictRedis,
                  connection_pool_class=SentinelConnectionPool, **kwargs):
        """
        Returns redis client instance for the ``service_name`` slave(s).

        A SentinelConnectionPool class is used to retrive the slave's
        address before establishing a new connection.

        By default clients will be a redis.StrictRedis instance. Specify a
        different class to the ``redis_class`` argument if you desire
        something different.

        The ``connection_pool_class`` specifies the connection pool to use.
        The SentinelConnectionPool will be used by default.

        All other arguments are passed directly to the SentinelConnectionPool.
        """
        kwargs['is_master'] = False
        return redis_class(connection_pool=connection_pool_class(
            service_name, self, **kwargs))
