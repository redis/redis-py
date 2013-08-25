import random

from redis.client import StrictRedis
from redis.connection import ConnectionPool, Connection
from redis.exceptions import ConnectionError, ResponseError
from redis._compat import xrange


class MasterNotFoundError(ConnectionError):
    pass


class SlaveNotFoundError(ConnectionError):
    pass


class SentinelManagedConnection(Connection):
    def __init__(self, **kwargs):
        self.connection_pool = kwargs.pop('connection_pool')
        super(SentinelManagedConnection, self).__init__(**kwargs)

    def connect(self):
        if self._sock:
            return # already connected
        if self.connection_pool.is_master:
            self.host, self.port = self.connection_pool.get_master_address()
            super(SentinelManagedConnection, self).connect()
        else:
            for slave in self.connection_pool.rotate_slaves():
                self.host, self.port = slave
                try:
                    return super(SentinelManagedConnection, self).connect()
                except ConnectionError:
                    continue
            raise SlaveNotFoundError # Never be here


class SentinelConnectionPool(ConnectionPool):
    def __init__(self, service_name, sentinel_manager, **kwargs):
        kwargs['connection_class'] = kwargs.get(
            'connection_class', SentinelManagedConnection)
        self.is_master = kwargs.pop('is_master', True)
        super(SentinelConnectionPool, self).__init__(**kwargs)
        self.connection_kwargs['connection_pool'] = self
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
            self.disconnect() # Master address changed
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
        raise SlaveNotFoundError('No slave found for %r' % (self.service_name))


class Sentinel(object):
    """
    Redis Sentinel cluster client::

    >>> from redis.sentinel import Sentinel
    >>> sentinel = Sentinel([('localhost', 26379)], socket_timeout=0.1)
    >>> master = sentinel.master_for('mymaster', socket_timeout=0.1)
    >>> master.set('foo', 'bar')
    >>> slave = sentinel.slave_for('mymaster', socket_timeout=0.1)
    >>> slave.get('foo')
    'bar'

    First required argument ``sentinels`` is a list of sentinel nodes each node
    is represented by a pair (hostname, port).

    Use ``socket_timeout`` to specify timeout for underlying sentinel clients,
    it's recommended to use short timeouts.

    Use ``min_other_sentinels`` to filter out sentinels with not enough peers.
    """

    def __init__(self, sentinels, password=None, socket_timeout=None,
                 min_other_sentinels=0):
        self.sentinels = [StrictRedis(hostname, port, password=password,
                                      socket_timeout=socket_timeout)
                          for hostname, port in sentinels]
        self.min_other_sentinels = min_other_sentinels

    def check_master_state(self, state, service_name):
        if not state['is_master'] or state['is_sdown'] or state['is_odown']:
            return False
        # Check if our sentinel doesn't see other nodes
        if state['num-other-sentinels'] < self.min_other_sentinels:
            return False
        return True

    def discover_master(self, service_name):
        """
        Asks sentinels for master's address corresponding to the service
        labeled ``service_name``.

        Returns a pair (address, port) or raises MasterNotFoundError if no alive
        master is found.
        """
        for sentinel_no, sentinel in enumerate(self.sentinels):
            try:
                masters = sentinel.sentinel_masters()
            except ConnectionError:
                continue
            state = masters.get(service_name)
            if state and self.check_master_state(state, service_name):
                # Put this sentinel at the top of the list
                self.sentinels[0], self.sentinels[sentinel_no] = (
                    sentinel, self.sentinels[0])
                return state['ip'], state['port']
        raise MasterNotFoundError("No master found for %r" % (service_name,))

    def filter_slaves(self, slaves):
        "Remove slaves that are in ODOWN or SDOWN state"
        slaves_alive = []
        for slave in slaves:
            if slave['is_odown'] or slave['is_sdown']:
                continue
            slaves_alive.append((slave['ip'], slave['port']))
        return slaves_alive

    def discover_slaves(self, service_name):
        "Returns list of alive slaves for service ``service_name``"
        for sentinel in self.sentinels:
            try:
                slaves = sentinel.sentinel_slaves(service_name)
            except (ConnectionError, ResponseError):
                continue
            slaves = self.filter_slaves(slaves)
            if slaves:
                return slaves
        return []

    def master_for(self, service_name, redis_class=StrictRedis, **kwargs):
        """
        Returns redis client instance for master of ``service_name``.

        Undercover it uses SentinelConnectionPool class to retrive master's
        address each time before establishing new connection.

        NOTE: If master address change is detected all other connections from
        the pool are closed.

        By default redis.StrictRedis class is used you can override this with
        ``redis_class`` argument. All other arguments are passed directly to
        the SentinelConnectionPool.
        """
        kwargs['is_master'] = True
        connection_pool = SentinelConnectionPool(service_name, self, **kwargs)
        return redis_class(connection_pool=connection_pool)

    def slave_for(self, service_name, redis_class=StrictRedis, **kwargs):
        """
        Returns redis client instance for slave of ``service_name``.

        Undercover it uses SentinelConnectionPool class to choose slave's
        address each time before establishing new connection.

        By default redis.StrictRedis class is used you can override this with
        ``redis_class`` argument. All other arguments are passed directly to
        the SentinelConnectionPool.
        """
        kwargs['is_master'] = False
        connection_pool = SentinelConnectionPool(service_name, self, **kwargs)
        return redis_class(connection_pool=connection_pool)
