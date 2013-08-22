import random

from redis.client import StrictRedis
from redis.connection import ConnectionPool, Connection
from redis.exceptions import ConnectionError, ResponseError


class MasterNotFoundError(ConnectionError):
    pass


class SlaveNotFoundError(ConnectionError):
    pass


class SentinelManagedConnection(Connection):
    def __init__(self, **kwargs):
        self.connection_pool = kwargs.pop('connection_pool')
        super(SentinelManagedConnection, self).__init__(**kwargs)

    def connect(self):
        self.host, self.port = self.connection_pool.discover()
        super(SentinelManagedConnection, self).connect()


class SentinelConnectionPool(ConnectionPool):
    def __init__(self, service_name, sentinel_manager, **kwargs):
        kwargs['connection_class'] = kwargs.get(
            'connection_class', SentinelManagedConnection)
        self.is_master = kwargs.pop('is_master', True)
        super(SentinelConnectionPool, self).__init__(**kwargs)
        self.connection_kwargs['connection_pool'] = self
        self.service_name = service_name
        self.master_address = None
        self.sentinel_manager = sentinel_manager

    def discover_master(self):
        master_address = self.sentinel_manager.discover_master(
            self.service_name)
        if self.master_address is None:
            self.master_address = master_address
        elif master_address != self.master_address:
            # Master address change disconnect
            self.disconnect()
        return master_address

    def discover_slave(self):
        return self.sentinel_manager.discover_slave(self.service_name)

    def discover(self):
        if self.is_master:
            return self.discover_master()
        else:
            return self.discover_slave()


class Sentinel(object):
    def __init__(self, sentinels, password=None, socket_timeout=None,
                 min_other_sentinels=0):
        self.sentinels = [StrictRedis(hostname, port, password=password,
                                      socket_timeout=socket_timeout)
                          for hostname, port in sentinels]
        self.min_other_sentinels = min_other_sentinels

    def check_master_state(self, state, service_name):
        if not state['is_master'] or state['is_sdown'] or state['is_odown']:
            return False
        # Check whether our sentinel doesn't see others
        if state['num-other-sentinels'] < self.min_other_sentinels:
            return False
        return True

    def discover_master(self, service_name):
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

    def choose_slave(self, slaves):
        slaves_alive = []
        for slave in slaves:
            if slave['is_odown'] or slave['is_sdown']:
                continue
            slaves_alive.append(slave)
        if slaves_alive:
            return random.choice(slaves_alive)

    def discover_slave(self, service_name, use_master=True):
        for sentinel in self.sentinels:
            try:
                slaves = sentinel.sentinel_slaves(service_name)
            except (ConnectionError, ResponseError):
                continue
            slave = self.choose_slave(slaves)
            if slave is not None:
                return slave['ip'], slave['port']
        if use_master:
            try:
                return self.discover_master(service_name)
            except MasterNotFoundError:
                pass
        raise SlaveNotFoundError("No slave found for %r" % (service_name,))

    def master_for(self, service_name, redis_class=StrictRedis, **kwargs):
        kwargs['is_master'] = True
        connection_pool = SentinelConnectionPool(service_name, self, **kwargs)
        return redis_class(connection_pool=connection_pool)

    def slave_for(self, service_name, redis_class=StrictRedis, **kwargs):
        kwargs['is_master'] = False
        connection_pool = SentinelConnectionPool(service_name, self, **kwargs)
        return redis_class(connection_pool=connection_pool)
