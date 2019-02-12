import pytest
import time
from redis import selector

_SELECTORS = (
        'SelectSelector',
        'PollSelector',
)


@pytest.mark.parametrize('selector_name', _SELECTORS)
class TestSelector(object):

    @pytest.fixture()
    def selector_patch(self, selector_name, request):
        "A fixture to patch the DefaultSelector with each selector"
        if not hasattr(selector, selector_name):
            pytest.skip('selector %s unavailable' % selector_name)
        default_selector = selector._DEFAULT_SELECTOR

        def revert_selector():
            selector._DEFAULT_SELECTOR = default_selector
        request.addfinalizer(revert_selector)

        selector._DEFAULT_SELECTOR = getattr(selector, selector_name)

    def kill_connection(self, connection, r):
        "Helper that tells the redis server to kill `connection`"
        # set a name for the connection so that we can identify and kill it
        connection.send_command('client', 'setname', 'redis-py-1')
        assert connection.read_response() == b'OK'

        # find the client based on its name and kill it
        for client in r.client_list():
            if client['name'] == 'redis-py-1':
                assert r.client_kill(client['addr'])
                break
        else:
            assert False, 'Client redis-py-1 not found in client list'

    def test_can_read(self, selector_patch, r):
        c = r.connection_pool.get_connection('_')

        # a fresh connection should not be readable
        assert not c.can_read()

        c.send_command('PING')
        # a connection should be readable when a response is available
        # note that we supply a timeout here to make sure the server has
        # a chance to respond
        assert c.can_read(1.0)

        assert c.read_response() == b'PONG'

        # once the response is read, the connection is no longer readable
        assert not c.can_read()

    def test_is_ready_for_command(self, selector_patch, r):
        c = r.connection_pool.get_connection('_')

        # a fresh connection should be ready for a new command
        assert c.is_ready_for_command()

        c.send_command('PING')
        # once the server replies with a response, the selector should report
        # that the connection is no longer ready since there is data that
        # can be read. note that we need to wait for the server to respond
        wait_until = time.time() + 2
        while time.time() < wait_until:
            if not c.is_ready_for_command():
                break
            time.sleep(0.01)

        assert not c.is_ready_for_command()

        assert c.read_response() == b'PONG'

        # once the response is read, the connection should be ready again
        assert c.is_ready_for_command()

    def test_killed_connection_no_longer_ready(self, selector_patch, r):
        "A connection that becomes disconnected is no longer ready"
        c = r.connection_pool.get_connection('_')
        # the connection should start as ready
        assert c.is_ready_for_command()

        self.kill_connection(c, r)

        # the selector should immediately report that the socket is no
        # longer ready
        assert not c.is_ready_for_command()

    def test_pool_restores_killed_connection(self, selector_patch, r2):
        """
        The ConnectionPool only returns healthy connecdtions, even if the
        connection was killed while idle in the pool.
        """
        # r2 provides two separate clients/connection pools
        r = r2[0]
        c = r.connection_pool.get_connection('_')
        c._test_client = True
        # the connection should start as ready
        assert c.is_ready_for_command()

        # release the connection back to the pool
        r.connection_pool.release(c)

        # kill the connection that is now idle in the pool
        # use the second redis client/pool instance run the kill command
        # such that it doesn't manipulate the primary connection pool
        self.kill_connection(c, r2[1])

        assert not c.is_ready_for_command()

        # retrieving the connection from the pool should provide us with
        # the same connection we were previously using and it should now
        # be ready for a command
        c2 = r.connection_pool.get_connection('_')
        assert c2 == c
        assert c2._test_client is True

        assert c.is_ready_for_command()
