from __future__ import unicode_literals


class TestPipeline(object):
    def test_monitor(self, r):
        with r.monitor() as m:
            r.ping()
            response = m.next_command()
            assert set(response.keys()) == {'time', 'db', 'client_address',
                                            'client_port', 'command'}
