#!/usr/bin/env python
from redis.sentinel import Sentinel


SSL_ROOT='/Source/ssl_certs'
# Path for certificate, CA, and key
SSL_ARGS = {
    'ssl_keyfile': SSL_ROOT + '/client.key',
    'ssl_certfile': SSL_ROOT + '/client.pem',
    'ssl_ca_certs': SSL_ROOT + '/rootCA.pem'
}

# Path for sentinel itself. This contains the SSL sentinel
ss = Sentinel([('localhost', 18001)], ssl_args=SSL_ARGS, socket_timeout=0.1)
ss.discover_master('db1')
c = ss.master_for('db1')
c.execute_command('INFO')
c.set('foo', 'bar')
print c.get('foo')