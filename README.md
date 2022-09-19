# redis-py

The Python interface to the Redis key-value store.

[![CI](https://github.com/redis/redis-py/workflows/CI/badge.svg?branch=master)](https://github.com/redis/redis-py/actions?query=workflow%3ACI+branch%3Amaster)
[![docs](https://readthedocs.org/projects/redis/badge/?version=stable&style=flat)](https://redis-py.readthedocs.io/en/stable/)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![pypi](https://badge.fury.io/py/redis.svg)](https://pypi.org/project/redis/)
[![codecov](https://codecov.io/gh/redis/redis-py/branch/master/graph/badge.svg?token=yenl5fzxxr)](https://codecov.io/gh/redis/redis-py)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/redis/redis-py.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/redis/redis-py/alerts/)

[Installation](#installation) |  [Usage](#usage) | [Advanced Topics](#advanced-topics) | [Contributing](https://github.com/redis/redis-py/blob/master/CONTRIBUTING.md)

---------------------------------------------

## Python Notice

redis-py 4.3.x will be the last generation of redis-py to support python 3.6 as it has been [End of Life'd](https://www.python.org/dev/peps/pep-0494/#schedule-last-security-only-release).  Async support was introduced in redis-py 4.2.x thanks to [aioredis](https://github.com/aio-libs/aioredis-py), which necessitates this change. We will continue to maintain 3.6 support as long as possible - but the plan is for redis-py version 4.4+ to officially remove 3.6.

---------------------------

## Installation

To install redis-py, simply:

``` bash
$ pip install redis
```

For faster performance, install redis with hiredis support, this provides a compiled response parser, and *for most cases* requires zero code changes.
By default, if hiredis >= 1.0 is available, redis-py will attempt to use it for response parsing.

``` bash
$ pip install redis[hiredis]
```

Looking for a high-level library to handle object mapping? See [redis-om-python](https://github.com/redis/redis-om-python)!

## Usage

### Basic Example

``` python
>>> import redis
>>> r = redis.Redis(host='localhost', port=6379, db=0)
>>> r.set('foo', 'bar')
True
>>> r.get('foo')
b'bar'
```

The above code connects to localhost on port 6379, sets a value in Redis, and retrieves it. All responses are returned as bytes in Python, to receive decoded strings, set *decode_responses=True*.  For this, and more connection options, see [these examples](https://redis.readthedocs.io/en/stable/examples.html)

### Connection Pools

By default, redis-py uses a connection pool to manage connections. Each instance of a Redis class receives its own connection pool. You can however define your own [redis.ConnectionPool](https://redis.readthedocs.io/en/stable/connections.html#connection-pools)

``` python
>>> pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
>>> r = redis.Redis(connection_pool=pool)
```

Alternatively, you might want to look at [Async connections](https://redis.readthedocs.io/en/stable/examples/asyncio_examples.html), or [Cluster connections](https://redis.readthedocs.io/en/stable/connections.html#cluster-client), or even [Async Cluster connections](https://redis.readthedocs.io/en/stable/connections.html#async-cluster-client)

### Redis Commands

There is built-in support for all of the [out-of-the-box Redis commands](https://redis.io/commands). They are exposed using the raw Redis command names (`HSET`, `HGETALL`, etc.) except where a word (i.e del) is reserved by the language. The complete set of commands can be found [here](https://github.com/redis/redis-py/tree/master/redis/commands), or [the documentation](https://redis.readthedocs.io/en/stable/commands.html).

## Advanced Topics

The [official Redis command documentation](https://redis.io/commands)
does a great job of explaining each command in detail. redis-py attempts
to adhere to the official command syntax. There are a few exceptions:

-   **MULTI/EXEC**: These are implemented as part of the Pipeline class.
    The pipeline is wrapped with the MULTI and EXEC statements by
    default when it is executed, which can be disabled by specifying
    transaction=False. See more about Pipelines below.

-   **SUBSCRIBE/LISTEN**: Similar to pipelines, PubSub is implemented as
    a separate class as it places the underlying connection in a state
    where it can\'t execute non-pubsub commands. Calling the pubsub
    method from the Redis client will return a PubSub instance where you
    can subscribe to channels and listen for messages. You can only call
    PUBLISH from the Redis client (see [this comment on issue
    #151](https://github.com/redis/redis-py/issues/151#issuecomment-1545015)
    for details).

For more details, please see the documentation on [advanced topics page](https://redis.readthedocs.io/en/stable/advanced_features.html).

### Pipelines

The following is a basic example of a [Redis pipeline](https://redis.io/docs/manual/pipelining/), a method to optimize round-trip calls, by batching Redis commands, and receiving their results as a list.


``` python
>>> pipe = r.pipeline()
>>> pipe.set('foo', 5)
>>> pipe.set('bar', 18.5)
>>> pipe.set('blee', "hello world!")
>>> pipe.execute()
[True, True, True]
```

### PubSub

The following example shows how to utilize [Redis Pub/Sub](https://redis.io/docs/manual/pubsub/) to subscribe to specific channels.

``` python
>>> r = redis.Redis(...)
>>> p = r.pubsub()
>>> p.subscribe('my-first-channel', 'my-second-channel', ...)
>>> p.get_message()
{'pattern': None, 'type': 'subscribe', 'channel': b'my-second-channel', 'data': 1}
```


--------------------------

### Author

redis-py is developed and maintained by [Redis Inc](https://redis.com). It can be found [here](
https://github.com/redis/redis-py), or downloaded from [pypi](https://pypi.org/project/redis/).

Special thanks to:

-   Andy McCurdy (<sedrik@gmail.com>) the original author of redis-py.
-   Ludovico Magnocavallo, author of the original Python Redis client,
    from which some of the socket code is still used.
-   Alexander Solovyov for ideas on the generic response callback
    system.
-   Paul Hubbard for initial packaging support.

[![Redis](./docs/logo-redis.png)](https://www.redis.com)
