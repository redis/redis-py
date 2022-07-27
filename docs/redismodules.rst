Redis Modules Commands
######################

Accessing redis module commands requires the installation of the supported `Redis module <https://docs.redis.com/latest/modules/>`_. For a quick start with redis modules, try the `Redismod docker <https://hub.docker.com/r/redislabs/redismod>`_.


RedisBloom Commands
*******************

These are the commands for interacting with the `RedisBloom module <https://redisbloom.io>`_. Below is a brief example, as well as documentation on the commands themselves.

**Create and add to a bloom filter**

.. code-block:: python

    import redis
    filter = redis.bf().create("bloom", 0.01, 1000)
    filter.add("bloom", "foo")

**Create and add to a cuckoo filter**

.. code-block:: python

    import redis
    filter = redis.cf().create("cuckoo", 1000)
    filter.add("cuckoo", "filter")

**Create Count-Min Sketch and get information**

.. code-block:: python

    import redis
    r = redis.cms().initbydim("dim", 1000, 5)
    r.cms().incrby("dim", ["foo"], [5])
    r.cms().info("dim")

**Create a topk list, and access the results**

.. code-block:: python

    import redis
    r = redis.topk().reserve("mytopk", 3, 50, 4, 0.9)
    info = r.topk().info("mytopk)

.. automodule:: redis.commands.bf.commands
    :members: BFCommands, CFCommands, CMSCommands, TOPKCommands

------

RedisGraph Commands
*******************

These are the commands for interacting with the `RedisGraph module <https://redisgraph.io>`_. Below is a brief example, as well as documentation on the commands themselves.

**Create a graph, adding two nodes**

.. code-block:: python

    import redis
    from redis.graph.node import Node

    john = Node(label="person", properties={"name": "John Doe", "age": 33}
    jane = Node(label="person", properties={"name": "Jane Doe", "age": 34}

    r = redis.Redis()
    graph = r.graph()
    graph.add_node(john)
    graph.add_node(jane)
    graph.add_node(pat)
    graph.commit()

.. automodule:: redis.commands.graph.node
    :members: Node

.. automodule:: redis.commands.graph.edge
    :members: Edge

.. automodule:: redis.commands.graph.commands
    :members: GraphCommands

------

RedisJSON Commands
******************

These are the commands for interacting with the `RedisJSON module <https://redisjson.io>`_. Below is a brief example, as well as documentation on the commands themselves.

**Create a json object**

.. code-block:: python

    import redis
    r = redis.Redis()
    r.json().set("mykey", ".", {"hello": "world", "i am": ["a", "json", "object!"]}

Examples of how to combine search and json can be found `here <examples/search_json_examples.html>`_.

.. automodule:: redis.commands.json.commands
    :members: JSONCommands

-----

RediSearch Commands
*******************

These are the commands for interacting with the `RediSearch module <https://redisearch.io>`_. Below is a brief example, as well as documentation on the commands themselves.

**Create a search index, and display its information**

.. code-block:: python

    import redis
    from redis.commands.search.field import TextField

    r = redis.Redis()
    r.ft().create_index(TextField("play", weight=5.0), TextField("ball"))
    print(r.ft().info())


.. automodule:: redis.commands.search.commands
    :members: SearchCommands

-----

RedisTimeSeries Commands
************************

These are the commands for interacting with the `RedisTimeSeries module <https://redistimeseries.io>`_. Below is a brief example, as well as documentation on the commands themselves.


**Create a timeseries object with 5 second retention**

.. code-block:: python

    import redis
    r = redis.Redis()
    r.ts().create(2, retension_msecs=5000)

.. automodule:: redis.commands.timeseries.commands
    :members: TimeSeriesCommands


