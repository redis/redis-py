Lua Scripting
===

`Lua Scripting <#lua-scripting-in-default-connections>`__ \|
`Pipelines <#pipelines>`__ \| `Cluster mode <#cluster-mode>`__

--------------

Lua Scripting in default connections
------------------------------------

redis-py supports the EVAL, EVALSHA, and SCRIPT commands. However, there
are a number of edge cases that make these commands tedious to use in
real world scenarios. Therefore, redis-py exposes a Script object that
makes scripting much easier to use. (RedisClusters have limited support
for scripting.)

To create a Script instance, use the register_script function on a
client instance passing the Lua code as the first argument.
register_script returns a Script instance that you can use throughout
your code.

The following trivial Lua script accepts two parameters: the name of a
key and a multiplier value. The script fetches the value stored in the
key, multiplies it with the multiplier value and returns the result.

.. code:: pycon

   >>> r = redis.Redis()
   >>> lua = """
   ... local value = redis.call('GET', KEYS[1])
   ... value = tonumber(value)
   ... return value * ARGV[1]"""
   >>> multiply = r.register_script(lua)

multiply is now a Script instance that is invoked by calling it like a
function. Script instances accept the following optional arguments:

-  **keys**: A list of key names that the script will access. This
   becomes the KEYS list in Lua.
-  **args**: A list of argument values. This becomes the ARGV list in
   Lua.
-  **client**: A redis-py Client or Pipeline instance that will invoke
   the script. If client isn't specified, the client that initially
   created the Script instance (the one that register_script was invoked
   from) will be used.

Continuing the example from above:

.. code:: pycon

   >>> r.set('foo', 2)
   >>> multiply(keys=['foo'], args=[5])
   10

The value of key 'foo' is set to 2. When multiply is invoked, the 'foo'
key is passed to the script along with the multiplier value of 5. Lua
executes the script and returns the result, 10.

Script instances can be executed using a different client instance, even
one that points to a completely different Redis server.

.. code:: pycon

   >>> r2 = redis.Redis('redis2.example.com')
   >>> r2.set('foo', 3)
   >>> multiply(keys=['foo'], args=[5], client=r2)
   15

The Script object ensures that the Lua script is loaded into Redis's
script cache. In the event of a NOSCRIPT error, it will load the script
and retry executing it.

Pipelines
---------

Script objects can also be used in pipelines. The pipeline instance
should be passed as the client argument when calling the script. Care is
taken to ensure that the script is registered in Redis's script cache
just prior to pipeline execution.

.. code:: pycon

   >>> pipe = r.pipeline()
   >>> pipe.set('foo', 5)
   >>> multiply(keys=['foo'], args=[5], client=pipe)
   >>> pipe.execute()
   [True, 25]

Cluster Mode
------------

Cluster mode has limited support for lua scripting.

The following commands are supported, with caveats: - ``EVAL`` and
``EVALSHA``: The command is sent to the relevant node, depending on the
keys (i.e., in ``EVAL "<script>" num_keys key_1 ... key_n ...``). The
keys *must* all be on the same node. If the script requires 0 keys, *the
command is sent to a random (primary) node*. - ``SCRIPT EXISTS``: The
command is sent to all primaries. The result is a list of booleans
corresponding to the input SHA hashes. Each boolean is an AND of “does
the script exist on each node?”. In other words, each boolean is True
iff the script exists on all nodes. - ``SCRIPT FLUSH``: The command is
sent to all primaries. The result is a bool AND over all nodes’
responses. - ``SCRIPT LOAD``: The command is sent to all primaries. The
result is the SHA1 digest.

The following commands are not supported: - ``EVAL_RO`` - ``EVALSHA_RO``

Using scripting within pipelines in cluster mode is **not supported**.
