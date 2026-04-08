RESP2/RESP3 Response Unification
==================================

.. note::

   **Migration Guide** — This document covers breaking changes introduced in
   redis-py 8.0+ as part of the RESP2/RESP3 response unification effort.

Starting with version 8.0, redis-py **unifies command return types across
RESP2 and RESP3 protocols**.  The same command now returns the same Python type
and structure regardless of which protocol version is in use.  This eliminates
the need for protocol-specific branching in application code and makes the
protocol version a pure transport detail.

Approximately **84 commands** are affected across core Redis, Search, JSON,
TimeSeries, and Probabilistic modules.

.. warning::

   All changes listed below are **breaking changes** relative to redis-py 7.x
   and earlier.

.. contents::
   :local:
   :depth: 3


Changes for RESP2 Users (protocol=2, the default for versions before 8.0)
---------------------------------------------------------------------------

If you are using the default RESP2 protocol, the following categories of
changes apply to your code.

Sorted Sets — Tuples → Lists, Scores → Float
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All sorted set commands that return ``(member, score)`` pairs now return
**lists** instead of **tuples**, and scores are always **float** regardless of
protocol.

.. code:: python

   # Before (redis-py 7.x, RESP2)
   r.zrange("myset", 0, -1, withscores=True)
   # [(b"a", 1), (b"b", 2)]        — list of tuples, int scores

   # After (redis-py 8.0+)
   r.zrange("myset", 0, -1, withscores=True)
   # [[b"a", 1.0], [b"b", 2.0]]    — list of lists, float scores

Affected commands: ``ZDIFF``, ``ZINTER``, ``ZPOPMAX``, ``ZPOPMIN``,
``ZRANGE``, ``ZRANGEBYSCORE``, ``ZREVRANGE``, ``ZREVRANGEBYSCORE``,
``ZUNION``, ``ZRANDMEMBER`` (withscores), ``ZSCAN``, ``ZMPOP``, ``BZMPOP``.

``ZRANK`` / ``ZREVRANK`` with ``withscore=True`` now return the score as
``float`` (e.g. ``[2, 3.0]`` instead of ``[2, 3]``).

``score_cast_func`` Change
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Custom ``score_cast_func`` callables now receive a **float** instead of raw
bytes.  For example, if you pass ``score_cast_func=str``, the input is now
``str(1.0)`` → ``"1.0"`` rather than ``str(b"1")`` → ``"b'1'"``.  Scientific
notation in large scores (e.g. ``1.77e+18``) is handled automatically via a
``float()`` intermediate conversion.

Blocking Pops — Tuples → Lists
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``BLPOP``, ``BRPOP``, ``BZPOPMAX``, and ``BZPOPMIN`` now return **lists**
instead of tuples:

.. code:: python

   # Before
   r.blpop("mylist")          # (b"mylist", b"value")

   # After
   r.blpop("mylist")          # [b"mylist", b"value"]

Flat Lists → Nested Structures
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``ZRANDMEMBER`` (withscores) and ``HRANDFIELD`` (withvalues) previously
returned flat interleaved lists in RESP2.  They now return nested
``[[key, value], ...]`` pairs, matching the RESP3 native format.

``XREAD`` / ``XREADGROUP`` now return a **dict** (``{stream: entries}``)
instead of a list of lists.  An empty result is ``{}`` instead of ``[]``.

Other Core Changes
^^^^^^^^^^^^^^^^^^^^

- ``LCS`` with IDX: flat list → ``dict``
- ``CLIENT TRACKINGINFO``: flat list → ``dict``
- ``GEOPOS``: coordinate tuples → lists
- ``GEOSEARCH`` / ``GEORADIUS`` / ``GEORADIUSBYMEMBER`` (withcoord): coordinate tuples → lists
- ``SENTINEL`` commands: ``flags`` comma-string → ``set``
- ``COMMAND``: ``flags`` list → ``set``; new ``acl_categories`` field
- ``ACL GETUSER``: selectors flat lists → dicts
- ``CLUSTER LINKS`` / ``CLUSTER SHARDS``: bytes keys → string keys
- ``FUNCTION LIST``: flat sublists → nested dicts
- ``MEMORY STATS``: string values → native int types
- ``XINFO CONSUMERS`` / ``XINFO GROUPS``: bytes values → native types

Detailed RESP2 Core Command Reference
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 18 35 12 35

   * - Command
     - Change
     - Before (7.x)
     - After (8.0+)
   * - ``ZDIFF`` (withscores)
     - Tuples → lists; scores bytes → float
     - ``[(b"a", b"1")]``
     - ``[[b"a", 1.0]]``
   * - ``ZINTER`` (withscores)
     - Tuples → lists; scores bytes → float
     - ``[(b"a", b"1")]``
     - ``[[b"a", 1.0]]``
   * - ``ZPOPMAX``
     - Tuples → lists; scores → float
     - ``[(b"a", 3)]``
     - ``[[b"a", 3.0]]``
   * - ``ZPOPMIN``
     - Tuples → lists; scores → float
     - ``[(b"a", 1)]``
     - ``[[b"a", 1.0]]``
   * - ``ZRANGE`` (withscores)
     - Tuples → lists; scores bytes → float
     - ``[(b"a", b"1"), (b"b", b"2")]``
     - ``[[b"a", 1.0], [b"b", 2.0]]``
   * - ``ZRANGEBYSCORE`` (withscores)
     - Tuples → lists; scores bytes → float
     - ``[(b"a", b"1")]``
     - ``[[b"a", 1.0]]``
   * - ``ZREVRANGE`` (withscores)
     - Tuples → lists; scores bytes → float
     - ``[(b"b", b"2")]``
     - ``[[b"b", 2.0]]``
   * - ``ZREVRANGEBYSCORE`` (withscores)
     - Tuples → lists; scores bytes → float
     - ``[(b"b", b"2")]``
     - ``[[b"b", 2.0]]``
   * - ``ZUNION`` (withscores)
     - Tuples → lists; scores bytes → float
     - ``[(b"a", b"1")]``
     - ``[[b"a", 1.0]]``
   * - ``ZRANDMEMBER`` (withscores)
     - Flat interleaved → nested pairs; scores → float
     - ``[b"a", b"1", b"b", b"2"]``
     - ``[[b"a", 1.0], [b"b", 2.0]]``
   * - ``HRANDFIELD`` (withvalues)
     - Flat interleaved → nested pairs
     - ``[b"f1", b"v1", b"f2", b"v2"]``
     - ``[[b"f1", b"v1"], [b"f2", b"v2"]]``
   * - ``BLPOP``
     - Tuple → list
     - ``(b"key", b"val")``
     - ``[b"key", b"val"]``
   * - ``BRPOP``
     - Tuple → list
     - ``(b"key", b"val")``
     - ``[b"key", b"val"]``
   * - ``BZPOPMAX``
     - Tuple → list; score as float
     - ``(b"key", b"member", 3)``
     - ``[b"key", b"member", 3.0]``
   * - ``BZPOPMIN``
     - Tuple → list; score as float
     - ``(b"key", b"member", 1)``
     - ``[b"key", b"member", 1.0]``
   * - ``ZMPOP``
     - Scores bytes → float; structure normalized
     - ``[b"key", [(b"a", b"1")]]``
     - ``[b"key", [[b"a", 1.0]]]``
   * - ``BZMPOP``
     - Same as ZMPOP
     - Same as ZMPOP
     - Same as ZMPOP
   * - ``ZRANK`` / ``ZREVRANK`` (withscore)
     - Score int → float
     - ``[2, 3]``
     - ``[2, 3.0]``
   * - ``ZSCAN``
     - score_cast_func receives float
     - ``score_cast_func(b"1")``
     - ``score_cast_func(1.0)``
   * - ``LCS`` (with IDX)
     - Flat list → dict
     - ``[b"matches", [...], b"len", 6]``
     - ``{"matches": [...], "len": 6}``
   * - ``XREAD`` / ``XREADGROUP``
     - List of lists → dict
     - ``[[b"stream", [(id, fields)]]]``
     - ``{b"stream": [(id, fields)]}``
   * - ``CLIENT TRACKINGINFO``
     - Flat list → dict
     - ``["flags", [...], "redirect", -1]``
     - ``{"flags": [...], "redirect": -1}``
   * - ``GEOPOS``
     - Tuples → lists for coordinates
     - ``[(1.0, 2.0), None]``
     - ``[[1.0, 2.0], None]``
   * - ``GEOSEARCH`` / ``GEORADIUS`` / ``GEORADIUSBYMEMBER`` (withcoord)
     - Tuples → lists for coordinates
     - ``[b"place", (1.0, 2.0)]``
     - ``[b"place", [1.0, 2.0]]``
   * - ``SENTINEL`` commands
     - Flags comma-string → set
     - ``{"flags": "master,odown"}``
     - ``{"flags": {"master", "odown"}}``
   * - ``COMMAND``
     - Flags list → set; new ``acl_categories``
     - ``{"flags": ["readonly"]}``
     - ``{"flags": {"readonly"}, "acl_categories": {...}}``
   * - ``ACL GETUSER``
     - Selectors flat lists → dicts
     - ``{"selectors": [["~key", "+get"]]}``
     - ``{"selectors": [{"keys": "~key", "commands": "+get"}]}``
   * - ``CLUSTER LINKS`` / ``SHARDS``
     - Bytes keys → string keys
     - ``[{b"direction": b"to"}]``
     - ``[{"direction": b"to"}]``
   * - ``FUNCTION LIST``
     - Flat sublists → nested dicts
     - ``[[b"library_name", b"mylib"]]``
     - ``[{"library_name": "mylib", "functions": [...]}]``
   * - ``MEMORY STATS``
     - String values → native int types
     - ``{"peak.allocated": "1024"}``
     - ``{"peak.allocated": 1024}``
   * - ``XINFO CONSUMERS`` / ``GROUPS``
     - Bytes values → native types
     - ``{b"idle": b"1234"}``
     - ``{"idle": 1234}``


Changes for RESP3 Users (protocol=3)
--------------------------------------

If you are already using RESP3, fewer changes affect your code, but several
commands now gain proper parsing that was previously missing.

Search Module — Raw Dictionaries → Rich Objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is the most significant change for RESP3 users.  Search commands now
return the same rich objects as RESP2:

- ``FT.SEARCH`` → ``Result`` object (with ``.total``, ``.docs``, ``.warnings``)
- ``FT.AGGREGATE`` → ``AggregateResult`` object (with ``.total``, ``.rows``, ``.warnings``)
- ``FT.PROFILE`` → ``(Result|AggregateResult, ProfileInformation)`` tuple

Probabilistic Module — Raw Dictionaries → Info Objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``BF.INFO``, ``CF.INFO``, ``CMS.INFO``, ``TOPK.INFO``, and ``TDIGEST.INFO``
now return their respective ``*Info`` objects instead of raw dicts with bytes
keys.

Other RESP3 Changes
^^^^^^^^^^^^^^^^^^^^^

- ``ACL CAT``, ``ACL LIST``, ``ACL USERS``, ``ACL WHOAMI``, ``ACL GENPASS``,
  ``ACL HELP``: bytes → str
- ``ACL LOG``: ``age-seconds`` string → float; ``client-info`` string → dict
- ``CLIENT GETNAME``, ``GEOHASH``, ``RESET``: bytes → str
- ``BGREWRITEAOF``, ``BGSAVE``: status string → bool
- ``SENTINEL MASTERS``: list of dicts → dict keyed by name
- ``DEBUG OBJECT``: raw string → parsed dict

Detailed RESP3 Core Command Reference
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 22 30 22 26

   * - Command
     - Change
     - Before (7.x)
     - After (8.0+)
   * - ``ZINTER`` (withscores)
     - Scores now cast via ``score_cast_func``
     - ``[[b"a", 1.0]]`` (no cast)
     - ``[[b"a", 1.0]]`` (cast applied)
   * - ``ZPOPMAX`` / ``ZPOPMIN``
     - Scores normalized via callback
     - ``[[b"a", 3.0]]`` (no cast)
     - ``[[b"a", 3.0]]`` (cast applied)
   * - ``ZRANDMEMBER`` (withscores)
     - Scores explicitly cast to float
     - ``[[b"a", 1.0]]``
     - ``[[b"a", 1.0]]``
   * - ``BZPOPMAX`` / ``BZPOPMIN``
     - Score explicitly cast to float
     - ``[b"key", b"member", 3.0]``
     - ``[b"key", b"member", 3.0]``
   * - ``ACL CAT/LIST/USERS/WHOAMI``
     - bytes → str
     - ``[b"read", b"write"]``
     - ``["read", "write"]``
   * - ``ACL GENPASS`` / ``ACL HELP``
     - bytes → str
     - ``b"abc123..."``
     - ``"abc123..."``
   * - ``ACL LOG``
     - ``age-seconds`` str → float; ``client-info`` → dict
     - ``{"age-seconds": "0.5"}``
     - ``{"age-seconds": 0.5, "client-info": {...}}``
   * - ``CLIENT GETNAME``
     - bytes → str
     - ``b"myconn"``
     - ``"myconn"``
   * - ``GEOHASH``
     - bytes → str
     - ``[b"sqc8b49rny0"]``
     - ``["sqc8b49rny0"]``
   * - ``BGREWRITEAOF`` / ``BGSAVE``
     - Status string → bool
     - ``"Background saving started"``
     - ``True``
   * - ``SENTINEL MASTERS``
     - list[dict] → dict keyed by name
     - ``[{"name": "mymaster", ...}]``
     - ``{"mymaster": {"name": "mymaster", ...}}``
   * - ``DEBUG OBJECT``
     - Raw string → parsed dict
     - ``b"Value at:0x... refcount:1 ..."``
     - ``{"Value": "0x...", "refcount": 1}``
   * - ``CLUSTER LINKS`` / ``SHARDS``
     - Bytes keys → string keys
     - ``[{b"direction": b"to"}]``
     - ``[{"direction": b"to"}]``

Detailed RESP3 Search Reference
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 20 30 22 28

   * - Command
     - Change
     - Before (7.x)
     - After (8.0+)
   * - ``FT.SEARCH``
     - Raw dict → ``Result`` object
     - ``{"total_results": 2, "results": [...]}``
     - ``result.total == 2``, ``result.docs[0].id``
   * - ``FT.AGGREGATE``
     - Raw dict → ``AggregateResult`` object
     - ``{"total_results": 5, "results": [...]}``
     - ``result.total == 5``, ``result.rows``
   * - ``FT.PROFILE``
     - Single wrapper → structured tuple
     - ``ProfileInformation(raw_dict)``
     - ``(Result, ProfileInformation)``
   * - ``FT.SPELLCHECK``
     - Raw nested → normalized dict
     - Raw nested dict/arrays
     - ``{"importnt": [{"score": "0", "suggestion": "important"}]}``
   * - ``FT.INFO``
     - Bytes keys → string keys
     - ``{b"index_name": b"idx"}``
     - ``{"index_name": "idx"}``
   * - ``FT.CONFIG GET``
     - Bytes keys → string keys
     - ``{b"TIMEOUT": b"500"}``
     - ``{"TIMEOUT": "500"}``
   * - ``FT.SYNDUMP``
     - Bytes keys → string keys
     - ``{b"term": [b"syn1"]}``
     - ``{"term": ["syn1"]}``

Detailed RESP3 Probabilistic Reference
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 22 30 22 26

   * - Command
     - Change
     - Before (7.x)
     - After (8.0+)
   * - ``BF.INFO``
     - Raw dict → ``BFInfo`` object
     - ``{b"Capacity": 100}``
     - ``bf_info.capacity == 100``
   * - ``CF.INFO``
     - Raw dict → ``CFInfo`` object
     - ``{b"Size": 512}``
     - ``cf_info.size == 512``
   * - ``CMS.INFO``
     - Raw dict → ``CMSInfo`` object
     - ``{b"width": 2000}``
     - ``cms_info.width == 2000``
   * - ``TOPK.INFO``
     - Raw dict → ``TopKInfo`` object
     - ``{b"k": 5}``
     - ``topk_info.k == 5``
   * - ``TDIGEST.INFO``
     - Raw dict → ``TDigestInfo`` object
     - ``{b"Compression": 100}``
     - ``tdigest_info.compression == 100``
   * - ``TDIGEST.BYRANK`` / ``BYREVRANK``
     - Raw list → parsed list
     - ``[0.5, b"inf"]``
     - ``[0.5, "inf"]``
   * - ``TDIGEST.CDF`` / ``QUANTILE``
     - Raw list → parsed list
     - ``[0.5]``
     - ``[0.5]``


Module-Specific Changes (Both Protocols)
------------------------------------------

TimeSeries
^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 22 30 22 26

   * - Command
     - Change
     - Before (7.x)
     - After (8.0+)
   * - ``TS.GET``
     - Tuple → list
     - ``(1234567890, 1.5)``
     - ``[1234567890, 1.5]``
   * - ``TS.RANGE`` / ``TS.REVRANGE``
     - List of tuples → list of lists
     - ``[(ts, val), ...]``
     - ``[[ts, val], ...]``
   * - ``TS.QUERYINDEX``
     - Remove incorrect int coercion
     - ``"2"`` → ``2``
     - ``"2"`` stays ``"2"``
   * - ``TS.MGET``
     - Sorted list of dicts → dict
     - ``[{"1": [{}, None, None]}]``
     - ``{"1": [{}, [ts, val]]}``
   * - ``TS.MRANGE`` / ``TS.MREVRANGE``
     - Sorted list of dicts → dict; adds metadata slot
     - ``[{"1": [labels, samples]}]``
     - ``{"1": [labels, metadata, samples]}``

JSON
^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 25 30 20 25

   * - Command
     - Change
     - Before (7.x)
     - After (8.0+)
   * - ``JSON.NUMINCRBY`` / ``NUMMULTBY``
     - Scalar → array for legacy paths
     - ``5``
     - ``[5]``
   * - ``JSON.RESP``
     - String-encoded floats → native floats
     - ``[b"{", b"price", b"-19.5"]``
     - ``[b"{", b"price", -19.5]``
   * - ``JSON.OBJKEYS``
     - Respects ``decode_responses`` setting
     - ``["key1", "key2"]`` (forced str)
     - ``[b"key1", b"key2"]`` (raw bytes)

Probabilistic
^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 25 30 20 25

   * - Command
     - Change
     - Before (7.x)
     - After (8.0+)
   * - ``TOPK.ADD``
     - Remove incorrect int/float coercion
     - ``"42"`` → ``42``
     - ``"42"`` stays ``"42"``
   * - ``TOPK.INCRBY``
     - Remove incorrect int/float coercion
     - ``"42"`` → ``42``
     - ``"42"`` stays ``"42"``
   * - ``TOPK.LIST``
     - Remove incorrect int/float coercion
     - ``"42"`` → ``42``
     - ``"42"`` stays ``"42"``

Search
^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 22 30 22 26

   * - Command
     - Change
     - Before (7.x)
     - After (8.0+)
   * - ``FT.INFO``
     - Flat attribute sublists → structured dicts
     - ``[["identifier", "title", "type", "TEXT", "SORTABLE"]]``
     - ``[{"identifier": "title", "type": "TEXT", "flags": ["SORTABLE"]}]``
   * - ``FT.CONFIG GET``
     - Bytes keys/values → string keys/values
     - ``{b"TIMEOUT": b"500"}``
     - ``{"TIMEOUT": "500"}``
   * - ``FT.SEARCH`` / ``FT.AGGREGATE``
     - New ``warnings`` attribute
     - No ``warnings`` field
     - ``result.warnings == []``
   * - ``FT.SPELLCHECK``
     - Raw nested → normalized dict
     - Raw nested dict/arrays
     - ``{"importnt": [{"score": "0", "suggestion": "important"}]}``


Pipelines
-----------

Module pipelines (JSON, TimeSeries, Search) now include the full set of core
+ module response callbacks.  Previously, pipeline results for module commands
could return raw protocol data; they now match the non-pipeline behavior
exactly.  Search pipeline callbacks use ``functools.partial`` with
``_parse_results``, and each pipeline instance copies its ``response_callbacks``
dict to avoid polluting the shared client registry.

