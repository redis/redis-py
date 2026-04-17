# Cluster PubSub: Connection Extraction Analysis & Refactoring Plan

Scope: `redis.cluster.ClusterPubSub` (sync) and `redis.asyncio.cluster.ClusterPubSub`
(async). This document analyses how each implementation currently obtains the
connection(s) used to send `SUBSCRIBE` / `PSUBSCRIBE` / `SSUBSCRIBE` commands and
proposes a plan to align both with the resources already exposed by
`NodesManager` / `ClusterNode`, following the pattern used in
`redis.keyspace_notifications` / `redis.asyncio.keyspace_notifications`.

---

## 1. Current state — Sync (`redis/cluster.py`)

### 1.1 Underlying cluster resources available to `ClusterPubSub`

| Resource | Type | Provided by |
| --- | --- | --- |
| `ClusterNode.redis_connection` | `redis.Redis` | `NodesManager.create_redis_connections()` (lazy: via `RedisCluster.get_redis_connection(node)`) |
| `ClusterNode.redis_connection.connection_pool` | `ConnectionPool` | Created in `NodesManager.create_redis_node()` |
| `RedisCluster.get_redis_connection(node)` | `redis.Redis` | Ensures node has a `redis_connection`, returns it |
| `RedisCluster.get_primaries()` / `get_random_node()` | `list[ClusterNode]` / `ClusterNode` | Topology accessors on `NodesManager` |

Each sync `ClusterNode` owns a full `Redis` instance whose `ConnectionPool`
is the authoritative pool for that node. All normal commands go through it.

### 1.2 Primary (non-sharded) pubsub path

File: `redis/cluster.py`, class `ClusterPubSub`.

- `__init__` (L2598–L2641): if a node was supplied, eagerly pulls the node's
  pool:
  ````python
  connection_pool = (
      None if self.node is None
      else redis_cluster.get_redis_connection(self.node).connection_pool
  )
  ````
  That pool is forwarded to `PubSub.__init__`.
- `execute_command` (L2692–L2732): if `self.connection is None`:
  - Picks a node via `cluster.nodes_manager.get_node_from_slot(...)` (keyslot
    of the first channel) or `cluster.get_random_node()`.
  - Sets `self.connection_pool = cluster.get_redis_connection(node).connection_pool`.
  - Calls `self.connection = self.connection_pool.get_connection()` — the
    connection is then held for the entire subscribed lifetime (standard
    `PubSub` contract).

The sync implementation therefore **already reuses the node's backing
`ConnectionPool`** and its connection lifecycle (creation, retry config,
events, maintenance notifications) is controlled by `NodesManager`.

### 1.3 Sharded pubsub path (`ssubscribe`/`sunsubscribe`)

`_get_node_pubsub(node)` (L2734–L2742):
````python
pubsub = node.redis_connection.pubsub(push_handler_func=...)
````
It delegates to the node's `Redis` client `.pubsub()`, which internally
constructs a `PubSub` bound to that same `node.redis_connection.connection_pool`.
So sharded pubsubs also transitively reuse the node-level pool.

### 1.4 Disconnect

`disconnect()` (L2834–L2841) closes `self.connection` and every shard
pubsub's `connection`. No pool-level release call — the connections are
effectively owned by the pubsub for its lifetime (consistent with `PubSub`).

---

## 2. Current state — Async (`redis/asyncio/cluster.py`)

### 2.1 Underlying cluster resources available to `ClusterPubSub`

The async `ClusterNode` (L1398–L1615) has a **different model** than sync:

| Attribute / method | Role |
| --- | --- |
| `connection_class`, `connection_kwargs`, `max_connections` | Config for connections |
| `_connections: list[Connection]`, `_free: deque[Connection]` | Node-owned connection pool |
| `acquire_connection()` | Pop from `_free` or create a new `Connection` up to `max_connections` |
| `release(connection)` | Return connection to `_free` |
| `get_encoder()` | Build an `Encoder` from `connection_kwargs` |
| `disconnect_if_needed(conn)` | Lazy reconnect support for maintenance |

There is **no** `redis_connection` attribute, and **no** `ConnectionPool`
object — the node itself *is* the pool. All regular commands
(`execute_command`, `execute_pipeline`) go through
`acquire_connection()` / `release()`.

### 2.2 Primary (non-sharded) pubsub path

File: `redis/asyncio/cluster.py`, class `ClusterPubSub`.

- `__init__` (L3064–L3115): if a node was supplied, builds a **brand new**
  `ConnectionPool` from the node's kwargs:
  ````python
  connection_pool = ConnectionPool(
      connection_class=self.node.connection_class,
      **self.node.connection_kwargs,
  )
  ````
  This pool is passed to `PubSub.__init__`. It is completely disjoint from
  the node's own `_connections` / `_free` pool.
- `execute_command` (L3333–L3376): if `self.connection_pool is None`, picks a
  node (keyslot / random) and again constructs a fresh `ConnectionPool`
  with `node.connection_kwargs`, then delegates to
  `super().execute_command(...)` which lazily acquires a connection from
  that newly created pool.

### 2.3 Sharded pubsub path

`_get_node_pubsub(node)` (L3159–L3176) repeats the pattern:
````python
connection_pool = ConnectionPool(
    connection_class=node.connection_class, **node.connection_kwargs
)
pubsub = PubSub(connection_pool=connection_pool, ...)
````
Every sharded node gets yet another detached `ConnectionPool`.

### 2.4 Consequences

1. **Resource duplication**: each `ClusterPubSub` (and each shard) creates a
   parallel `ConnectionPool` that bypasses the node's `_free` queue and
   `max_connections` budget.
2. **Event / maintenance-notification divergence**: connections opened via
   the detached pool do not participate in the node's
   `update_active_connections_for_reconnect`, `disconnect_if_needed`, or
   event dispatcher wiring that `ClusterNode` owns.
3. **Credentials / retry / on_connect drift risk**: because the detached
   pool is reconstructed from `connection_kwargs` only, any adjustments
   made on the node after construction (e.g. retry rebinding) are not
   reflected.
4. **Inconsistency with sync**: the sync path centralises pubsub through
   the node's authoritative pool; async does not.

---

## 3. Reference implementation — keyspace notifications

`ClusterKeyspaceNotifications` / `AsyncClusterKeyspaceNotifications` already
solve the exact same problem for keyspace events. They serve as the
template for the refactor.

### 3.1 Sync (`redis/keyspace_notifications.py`)

`ClusterKeyspaceNotifications._ensure_node_pubsub(node)` (L1342–L1351):
````python
redis_conn = self.cluster.get_redis_connection(node)
pubsub = redis_conn.pubsub(ignore_subscribe_messages=False)
self._node_pubsubs[node.name] = pubsub
````
It asks `RedisCluster.get_redis_connection(node)` (which lazily materialises
`node.redis_connection` through `NodesManager.create_redis_connections`) and
calls `.pubsub()` on it, inheriting the node's `ConnectionPool`. Nodes are
enumerated via `self.cluster.get_primaries()`.

### 3.2 Async (`redis/asyncio/keyspace_notifications.py`)

`_ClusterNodePoolAdapter` (L68–L102) is a minimal object that implements the
tiny `ConnectionPool` surface `PubSub` needs, backed by the node itself:

````python
class _ClusterNodePoolAdapter:
    def __init__(self, node: ClusterNode) -> None:
        self._node = node
        self.connection_kwargs = node.connection_kwargs
    def get_encoder(self) -> Encoder:
        return self._node.get_encoder()
    async def get_connection(self, ...):
        connection = self._node.acquire_connection()
        await connection.connect()
        return connection
    async def release(self, connection) -> None:
        self._node.release(connection)
````

`AsyncClusterKeyspaceNotifications._ensure_node_pubsub(node)` (L634–L649)
wraps the node with this adapter and feeds it as
`connection_pool=` to `PubSub(...)`. No duplicate `ConnectionPool` is
created; `PubSub.aclose()` already disconnects the connection before
calling `release`, so the socket never re-enters the node's free queue in a
subscribed state.

This pattern:
- reuses the node's `_free` / `max_connections` budget,
- respects the node's event / maintenance machinery,
- keeps `ClusterPubSub` free of connection-construction logic.

---

## 4. Plan — migrate `ClusterPubSub` to `NodesManager`-owned resources

Goal: make both `ClusterPubSub` implementations obtain their connections
*exclusively* through the `NodesManager` / `ClusterNode` surface used by
normal cluster commands and by keyspace notifications. No new
public APIs, no behaviour changes visible to callers.

### 4.1 Async `ClusterPubSub` — primary work

This is the implementation that actually needs to change.

1. **Reuse the existing adapter.** Import
   `redis.asyncio.keyspace_notifications._ClusterNodePoolAdapter` (or, if
   cross-module import is undesirable, relocate it to a neutral module —
   e.g. `redis/asyncio/_cluster_pool_adapter.py` — and import it from both
   `keyspace_notifications` and `cluster`). Preferred option: move it to a
   neutral module to avoid a circular dependency between `cluster` and
   `keyspace_notifications`.

2. **Replace `ConnectionPool(...)` creation in `__init__`** (L3093–L3100):
   ````python
   if self.node is not None:
       connection_pool = _ClusterNodePoolAdapter(self.node)
   else:
       connection_pool = None
   ````
   The rest of `super().__init__(connection_pool=..., ...)` is unchanged;
   `PubSub` only calls `get_connection` / `release` / `get_encoder` on the
   pool, all of which the adapter satisfies.

3. **Replace `ConnectionPool(...)` creation in `execute_command`**
   (L3354–L3373):
   ````python
   if self.connection is None:
       if self.connection_pool is None:
           # ...node selection unchanged...
           self.node = node
           self.connection_pool = _ClusterNodePoolAdapter(node)
   return await super().execute_command(*args, **kwargs)
   ````

4. **Replace `ConnectionPool(...)` creation in `_get_node_pubsub`**
   (L3159–L3176):
   ````python
   def _get_node_pubsub(self, node: "ClusterNode") -> PubSub:
       try:
           return self.node_pubsub_mapping[node.name]
       except KeyError:
           pubsub = PubSub(
               connection_pool=_ClusterNodePoolAdapter(node),
               encoder=self.cluster.encoder,
               push_handler_func=self.push_handler_func,
               event_dispatcher=self._event_dispatcher,
           )
           self.node_pubsub_mapping[node.name] = pubsub
           return pubsub
   ````

5. **`aclose` / `get_redis_connection`**: unchanged. `PubSub.aclose()`
   disconnects `self.connection` before calling
   `connection_pool.release(connection)`, which the adapter forwards to
   `ClusterNode.release(...)`; the socket re-enters the node's free queue
   in a disconnected state, matching the guarantee already documented on
   `_ClusterNodePoolAdapter`.

6. **Encoder parity**: today the async `ClusterPubSub` passes
   `encoder=redis_cluster.encoder`; after the change the adapter would
   expose `node.get_encoder()` instead for any `PubSub` code path that
   uses `connection_pool.get_encoder()`. The explicit `encoder=` argument
   should be kept to preserve current behaviour (cluster-wide encoder).

### 4.2 Sync `ClusterPubSub` — smaller adjustments

The sync implementation already uses the node's own `ConnectionPool` via
`redis_cluster.get_redis_connection(node).connection_pool`, so it is
essentially compliant. The remaining improvements are about *symmetry*
and *delegation*, not correctness:

1. **Centralise node-pubsub creation through `get_redis_connection`.**
   In `_get_node_pubsub` (L2734–L2742) the current code reaches into
   `node.redis_connection` directly; if a node has not yet been
   materialised (e.g. just discovered on topology refresh), `redis_connection`
   can be `None`. Replace with:
   ````python
   redis_conn = self.cluster.get_redis_connection(node)
   pubsub = redis_conn.pubsub(push_handler_func=self.push_handler_func)
   ````
   matching `ClusterKeyspaceNotifications._ensure_node_pubsub` and
   guaranteeing the lazy `NodesManager.create_redis_connections([node])`
   path is taken.

2. **No change to the primary path** (`__init__` / `execute_command`):
   both already use `cluster.get_redis_connection(node).connection_pool`.

3. **`disconnect()` review**: iterating
   `self.node_pubsub_mapping.values()` and calling
   `pubsub.connection.disconnect()` is fine, but should tolerate
   `pubsub.connection is None` (a shard pubsub that has not yet sent a
   command). This is a pre-existing latent bug, not introduced by the
   refactor; fix it as part of this change for parity with async
   `aclose()` which already tolerates it.

### 4.3 Cross-cutting tasks

1. **Relocate `_ClusterNodePoolAdapter`** to a shared neutral module
   (e.g. `redis/asyncio/cluster_pool_adapter.py`) and re-export from
   `redis.asyncio.keyspace_notifications` for backwards compatibility.
   Rationale: avoid `redis.asyncio.cluster` importing from
   `redis.asyncio.keyspace_notifications` (the dependency direction today
   is the opposite).

2. **Import hygiene**: remove `ConnectionPool` import from
   `redis/asyncio/cluster.py` if it becomes unused after the refactor
   (check `ClusterPipeline` and `RedisCluster` first — likely still used).

3. **Tests**:
   - Add unit tests mirroring
     `tests/test_asyncio/test_keyspace_notifications.py::test_receives_notification_from_any_node`
     for `ClusterPubSub` to assert that the pubsub's `connection_pool`
     after lazy node selection is a `_ClusterNodePoolAdapter` wrapping the
     target `ClusterNode` (and not a detached `ConnectionPool`).
   - Add a test that `_get_node_pubsub(node).connection_pool._node is node`.
   - Add a test verifying that after `aclose()` the connection is
     returned to `node._free` in a disconnected state (so subsequent
     regular commands do not reuse a subscribed socket).
   - For the sync side, add a test that `_get_node_pubsub` materialises
     `node.redis_connection` when it is `None`
     (via `cluster.get_redis_connection`).

4. **Sync/async parity self-check** (per `.agent/instructions.md`):
   - Public API unchanged: `ClusterPubSub.__init__`, `execute_command`,
     `ssubscribe`, `sunsubscribe`, `get_redis_connection`, `disconnect` /
     `aclose` keep their signatures and semantics.
   - Both implementations now obtain connections exclusively via
     `NodesManager`-owned resources (`node.redis_connection.connection_pool`
     for sync, `_ClusterNodePoolAdapter(node)` for async).
   - No new dependencies; only an internal module move.

### 4.4 Out-of-scope (explicitly not changed)

- The `PubSub` base class contract (single long-lived connection per
  subscriber).
- Topology-refresh / auto-resubscribe behaviour. `ClusterPubSub` currently
  has **no** auto-re-subscribe on topology change (unlike
  `ClusterKeyspaceNotifications`); adding that is a separate feature and
  is intentionally excluded from this refactor.
- Routing rules for `SSUBSCRIBE` / `SUNSUBSCRIBE` / `SPUBLISH`
  (keyslot-based), which remain exactly as they are today.
