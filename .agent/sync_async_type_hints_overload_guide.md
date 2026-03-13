# Agent Overload Implementation Guide

## Purpose
This document provides instructions for implementing `@overload` signatures for redis-py command methods.

## How to Use This Guide

### Step 1: Select a Batch
Work on one batch at a time. Each batch contains methods grouped by command class.

### Step 2: For Each Method in the Batch
1. **Verify the return type** by checking Redis docs at https://redis.io/commands
2. **Add two `@overload` signatures** before the method when the sync and async return types differ:
   - One for sync (`self: SyncClientProtocol`) returning the sync type
   - One for async (`self: AsyncClientProtocol`) returning `Awaitable[sync_type]`
   - If both clients return the same concrete type, keep a single typed method instead of adding redundant overloads
3. **Mirror the full input signature exactly** in both overloads:
   - Same parameter names, order, defaults, positional/keyword shape, `*args`, and `**kwargs` as the implementation
   - Use the same modern annotation style as return types: prefer `X | Y` and `T | None` over `Union[...]` / `Optional[...]`
4. **Keep the original implementation** unchanged except for signature annotation normalization when needed
   - For implementation return unions, prefer the readable order `SyncType | Awaitable[SyncType]` (for example `(dict | None) | Awaitable[dict | None]`)
5. **Run type checker** to verify no errors

### Step 3: Mark Batch Complete
After implementing all methods in a batch, update status in inventory.

---

## Overload Pattern Template

```python
from typing import Awaitable, overload

from redis.typing import AsyncClientProtocol, SyncClientProtocol

class SomeCommands:
    @overload
    def method(self: SyncClientProtocol, arg: str | None = None) -> SyncReturnType: ...
    @overload
    def method(
        self: AsyncClientProtocol, arg: str | None = None
    ) -> Awaitable[SyncReturnType]: ...
    def method(
        self, arg: str | None = None
    ) -> SyncReturnType | Awaitable[SyncReturnType]:
        # original implementation unchanged
        ...
```

### Protocol Definitions (in `redis/typing.py`)

```python
class SyncClientProtocol(Protocol):
    """Marker for sync clients."""
    _is_async_client: Literal[False]

class AsyncClientProtocol(Protocol):
    """Marker for async clients."""
    _is_async_client: Literal[True]
```

**Note:** We use Protocol-based discrimination (not `Redis[bytes]` / `AsyncRedis[str]`) to avoid multiplying overloads by `decode_responses` setting.

---

## Status Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Can use standard overload pattern |
| ⚠️ | Has separate async implementation - SKIP |
| 🔄 | Returns iterator - SKIP |
| ❌ | Dunder method - SKIP |
| 📋 | Already has explicit types - may still need overloads |

---

## CRITICAL: Response Callback System

Understanding the callback system is essential for determining accurate return types.

### Three-Tier Callback Architecture

The `redis-py` library uses three callback dictionaries in `redis/_parsers/helpers.py`:

| Dictionary | Lines | Purpose |
|------------|-------|---------|
| `_RedisCallbacks` | 754-844 | **Base callbacks** - shared by both RESP2 and RESP3 |
| `_RedisCallbacksRESP2` | 847-896 | **RESP2-specific** overrides/additions |
| `_RedisCallbacksRESP3` | 899-947 | **RESP3-specific** overrides/additions |

### How Callbacks Are Applied

1. When a command is executed, the library checks for a callback in this order:
   - If using RESP2: Check `_RedisCallbacksRESP2` first, then fall back to `_RedisCallbacks`
   - If using RESP3: Check `_RedisCallbacksRESP3` first, then fall back to `_RedisCallbacks`

2. If no callback exists, the raw response is returned (depends on `decode_responses` setting)

### Key Callback Functions

| Callback | Return Type | Notes |
|----------|-------------|-------|
| `bool_ok` | `bool` | Converts "OK" to `True` |
| `bool` | `bool` | Converts int to bool |
| `str_if_bytes` | `str` | Converts bytes to str regardless of decode_responses |
| `float_or_none` | `float \| None` | Parses float, returns None if null |
| `parse_scan` | `tuple[int, list]` | Cursor + keys |
| `parse_info` | `dict[str, Any]` | Parses INFO output |
| `zset_score_pairs` | `list[tuple[..., float]]` | For sorted set with scores (RESP2) |
| `zset_score_pairs_resp3` | `list[tuple[..., float]]` | For sorted set with scores (RESP3) |

### Protocol-Specific Return Types

**IMPORTANT**: Some commands have different return types depending on protocol version:

| Command | RESP2 Return | RESP3 Return | Reason |
|---------|--------------|--------------|--------|
| `acl_cat` | `list[str]` | `list[bytes \| str]` | RESP2 has `str_if_bytes`, RESP3 has no callback |
| `acl_genpass` | `str` | `bytes \| str` | RESP2 has `str_if_bytes`, RESP3 has no callback |
| `client_getname` | `str \| None` | `bytes \| str \| None` | RESP2 has `str_if_bytes`, RESP3 has no callback |
| `geohash` | `list[str]` | `list[bytes \| str]` | RESP2 has `str_if_bytes`, RESP3 has no callback |
| `hgetall` | `dict` (pairs_to_dict) | `dict` (identity) | Different parsing logic |
| `zincrby`, `zscore` | `float` (float_or_none) | `float` (raw) | RESP2 parses, RESP3 returns directly |

### How to Determine Return Type

1. **Check `_RedisCallbacks`** (base) - Is there a callback for the command?
2. **Check `_RedisCallbacksRESP2`** - Is there a RESP2-specific override?
3. **Check `_RedisCallbacksRESP3`** - Is there a RESP3-specific override?
4. **If no callback** - Return type depends on `decode_responses`:
   - Bulk string: `bytes | str`
   - Integer: `int`
   - Array: `list[...]`
   - Null: `None`

### Recommended Typing Strategy

For commands with protocol-specific differences, use the **most permissive union type**:
- If RESP2 returns `str` and RESP3 returns `bytes | str`, use `bytes | str`
- This ensures type safety regardless of protocol version

---

## Batches

### BATCH 1: ACLCommands (core.py)
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 1 | `acl_cat` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | RESP2: str_if_bytes / RESP3: raw |
| 2 | `acl_dryrun` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback |
| 3 | `acl_deluser` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 4 | `acl_genpass` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | RESP2: str_if_bytes / RESP3: raw |
| 5 | `acl_getuser` | `dict \| None` | `Awaitable[dict \| None]` | ✅ | Base: parse_acl_getuser |
| 6 | `acl_help` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | RESP2: str_if_bytes / RESP3: raw |
| 7 | `acl_list` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | RESP2: str_if_bytes / RESP3: raw |
| 8 | `acl_log` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | Base: parse_acl_log / RESP3: lambda |
| 9 | `acl_log_reset` | `bool` | `Awaitable[bool]` | ✅ | Via acl_log with RESET |
| 10 | `acl_load` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 11 | `acl_save` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 12 | `acl_setuser` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 13 | `acl_users` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | RESP2: str_if_bytes / RESP3: raw |
| 14 | `acl_whoami` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | RESP2: str_if_bytes / RESP3: raw |

### BATCH 2: ManagementCommands Part 1 (core.py) - Methods 15-50
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 15 | `auth` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 16 | `bgrewriteaof` | `bool \| bytes \| str` | `Awaitable[bool \| bytes \| str]` | ✅ | RESP2: True / RESP3: raw |
| 17 | `bgsave` | `bool \| bytes \| str` | `Awaitable[bool \| bytes \| str]` | ✅ | RESP2: True / RESP3: raw |
| 18 | `role` | `list` | `Awaitable[list]` | ✅ | No callback - mixed types |
| 19 | `client_kill` | `bool \| int` | `Awaitable[bool \| int]` | ✅ | Base: parse_client_kill |
| 20 | `client_kill_filter` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 21 | `client_info` | `dict[str, str]` | `Awaitable[dict[str, str]]` | ✅ | Base: parse_client_info |
| 22 | `client_list` | `list[dict[str, str]]` | `Awaitable[list[dict[str, str]]]` | ✅ | Base: parse_client_list |
| 23 | `client_getname` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | RESP2: str_if_bytes / RESP3: raw |
| 24 | `client_getredir` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 25 | `client_reply` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback |
| 26 | `client_id` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 27 | `client_tracking_on` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - returns raw OK |
| 28 | `client_tracking_off` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - returns raw OK |
| 29 | `client_tracking` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - returns raw OK |
| 30 | `client_trackinginfo` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | RESP2: str_if_bytes / RESP3: raw |
| 31 | `client_setname` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 32 | `client_setinfo` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 33 | `client_unblock` | `bool` | `Awaitable[bool]` | ✅ | Base: bool (converts int) |
| 34 | `client_pause` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 35 | `client_unpause` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - returns raw OK |
| 36 | `client_no_evict` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - returns raw OK |
| 37 | `client_no_touch` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - returns raw OK |
| 38 | `command` | `list` | `Awaitable[list]` | ✅ | Base: parse_command / RESP3: parse_command_resp3 |
| 39 | `command_info` | `None` | `None` | ⚠️ SKIP | N/A |
| 40 | `command_count` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 41 | `command_list` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw |
| 42 | `command_getkeysandflags` | `list[list[bytes \| str \| list[bytes \| str]]]` | `Awaitable[list[list[bytes \| str \| list[bytes \| str]]]]` | ✅ | No callback - mixed [key, flags] shape |
| 43 | `command_docs` | `dict` | `Awaitable[dict]` | ✅ | No callback - raw dict |
| 44 | `config_get` | `dict[str, str]` | `Awaitable[dict[str, str]]` | ✅ | RESP2: parse_config_get / RESP3: lambda |
| 45 | `config_set` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 46 | `config_resetstat` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 47 | `config_rewrite` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - returns raw OK |
| 48 | `dbsize` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 49 | `debug_object` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | RESP2: parse_debug_object / RESP3: raw |
| 50 | `debug_segfault` | `None` | `None` | ⚠️ SKIP | N/A |

### BATCH 3: ManagementCommands Part 2 (core.py) - Methods 51-93
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 51 | `echo` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 52 | `flushall` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 53 | `flushdb` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 54 | `sync` | `bytes` | `Awaitable[bytes]` | ✅ | No callback - raw bytes |
| 55 | `psync` | `bytes` | `Awaitable[bytes]` | ✅ | No callback - raw bytes |
| 56 | `swapdb` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 57 | `select` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 58 | `info` | `dict[str, Any]` | `Awaitable[dict[str, Any]]` | ✅ | Base: parse_info |
| 59 | `lastsave` | `datetime` | `Awaitable[datetime]` | ✅ | Base: timestamp_to_datetime |
| 60 | `latency_doctor` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 61 | `latency_graph` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 62 | `lolwut` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 63 | `reset` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | RESP2: str_if_bytes / RESP3: raw |
| 64 | `migrate` | `bool \| bytes \| str` | `Awaitable[bool \| bytes \| str]` | ✅ | No callback - NOKEY or OK |
| 65 | `object` | `Any` | `Awaitable[Any]` | ✅ | Varies by subcommand |
| 66 | `memory_doctor` | `None` | `None` | ⚠️ SKIP | N/A |
| 67 | `memory_help` | `None` | `None` | ⚠️ SKIP | N/A |
| 68 | `memory_stats` | `dict[str, Any]` | `Awaitable[dict[str, Any]]` | ✅ | RESP2: parse_memory_stats / RESP3: lambda |
| 69 | `memory_malloc_stats` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 70 | `memory_usage` | `int \| None` | `Awaitable[int \| None]` | ✅ | Integer or nil reply |
| 71 | `memory_purge` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 72 | `latency_histogram` | `dict` | `Awaitable[dict]` | ✅ | No callback - raw dict |
| 73 | `latency_history` | `list[tuple[int, int]]` | `Awaitable[list[tuple[int, int]]]` | ✅ | No callback - array of arrays |
| 74 | `latency_latest` | `list[tuple[bytes \| str, int, int, int]]` | `Awaitable[list[tuple[bytes \| str, int, int, int]]]` | ✅ | First element is key name |
| 75 | `latency_reset` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 76 | `ping` | `bool` | `Awaitable[bool]` | 📋 | Base: lambda - returns True if PONG |
| 77 | `quit` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 78 | `replicaof` | `bool` | `Awaitable[bool]` | ✅ | No callback - OK |
| 79 | `save` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 80 | `shutdown` | `None` | `None` | ⚠️ SKIP | N/A |
| 81 | `slaveof` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 82 | `slowlog_get` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | Base: parse_slowlog_get |
| 83 | `slowlog_len` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 84 | `slowlog_reset` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 85 | `time` | `tuple[int, int]` | `Awaitable[tuple[int, int]]` | ✅ | Base: lambda - tuple(secs, usecs) |
| 86 | `wait` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 87 | `waitaof` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of integers |
| 88 | `hello` | `dict` | `Awaitable[dict]` | ✅ | No callback - raw dict |
| 89 | `failover` | `bool` | `Awaitable[bool]` | ✅ | No callback - OK |
| 90 | `hotkeys_start` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 | No callback - raw |
| 91 | `hotkeys_stop` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 | No callback - raw |
| 92 | `hotkeys_reset` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 | No callback - raw |
| 93 | `hotkeys_get` | `list[dict]` | `Awaitable[list[dict]]` | 📋 | RESP2: lambda pairs_to_dict |

### BATCH 4: BasicKeyCommands Part 1 (core.py) - Methods 94-130
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 94 | `append` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 95 | `bitcount` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 96 | `bitfield` | `BitFieldOperation` | `BitFieldOperation` | 📋 | Returns operation builder |
| 97 | `bitfield_ro` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of integers |
| 98 | `bitop` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 99 | `bitpos` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 100 | `copy` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 101 | `decrby` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 102 | `delete` | `int` | `Awaitable[int]` | ✅ | Integer reply - count deleted |
| 103 | `__delitem__` | `None` | N/A | ❌ SKIP | Dunder |
| 104 | `delex` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 105 | `dump` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ | Always bytes (serialized) |
| 106 | `exists` | `int` | `Awaitable[int]` | ✅ | Integer reply - count |
| 107 | `expire` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 108 | `expireat` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 109 | `expiretime` | `int` | `Awaitable[int]` | 📋 | Integer - timestamp |
| 110 | `digest_local` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 | No callback - raw |
| 111 | `digest` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | No callback - raw |
| 112 | `get` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ DONE | No callback - raw |
| 113 | `getdel` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | No callback - raw |
| 114 | `getex` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | No callback - raw |
| 115 | `__getitem__` | `bytes \| str` | N/A | ❌ SKIP | Dunder |
| 116 | `getbit` | `int` | `Awaitable[int]` | ✅ | Integer reply - 0 or 1 |
| 117 | `getrange` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 118 | `getset` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | No callback - raw |
| 119 | `incrby` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 120 | `incrbyfloat` | `float` | `Awaitable[float]` | ✅ | Base: float |
| 121 | `keys` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw array |
| 122 | `lmove` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | No callback - raw |
| 123 | `blmove` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | No callback - raw |
| 124 | `mget` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ | No callback - raw array |
| 125 | `mset` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 126 | `msetex` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 127 | `msetnx` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 128 | `move` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 129 | `persist` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 130 | `pexpire` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |

### BATCH 5: BasicKeyCommands Part 2 (core.py) - Methods 131-156
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 131 | `pexpireat` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 132 | `pexpiretime` | `int` | `Awaitable[int]` | ✅ | Integer - timestamp in ms |
| 133 | `psetex` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 134 | `pttl` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 135 | `hrandfield` | `bytes \| str \| list \| None` | `Awaitable[bytes \| str \| list \| None]` | ✅ | Varies by COUNT param |
| 136 | `randomkey` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | No callback - raw |
| 137 | `rename` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 138 | `renamenx` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 139 | `restore` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw OK |
| 140 | `set` | `bool \| None` | `Awaitable[bool \| None]` | ✅ DONE | Base: parse_set_result |
| 141 | `__setitem__` | `None` | N/A | ❌ SKIP | Dunder |
| 142 | `setbit` | `int` | `Awaitable[int]` | ✅ | Integer reply - prev bit |
| 143 | `setex` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 144 | `setnx` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 145 | `setrange` | `int` | `Awaitable[int]` | ✅ | Integer reply - new length |
| 146 | `stralgo` | `dict \| str \| int` | `Awaitable[dict \| str \| int]` | ✅ | RESP2: parse_stralgo / RESP3: lambda |
| 147 | `strlen` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 148 | `substr` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 149 | `touch` | `int` | `Awaitable[int]` | ✅ | Integer reply - count |
| 150 | `ttl` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 151 | `type` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 152 | `watch` | `bool` | `Awaitable[bool]` | ⚠️ SKIP | Transaction |
| 153 | `unwatch` | `bool` | `Awaitable[bool]` | ⚠️ SKIP | Transaction |
| 154 | `unlink` | `int` | `Awaitable[int]` | ✅ | Integer reply - count |
| 155 | `lcs` | `bytes \| str \| int \| list \| dict` | `Awaitable[bytes \| str \| int \| list \| dict]` | ✅ | Raw reply varies by options and protocol |
| 156 | `__contains__` | `bool` | N/A | ❌ SKIP | Dunder |

### BATCH 6: ListCommands (core.py) - Methods 157-178
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 157 | `blpop` | `tuple[bytes \| str, bytes \| str] \| list[bytes \| str] \| None` | `Awaitable[tuple[bytes \| str, bytes \| str] \| list[bytes \| str] \| None]` | ✅ | RESP2: tuple / RESP3: raw list |
| 158 | `brpop` | `tuple[bytes \| str, bytes \| str] \| list[bytes \| str] \| None` | `Awaitable[tuple[bytes \| str, bytes \| str] \| list[bytes \| str] \| None]` | ✅ | RESP2: tuple / RESP3: raw list |
| 159 | `brpoplpush` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | No callback - raw |
| 160 | `blmpop` | `list[bytes \| str \| list[bytes \| str]] \| None` | `Awaitable[list[bytes \| str \| list[bytes \| str]] \| None]` | ✅ | No callback - nested [key, values] shape |
| 161 | `lmpop` | `list[bytes \| str \| list[bytes \| str]] \| None` | `Awaitable[list[bytes \| str \| list[bytes \| str]] \| None]` | ✅ | No callback - nested [key, values] shape |
| 162 | `lindex` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | No callback - raw |
| 163 | `linsert` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 164 | `llen` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 165 | `lpop` | `bytes \| str \| list[bytes \| str] \| None` | `Awaitable[bytes \| str \| list[bytes \| str] \| None]` | ✅ | Varies by COUNT |
| 166 | `lpush` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 167 | `lpushx` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 168 | `lrange` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw array |
| 169 | `lrem` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 170 | `lset` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 171 | `ltrim` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 172 | `rpop` | `bytes \| str \| list[bytes \| str] \| None` | `Awaitable[bytes \| str \| list[bytes \| str] \| None]` | ✅ | Varies by COUNT |
| 173 | `rpoplpush` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | No callback - raw |
| 174 | `rpush` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 175 | `rpushx` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 176 | `lpos` | `int \| list[int] \| None` | `Awaitable[int \| list[int] \| None]` | ✅ | Varies by COUNT/RANK |
| 177 | `sort` | `list[bytes \| str] \| list[tuple[bytes \| str, ...]] \| int` | `Awaitable[list[bytes \| str] \| list[tuple[bytes \| str, ...]] \| int]` | ✅ | Base: sort_return_tuples incl. grouped tuples |
| 178 | `sort_ro` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw array |

### BATCH 7: ScanCommands + SetCommands (core.py) - Methods 179-202
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 179 | `scan` | `tuple[int, list[bytes \| str]]` | `Awaitable[tuple[int, list[bytes \| str]]]` | ✅ | Base: parse_scan |
| 180 | `scan_iter` | `Iterator` | `AsyncIterator` | 🔄 SKIP | Iterator |
| 181 | `sscan` | `tuple[int, list[bytes \| str]]` | `Awaitable[tuple[int, list[bytes \| str]]]` | ✅ | Base: parse_scan |
| 182 | `sscan_iter` | `Iterator` | `AsyncIterator` | 🔄 SKIP | Iterator |
| 183 | `hscan` | `tuple[int, dict[bytes \| str, bytes \| str] \| list[bytes \| str]]` | `Awaitable[tuple[int, dict[bytes \| str, bytes \| str] \| list[bytes \| str]]]` | ✅ | Base: parse_hscan, `NOVALUES` returns key list |
| 184 | `hscan_iter` | `Iterator` | `AsyncIterator` | 🔄 SKIP | Iterator |
| 185 | `zscan` | `tuple[int, list[tuple[bytes \| str, float]]]` | `Awaitable[tuple[int, list[tuple[bytes \| str, float]]]]` | ✅ | Base: parse_zscan |
| 186 | `zscan_iter` | `Iterator` | `AsyncIterator` | 🔄 SKIP | Iterator |
| 187 | `sadd` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 188 | `scard` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 189 | `sdiff` | `set[bytes \| str]` | `Awaitable[set[bytes \| str]]` | ✅ | RESP2+RESP3: lambda set |
| 190 | `sdiffstore` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 191 | `sinter` | `set[bytes \| str]` | `Awaitable[set[bytes \| str]]` | ✅ | RESP2+RESP3: lambda set |
| 192 | `sintercard` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 193 | `sinterstore` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 194 | `Literal[0] \| Literal[1]` | `Awaitable[Literal[0] \| Literal[1]]` | ✅ | Integer 0/1 - no callback |
| 195 | `smembers` | `set[bytes \| str]` | `Awaitable[set[bytes \| str]]` | ✅ | RESP2+RESP3: lambda set |
| 196 | `smismember` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of 0/1 |
| 197 | `smove` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 198 | `spop` | `bytes \| str \| set[bytes \| str] \| None` | `Awaitable[bytes \| str \| set[bytes \| str] \| None]` | ✅ | Varies by COUNT, count form returns a set |
| 199 | `srandmember` | `bytes \| str \| list[bytes \| str] \| None` | `Awaitable[bytes \| str \| list[bytes \| str] \| None]` | ✅ | Varies by NUMBER |
| 200 | `srem` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 201 | `sunion` | `set[bytes \| str]` | `Awaitable[set[bytes \| str]]` | ✅ | RESP2+RESP3: lambda set |
| 202 | `sunionstore` | `int` | `Awaitable[int]` | ✅ | Integer reply |

### BATCH 8: StreamCommands (core.py) - Methods 203-226
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 203 | `xack` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 204 | `xackdel` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 205 | `xadd` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - stream ID |
| 206 | `xcfgset` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw OK |
| 207 | `xautoclaim` | `list[Any]` | `Awaitable[list[Any]]` | ✅ | Base: parse_xautoclaim / JUSTID special case |
| 208 | `xclaim` | `list[tuple[bytes \| str \| None, dict \| None]] \| list[bytes \| str]` | `Awaitable[list[tuple[bytes \| str \| None, dict \| None]] \| list[bytes \| str]]` | ✅ | Base: parse_xclaim / JUSTID returns ID list |
| 209 | `xdel` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 210 | `xdelex` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 211 | `xgroup_create` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 212 | `xgroup_delconsumer` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 213 | `xgroup_destroy` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 214 | `xgroup_createconsumer` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 215 | `xgroup_setid` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 216 | `xinfo_consumers` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | RESP2: parse_list_of_dicts / RESP3: lambda |
| 217 | `xinfo_groups` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | RESP2: parse_list_of_dicts / RESP3: lambda |
| 218 | `xinfo_stream` | `dict[str, Any]` | `Awaitable[dict[str, Any]]` | ✅ | Base: parse_xinfo_stream |
| 219 | `xlen` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 220 | `xpending` | `dict[str, Any]` | `Awaitable[dict[str, Any]]` | ✅ | Base: parse_xpending |
| 221 | `xpending_range` | `list[dict[str, bytes \| str \| int]]` | `Awaitable[list[dict[str, bytes \| str \| int]]]` | ✅ | parse_xpending_range detail rows |
| 222 | `xrange` | `list[tuple[bytes \| str \| None, dict \| None]] \| None` | `Awaitable[list[tuple[bytes \| str \| None, dict \| None]] \| None]` | ✅ | Base: parse_stream_list |
| 223 | `xread` | `list \| dict` | `Awaitable[list \| dict]` | ✅ | RESP2: parse_xread / RESP3: parse_xread_resp3 |
| 224 | `xreadgroup` | `list \| dict` | `Awaitable[list \| dict]` | ✅ | RESP2: parse_xread / RESP3: parse_xread_resp3 |
| 225 | `xrevrange` | `list[tuple[bytes \| str \| None, dict \| None]] \| None` | `Awaitable[list[tuple[bytes \| str \| None, dict \| None]] \| None]` | ✅ | Base: parse_stream_list |
| 226 | `xtrim` | `int` | `Awaitable[int]` | ✅ | Integer reply |

### BATCH 9: SortedSetCommands Part 1 (core.py) - Methods 227-260
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 227 | `zadd` | `int \| float` | `Awaitable[int \| float]` | ✅ | RESP2: parse_zadd |
| 228 | `zcard` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 229 | `zcount` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 230 | `zdiff` | `ZSetRangeResponse` | `Awaitable[ZSetRangeResponse]` | ✅ | `WITHSCORES`: RESP2 tuple pairs / RESP3 raw nested lists |
| 231 | `zdiffstore` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 232 | `zincrby` | `float \| None` | `Awaitable[float \| None]` | ✅ | RESP2: `float_or_none` / RESP3: raw float |
| 233 | `zinter` | `ZSetRangeResponse` | `Awaitable[ZSetRangeResponse]` | ✅ | `score_cast_func` means scored branch uses `Any` |
| 234 | `zinterstore` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 235 | `zintercard` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 236 | `zlexcount` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 237 | `zpopmax` | `ZSetRangeResponse` | `Awaitable[ZSetRangeResponse]` | ✅ | RESP2 tuple pairs / RESP3 nested lists |
| 238 | `zpopmin` | `ZSetRangeResponse` | `Awaitable[ZSetRangeResponse]` | ✅ | RESP2 tuple pairs / RESP3 nested lists |
| 239 | `zrandmember` | `ZRandMemberResponse` | `Awaitable[ZRandMemberResponse]` | ✅ | COUNT / WITHSCORES shape varies by protocol |
| 240 | `bzpopmax` | `BlockingZSetPopResponse` | `Awaitable[BlockingZSetPopResponse]` | ✅ | RESP2 tuple / RESP3 raw list / `None` |
| 241 | `bzpopmin` | `BlockingZSetPopResponse` | `Awaitable[BlockingZSetPopResponse]` | ✅ | RESP2 tuple / RESP3 raw list / `None` |
| 242 | `zmpop` | `ZMPopResponse` | `Awaitable[ZMPopResponse]` | ✅ | Raw `[key, [[member, score], ...]]` or `None` |
| 243 | `bzmpop` | `ZMPopResponse` | `Awaitable[ZMPopResponse]` | ✅ | Raw `[key, [[member, score], ...]]` or `None` |
| 244 | `zrange` | `ZSetRangeResponse` | `Awaitable[ZSetRangeResponse]` | ✅ | `score_cast_func` means scored branch uses `Any` |
| 245 | `zrevrange` | `ZSetRangeResponse` | `Awaitable[ZSetRangeResponse]` | ✅ | `score_cast_func` means scored branch uses `Any` |
| 246 | `zrangestore` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 247 | `zrangebylex` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw array |
| 248 | `zrevrangebylex` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw array |
| 249 | `zrangebyscore` | `ZSetRangeResponse` | `Awaitable[ZSetRangeResponse]` | ✅ | `score_cast_func` means scored branch uses `Any` |
| 250 | `zrevrangebyscore` | `ZSetRangeResponse` | `Awaitable[ZSetRangeResponse]` | ✅ | `score_cast_func` means scored branch uses `Any` |
| 251 | `zrank` | `ZRankResponse` | `Awaitable[ZRankResponse]` | ✅ | `WITHSCORE` returns `[rank, score]`, not tuple |
| 252 | `zrem` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 253 | `zremrangebylex` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 254 | `zremrangebyrank` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 255 | `zremrangebyscore` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 256 | `zrevrank` | `ZRankResponse` | `Awaitable[ZRankResponse]` | ✅ | `WITHSCORE` returns `[rank, score]`, not tuple |
| 257 | `zscore` | `float \| None` | `Awaitable[float \| None]` | ✅ | RESP2: `float_or_none` / RESP3: raw float |
| 258 | `zunion` | `ZSetRangeResponse` | `Awaitable[ZSetRangeResponse]` | ✅ | `score_cast_func` means scored branch uses `Any` |
| 259 | `zunionstore` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 260 | `zmscore` | `list[float \| None]` | `Awaitable[list[float \| None]]` | ✅ | RESP2: `parse_zmscore` / RESP3: raw float list |

### BATCH 10: HyperlogCommands + HashCommands (core.py) - Methods 261-289
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 261 | `pfadd` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 262 | `pfcount` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 263 | `pfmerge` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 264 | `hdel` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 265 | `hexists` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 266 | `hget` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ | No callback - raw |
| 267 | `hgetall` | `dict[bytes \| str, bytes \| str]` | `Awaitable[dict[bytes \| str, bytes \| str]]` | ✅ | RESP2: pairs_to_dict / RESP3: identity |
| 268 | `hgetdel` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ | No callback - one result per requested field |
| 269 | `hgetex` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ | No callback - one result per requested field |
| 270 | `hincrby` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 271 | `hincrbyfloat` | `float` | `Awaitable[float]` | ✅ | Base: float |
| 272 | `hkeys` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw array |
| 273 | `hlen` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 274 | `hset` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 275 | `hsetex` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 276 | `hsetnx` | `int` | `Awaitable[int]` | ✅ | No callback - integer 0/1 reply |
| 277 | `hmset` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 278 | `hmget` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ | No callback - raw array |
| 279 | `hvals` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw array |
| 280 | `hstrlen` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 281 | `hexpire` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of integers |
| 282 | `hpexpire` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of integers |
| 283 | `hexpireat` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of integers |
| 284 | `hpexpireat` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of integers |
| 285 | `hpersist` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of integers |
| 286 | `hexpiretime` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of integers |
| 287 | `hpexpiretime` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of integers |
| 288 | `httl` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of integers |
| 289 | `hpttl` | `list[int]` | `Awaitable[list[int]]` | ✅ | Array of integers |

### BATCH 11: PubSubCommands + ScriptCommands (core.py) - Methods 290-306
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 290 | `publish` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 291 | `spublish` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 292 | `pubsub_channels` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw |
| 293 | `pubsub_shardchannels` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw |
| 294 | `pubsub_numpat` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 295 | `pubsub_numsub` | `list[tuple[bytes \| str, int]]` | `Awaitable[list[tuple[bytes \| str, int]]]` | ✅ | Base: parse_pubsub_numsub |
| 296 | `pubsub_shardnumsub` | `list[tuple[bytes \| str, int]]` | `Awaitable[list[tuple[bytes \| str, int]]]` | ✅ | Base: parse_pubsub_numsub |
| 297 | `eval` | `Any` | `Awaitable[Any]` | ✅ | Script-dependent |
| 298 | `eval_ro` | `Any` | `Awaitable[Any]` | ✅ | Script-dependent |
| 299 | `evalsha` | `Any` | `Awaitable[Any]` | ✅ | Script-dependent |
| 300 | `evalsha_ro` | `Any` | `Awaitable[Any]` | ✅ | Script-dependent |
| 301 | `script_exists` | `list[bool]` | `Awaitable[list[bool]]` | ✅ | Base: lambda map bool |
| 302 | `script_debug` | `None` | `None` | ⚠️ SKIP | N/A |
| 303 | `script_flush` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 304 | `script_kill` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 305 | `script_load` | `str` | `Awaitable[str]` | ✅ | Base: str_if_bytes |
| 306 | `register_script` | `Script` | `AsyncScript` | ⚠️ SKIP | Different classes |

### BATCH 12: GeoCommands + ModuleCommands + ClusterCommands + FunctionCommands (core.py) - Methods 307-335
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 307 | `geoadd` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 308 | `geodist` | `float \| None` | `Awaitable[float \| None]` | ✅ | Base: float_or_none |
| 309 | `geohash` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ | RESP2: str_if_bytes / RESP3: raw |
| 310 | `geopos` | `list[tuple[float, float] \| None]` | `Awaitable[list[tuple[float, float] \| None]]` | ✅ | RESP2: lambda float tuple / RESP3: raw |
| 311 | `georadius` | `list` | `Awaitable[list]` | ✅ | Base: parse_geosearch_generic |
| 312 | `georadiusbymember` | `list` | `Awaitable[list]` | ✅ | Base: parse_geosearch_generic |
| 313 | `geosearch` | `list` | `Awaitable[list]` | ✅ | Base: parse_geosearch_generic |
| 314 | `geosearchstore` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 315 | `module_load` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 316 | `module_loadex` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 317 | `module_unload` | `bool` | `Awaitable[bool]` | ✅ | Base: bool |
| 318 | `module_list` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | RESP2: lambda pairs_to_dict |
| 319 | `command_info` | `None` | `None` | ⚠️ SKIP | N/A |
| 320 | `command_count` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 321 | `command_getkeys` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | RESP2: str_if_bytes / RESP3: raw |
| 322 | `command` | `list` | `Awaitable[list]` | ✅ | Base: parse_command / RESP3: parse_command_resp3 |
| 323 | `cluster` | `Any` | `Awaitable[Any]` | ✅ | Subcommand-dependent |
| 324 | `readwrite` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 325 | `readonly` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 326 | `function_load` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 327 | `function_delete` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 328 | `function_flush` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 329 | `function_list` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | No callback - raw |
| 330 | `fcall` | `Any` | `Awaitable[Any]` | ✅ | Function-dependent |
| 331 | `fcall_ro` | `Any` | `Awaitable[Any]` | ✅ | Function-dependent |
| 332 | `function_dump` | `bytes` | `Awaitable[bytes]` | ✅ | No callback - raw bytes |
| 333 | `function_restore` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 334 | `function_kill` | `bool` | `Awaitable[bool]` | ✅ | No callback - OK |
| 335 | `function_stats` | `dict` | `Awaitable[dict]` | ✅ | No callback - raw dict |

### BATCH 13: ClusterCommands (cluster.py) - Methods 336-378
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 336 | `mget_nonatomic` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ⚠️ SKIP | Complex multi-node |
| 337 | `mset_nonatomic` | `list[bool]` | `Awaitable[list[bool]]` | ⚠️ SKIP | Complex multi-node |
| 338 | `exists` | `int` | `Awaitable[int]` | 📋 | Integer reply |
| 339 | `delete` | `int` | `Awaitable[int]` | 📋 | Integer reply |
| 340 | `touch` | `int` | `Awaitable[int]` | 📋 | Integer reply |
| 341 | `unlink` | `int` | `Awaitable[int]` | 📋 | Integer reply |
| 342 | `slaveof` | `NoReturn` | `NoReturn` | 📋 SKIP | Raises error |
| 343 | `replicaof` | `NoReturn` | `NoReturn` | 📋 SKIP | Raises error |
| 344 | `swapdb` | `NoReturn` | `NoReturn` | 📋 SKIP | Raises error |
| 345 | `cluster_myid` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 346 | `cluster_addslots` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 347 | `cluster_addslotsrange` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 348 | `cluster_countkeysinslot` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 349 | `cluster_count_failure_report` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 350 | `cluster_delslots` | `list[bool]` | `Awaitable[list[bool]]` | ⚠️ SKIP | Complex multi-node |
| 351 | `cluster_delslotsrange` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 352 | `cluster_failover` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 353 | `cluster_info` | `dict[str, str]` | `Awaitable[dict[str, str]]` | ✅ | Base: parse_cluster_info |
| 354 | `cluster_keyslot` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 355 | `cluster_meet` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 356 | `cluster_nodes` | `str` | `Awaitable[str]` | ✅ | Base: parse_cluster_nodes |
| 357 | `cluster_replicate` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 358 | `cluster_reset` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 359 | `cluster_save_config` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 360 | `cluster_get_keys_in_slot` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | RESP2: str_if_bytes / RESP3: raw |
| 361 | `cluster_set_config_epoch` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 362 | `cluster_setslot` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 363 | `cluster_setslot_stable` | `bool` | `Awaitable[bool]` | ✅ | No callback - OK |
| 364 | `cluster_replicas` | `str` | `Awaitable[str]` | ✅ | Base: parse_cluster_nodes |
| 365 | `cluster_slots` | `list` | `Awaitable[list]` | ✅ | No callback - raw |
| 366 | `cluster_shards` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | No callback - raw |
| 367 | `cluster_myshardid` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 368 | `cluster_links` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | No callback - raw |
| 369 | `cluster_flushslots` | `bool` | `Awaitable[bool]` | ✅ | No callback - OK |
| 370 | `cluster_bumpepoch` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 371 | `client_tracking_on` | `bytes \| str` | `Awaitable[bytes \| str]` | ⚠️ SKIP | Cluster-specific, no callback |
| 372 | `client_tracking_off` | `bytes \| str` | `Awaitable[bytes \| str]` | ⚠️ SKIP | Cluster-specific, no callback |
| 373 | `hotkeys_start` | `bytes \| str` | `Awaitable[bytes \| str]` | ⚠️ SKIP | Cluster-specific |
| 374 | `hotkeys_stop` | `bytes \| str` | `Awaitable[bytes \| str]` | ⚠️ SKIP | Cluster-specific |
| 375 | `hotkeys_reset` | `bytes \| str` | `Awaitable[bytes \| str]` | ⚠️ SKIP | Cluster-specific |
| 376 | `hotkeys_get` | `list[dict]` | `Awaitable[list[dict]]` | ⚠️ SKIP | Cluster-specific |
| 377 | `stralgo` | `dict \| bytes \| str \| int` | `Awaitable[dict \| bytes \| str \| int]` | ✅ | RESP2: parse_stralgo / RESP3: lambda |
| 378 | `scan_iter` | `Iterator` | `AsyncIterator` | 🔄 SKIP | Iterator |

### BATCH 14: SentinelCommands (sentinel.py) - Methods 379-391
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 379 | `sentinel` | `Any` | `Awaitable[Any]` | ⚠️ SKIP | Async differs |
| 380 | `sentinel_get_master_addr_by_name` | `tuple[str, int] \| None` | `Awaitable[tuple[str, int] \| None]` | ✅ | Base: parse_sentinel_get_master |
| 381 | `sentinel_master` | `dict` | `Awaitable[dict]` | ✅ | RESP2: parse_sentinel_master / RESP3: parse_sentinel_state_resp3 |
| 382 | `sentinel_masters` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | RESP2: parse_sentinel_masters / RESP3: parse_sentinel_masters_resp3 |
| 383 | `sentinel_monitor` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 384 | `sentinel_remove` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 385 | `sentinel_sentinels` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | RESP2: parse_sentinel_slaves_and_sentinels / RESP3: _resp3 |
| 386 | `sentinel_set` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 387 | `sentinel_slaves` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | RESP2: parse_sentinel_slaves_and_sentinels / RESP3: _resp3 |
| 388 | `sentinel_reset` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 389 | `sentinel_failover` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 390 | `sentinel_ckquorum` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |
| 391 | `sentinel_flushconfig` | `bool` | `Awaitable[bool]` | ✅ | Base: bool_ok |

### BATCH 15: SearchCommands (search/commands.py) - Methods 392-424
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 392 | `batch_indexer` | `BatchIndexer` | `BatchIndexer` | 📋 SKIP | Builder pattern |
| 393 | `create_index` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 394 | `alter_schema_add` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 395 | `dropindex` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 396 | `add_document` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 397 | `add_document_hash` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 398 | `delete_document` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 399 | `load_document` | `Document` | `Awaitable[Document]` | ⚠️ SKIP | Async result parsing |
| 400 | `get` | `Document` | `Awaitable[Document]` | ✅ | Module-specific |
| 401 | `info` | `dict` | `Awaitable[dict]` | ⚠️ SKIP | Async result parsing |
| 402 | `get_params_args` | `list` | `list` | 📋 SKIP | Helper method |
| 403 | `search` | `Result` | `Awaitable[Result]` | ⚠️ SKIP | Async result parsing |
| 404 | `hybrid_search` | `HybridResult` | `Awaitable[HybridResult]` | ⚠️ SKIP | Async result parsing |
| 405 | `explain` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | No callback - raw |
| 406 | `explain_cli` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw |
| 407 | `aggregate` | `AggregateResult` | `Awaitable[AggregateResult]` | ⚠️ SKIP | Async result parsing |
| 408 | `profile` | `tuple` | `Awaitable[tuple]` | ✅ | Module-specific |
| 409 | `spellcheck` | `dict` | `Awaitable[dict]` | ⚠️ SKIP | Async result parsing |
| 410 | `dict_add` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 411 | `dict_del` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 412 | `dict_dump` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw |
| 413 | `config_set` | `bool` | `Awaitable[bool]` | ⚠️ SKIP | Async result parsing |
| 414 | `config_get` | `dict` | `Awaitable[dict]` | ⚠️ SKIP | Async result parsing |
| 415 | `tagvals` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | No callback - raw |
| 416 | `aliasadd` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 417 | `aliasupdate` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 418 | `aliasdel` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 419 | `sugadd` | `int` | `Awaitable[int]` | ⚠️ SKIP | Async result parsing |
| 420 | `suglen` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 421 | `sugdel` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 422 | `sugget` | `list` | `Awaitable[list]` | ⚠️ SKIP | Async result parsing |
| 423 | `synupdate` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 424 | `syndump` | `dict[bytes \| str, list[bytes \| str]]` | `Awaitable[dict[bytes \| str, list[bytes \| str]]]` | ✅ | No callback - raw |

### BATCH 16: JSONCommands (json/commands.py) - Methods 425-452
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 425 | `arrappend` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | Module int array |
| 426 | `arrindex` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | Module int array |
| 427 | `arrinsert` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | Module int array |
| 428 | `arrlen` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | Module int array |
| 429 | `arrpop` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ | Depends on decode_responses |
| 430 | `arrtrim` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | Module int array |
| 431 | `type` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | Depends on decode_responses |
| 432 | `resp` | `list` | `Awaitable[list]` | ✅ | JSON structure |
| 433 | `objkeys` | `list[list[bytes \| str] \| None]` | `Awaitable[list[list[bytes \| str] \| None]]` | ✅ | Depends on decode_responses |
| 434 | `objlen` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | Module int array |
| 435 | `numincrby` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | Depends on decode_responses |
| 436 | `nummultby` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ | Depends on decode_responses |
| 437 | `clear` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 438 | `delete` | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 439 | `get` | `Any` | `Awaitable[Any]` | ✅ | JSON parsed |
| 440 | `mget` | `list[Any]` | `Awaitable[list[Any]]` | ✅ | JSON parsed |
| 441 | `set` | `bool \| None` | `Awaitable[bool \| None]` | ✅ | OK or None |
| 442 | `mset` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 443 | `merge` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 444 | `set_file` | `bool \| None` | `Awaitable[bool \| None]` | 📋 | Explicit |
| 445 | `set_path` | `dict[str, bool]` | `Awaitable[dict[str, bool]]` | 📋 | Explicit |
| 446 | `strlen` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | Module int array |
| 447 | `toggle` | `bool \| list[bool]` | `Awaitable[bool \| list[bool]]` | ✅ | Module bool |
| 448 | `strappend` | `int \| list[int \| None]` | `Awaitable[int \| list[int \| None]]` | ✅ | Module int |
| 449 | `debug` | `int \| list[bytes \| str]` | `Awaitable[int \| list[bytes \| str]]` | ✅ | Module mixed |
| 450 | `jsonget` | `Any` | `Awaitable[Any]` | ✅ | Deprecated alias |
| 451 | `jsonmget` | `Any` | `Awaitable[Any]` | ✅ | Deprecated alias |
| 452 | `jsonset` | `Any` | `Awaitable[Any]` | ✅ | Deprecated alias |

### BATCH 17: TimeSeriesCommands (timeseries/commands.py) - Methods 453-469
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 453 | `create` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 454 | `alter` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 455 | `add` | `int` | `Awaitable[int]` | ✅ | Timestamp |
| 456 | `madd` | `list[int]` | `Awaitable[list[int]]` | ✅ | Timestamps |
| 457 | `incrby` | `int` | `Awaitable[int]` | ✅ | Timestamp |
| 458 | `decrby` | `int` | `Awaitable[int]` | ✅ | Timestamp |
| 459 | `delete` | `int` | `Awaitable[int]` | ✅ | Count deleted |
| 460 | `createrule` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 461 | `deleterule` | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 462 | `range` | `list[tuple[int, float]]` | `Awaitable[list[tuple[int, float]]]` | ✅ | Parsed samples |
| 463 | `revrange` | `list[tuple[int, float]]` | `Awaitable[list[tuple[int, float]]]` | ✅ | Parsed samples |
| 464 | `mrange` | `list` | `Awaitable[list]` | ✅ | Complex structure |
| 465 | `mrevrange` | `list` | `Awaitable[list]` | ✅ | Complex structure |
| 466 | `get` | `tuple[int, float] \| list` | `Awaitable[tuple[int, float] \| list]` | ✅ | Parsed sample |
| 467 | `mget` | `list` | `Awaitable[list]` | ✅ | Complex structure |
| 468 | `info` | `TSInfo` | `Awaitable[TSInfo]` | 📋 | Explicit type |
| 469 | `queryindex` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | Depends on decode_responses |

### BATCH 18: BloomFilter Commands (bf/commands.py) - Methods 470-491
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 470 | `create` (BF) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 471 | `add` (BF) | `int` | `Awaitable[int]` | ✅ | 0 or 1 |
| 472 | `madd` (BF) | `list[int]` | `Awaitable[list[int]]` | ✅ | 0s and 1s |
| 473 | `insert` (BF) | `list[int]` | `Awaitable[list[int]]` | ✅ | 0s and 1s |
| 474 | `exists` (BF) | `int` | `Awaitable[int]` | ✅ | 0 or 1 |
| 475 | `mexists` (BF) | `list[int]` | `Awaitable[list[int]]` | ✅ | 0s and 1s |
| 476 | `scandump` (BF) | `tuple[int, bytes \| None]` | `Awaitable[tuple[int, bytes \| None]]` | ✅ | Cursor + data |
| 477 | `loadchunk` (BF) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 478 | `info` (BF) | `dict` | `Awaitable[dict]` | ✅ | Parsed info |
| 479 | `card` (BF) | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 480 | `create` (CF) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 481 | `add` (CF) | `int` | `Awaitable[int]` | ✅ | 0 or 1 |
| 482 | `addnx` (CF) | `int` | `Awaitable[int]` | ✅ | 0 or 1 |
| 483 | `insert` (CF) | `list[int]` | `Awaitable[list[int]]` | ✅ | 0s and 1s |
| 484 | `insertnx` (CF) | `list[int]` | `Awaitable[list[int]]` | ✅ | 0s and 1s |
| 485 | `exists` (CF) | `int` | `Awaitable[int]` | ✅ | 0 or 1 |
| 486 | `mexists` (CF) | `list[int]` | `Awaitable[list[int]]` | ✅ | 0s and 1s |
| 487 | `delete` (CF) | `int` | `Awaitable[int]` | ✅ | 0 or 1 |
| 488 | `count` (CF) | `int` | `Awaitable[int]` | ✅ | Integer reply |
| 489 | `scandump` (CF) | `tuple[int, bytes \| None]` | `Awaitable[tuple[int, bytes \| None]]` | ✅ | Cursor + data |
| 490 | `loadchunk` (CF) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 491 | `info` (CF) | `dict` | `Awaitable[dict]` | ✅ | Parsed info |

### BATCH 19: TOPKCommands + TDigestCommands + CMSCommands (bf/commands.py) - Methods 492-518
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 492 | `reserve` (TOPK) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 493 | `add` (TOPK) | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ | Depends on decode_responses |
| 494 | `incrby` (TOPK) | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ | Depends on decode_responses |
| 495 | `query` (TOPK) | `list[int]` | `Awaitable[list[int]]` | ✅ | 0s and 1s |
| 496 | `count` (TOPK) | `list[int]` | `Awaitable[list[int]]` | ✅ | Counts |
| 497 | `list` (TOPK) | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ | Depends on decode_responses |
| 498 | `info` (TOPK) | `dict` | `Awaitable[dict]` | ✅ | Parsed info |
| 499 | `create` (TD) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 500 | `reset` (TD) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 501 | `add` (TD) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 502 | `merge` (TD) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 503 | `min` (TD) | `float` | `Awaitable[float]` | ✅ | Float reply |
| 504 | `max` (TD) | `float` | `Awaitable[float]` | ✅ | Float reply |
| 505 | `quantile` (TD) | `list[float]` | `Awaitable[list[float]]` | ✅ | Floats |
| 506 | `cdf` (TD) | `list[float]` | `Awaitable[list[float]]` | ✅ | Floats |
| 507 | `info` (TD) | `dict` | `Awaitable[dict]` | ✅ | Parsed info |
| 508 | `trimmed_mean` (TD) | `float` | `Awaitable[float]` | ✅ | Float reply |
| 509 | `rank` (TD) | `list[int]` | `Awaitable[list[int]]` | ✅ | Integers |
| 510 | `revrank` (TD) | `list[int]` | `Awaitable[list[int]]` | ✅ | Integers |
| 511 | `byrank` (TD) | `list[float]` | `Awaitable[list[float]]` | ✅ | Floats |
| 512 | `byrevrank` (TD) | `list[float]` | `Awaitable[list[float]]` | ✅ | Floats |
| 513 | `initbydim` (CMS) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 514 | `initbyprob` (CMS) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 515 | `incrby` (CMS) | `list[int]` | `Awaitable[list[int]]` | ✅ | Integers |
| 516 | `query` (CMS) | `list[int]` | `Awaitable[list[int]]` | ✅ | Integers |
| 517 | `merge` (CMS) | `bool` | `Awaitable[bool]` | ✅ | Module OK |
| 518 | `info` (CMS) | `dict` | `Awaitable[dict]` | ✅ | Parsed info |

### BATCH 20: VectorSetCommands (vectorset/commands.py) - Methods 519-530 ✅ DONE
| # | Method | Sync Type | Async Type | Status | Notes |
|---|--------|-----------|------------|--------|-------|
| 519 | `vadd` | `int` | `Awaitable[int]` | ✅ DONE | Integer reply |
| 520 | `vsim` | `VSimResult` | `Awaitable[VSimResult]` | 📋 DONE | Explicit type |
| 521 | `vdim` | `int` | `Awaitable[int]` | ✅ DONE | Integer reply |
| 522 | `vcard` | `int` | `Awaitable[int]` | ✅ DONE | Integer reply |
| 523 | `vrem` | `int` | `Awaitable[int]` | ✅ DONE | Integer reply |
| 524 | `vemb` | `VEmbResult` | `Awaitable[VEmbResult]` | 📋 DONE | Explicit type |
| 525 | `vlinks` | `VLinksResult` | `Awaitable[VLinksResult]` | 📋 DONE | Explicit type |
| 526 | `vinfo` | `dict` | `Awaitable[dict]` | ✅ DONE | Standard dict |
| 527 | `vsetattr` | `int` | `Awaitable[int]` | ✅ DONE | Integer reply |
| 528 | `vgetattr` | `VGetAttrResult` | `Awaitable[VGetAttrResult]` | 📋 DONE | Explicit type |
| 529 | `vrandmember` | `VRandMemberResult` | `Awaitable[VRandMemberResult]` | 📋 DONE | Explicit type |
| 530 | `vrange` | `list[str]` | `Awaitable[list[str]]` | ✅ DONE | Standard list |

---

## Batch Summary

| Batch | File | Command Class | Methods | Count |
|-------|------|---------------|---------|-------|
| 1 | core.py | ACLCommands | 1-14 | 14 |
| 2 | core.py | ManagementCommands (Part 1) | 15-50 | 36 |
| 3 | core.py | ManagementCommands (Part 2) | 51-93 | 43 |
| 4 | core.py | BasicKeyCommands (Part 1) | 94-130 | 37 |
| 5 | core.py | BasicKeyCommands (Part 2) | 131-156 | 26 |
| 6 | core.py | ListCommands | 157-178 | 22 |
| 7 | core.py | ScanCommands + SetCommands | 179-202 | 24 |
| 8 | core.py | StreamCommands | 203-226 | 24 |
| 9 | core.py | SortedSetCommands | 227-260 | 34 |
| 10 | core.py | HyperlogCommands + HashCommands | 261-289 | 29 |
| 11 | core.py | PubSubCommands + ScriptCommands | 290-306 | 17 |
| 12 | core.py | GeoCommands + ModuleCommands + ClusterCommands + FunctionCommands | 307-335 | 29 |
| 13 | cluster.py | ClusterCommands | 336-378 | 43 |
| 14 | sentinel.py | SentinelCommands | 379-391 | 13 |
| 15 | search/commands.py | SearchCommands | 392-424 | 33 |
| 16 | json/commands.py | JSONCommands | 425-452 | 28 |
| 17 | timeseries/commands.py | TimeSeriesCommands | 453-469 | 17 |
| 18 | bf/commands.py | BFCommands + CFCommands | 470-491 | 22 |
| 19 | bf/commands.py | TOPKCommands + TDigestCommands + CMSCommands | 492-518 | 27 |
| 20 | vectorset/commands.py | VectorSetCommands | 519-530 | 12 ✅ DONE |

---

## Implementation Checklist

For each batch:
- [ ] Review all methods in the batch
- [ ] Skip methods marked with ⚠️ SKIP, 🔄 SKIP, ❌ SKIP
- [ ] For each ✅ method:
  - [ ] Check the Notes column for callback info
  - [ ] Verify return type against `redis/_parsers/helpers.py` callback dictionaries
  - [ ] Add two `@overload` signatures
  - [ ] Run type checker
- [ ] Test changes
- [ ] Mark batch complete

---

## Notes

1. **Callback System is Critical** - Always check the three-tier callback hierarchy in `redis/_parsers/helpers.py`:
   - `_RedisCallbacks` (base) - shared by RESP2 and RESP3
   - `_RedisCallbacksRESP2` - RESP2-specific overrides
   - `_RedisCallbacksRESP3` - RESP3-specific overrides
2. **Protocol-Specific Types** - When RESP2 and RESP3 have different callbacks, use the most permissive union type
3. **Import considerations** - Make sure `TYPE_CHECKING`, `overload`, and `Awaitable` are imported
4. **Self-type discrimination** - Use `self: SyncClientProtocol` and `self: AsyncClientProtocol`
5. **Keep original method** - Only add overloads, don't modify the actual implementation
6. **No callback = raw response** - If a command has no callback, return type depends on `decode_responses`
