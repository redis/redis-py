# Agent Overload Implementation Guide

## Purpose
This document provides instructions for implementing `@overload` signatures for redis-py command methods.

## How to Use This Guide

### Step 1: Select a Batch
Work on one batch at a time. Each batch contains methods grouped by command class.

### Step 2: For Each Method in the Batch
1. **Verify the return type** by checking Redis docs at https://redis.io/commands
2. **Add two `@overload` signatures** before the method:
   - One for sync (`self: Redis[...]`) returning the sync type
   - One for async (`self: AsyncRedis[...]`) returning `Awaitable[sync_type]`
3. **Keep the original implementation** unchanged
4. **Run type checker** to verify no errors

### Step 3: Mark Batch Complete
After implementing all methods in a batch, update status in inventory.

---

## Overload Pattern Template

```python
from typing import overload, Awaitable, TYPE_CHECKING

if TYPE_CHECKING:
    from redis import Redis
    from redis.asyncio import Redis as AsyncRedis

class SomeCommands:
    @overload
    def method(self: "Redis[bytes]", arg: str) -> SyncReturnType: ...
    @overload
    def method(self: "AsyncRedis[bytes]", arg: str) -> Awaitable[SyncReturnType]: ...
    def method(self, arg: str) -> ResponseT:
        # original implementation
        ...
```

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

## Batches

### BATCH 1: ACLCommands (core.py)
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 1 | `acl_cat` | `list[str]` | `Awaitable[list[str]]` | ✅ | 1 |
| 2 | `acl_dryrun` | `str` | `Awaitable[str]` | ✅ | 1 |
| 3 | `acl_deluser` | `int` | `Awaitable[int]` | ✅ | 1 |
| 4 | `acl_genpass` | `str` | `Awaitable[str]` | ✅ | 1 |
| 5 | `acl_getuser` | `dict` | `Awaitable[dict]` | ✅ | 1 |
| 6 | `acl_help` | `list[str]` | `Awaitable[list[str]]` | ✅ | 1 |
| 7 | `acl_list` | `list[str]` | `Awaitable[list[str]]` | ✅ | 1 |
| 8 | `acl_log` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | 1 |
| 9 | `acl_log_reset` | `bool` | `Awaitable[bool]` | ✅ | 1 |
| 10 | `acl_load` | `bool` | `Awaitable[bool]` | ✅ | 1 |
| 11 | `acl_save` | `bool` | `Awaitable[bool]` | ✅ | 1 |
| 12 | `acl_setuser` | `bool` | `Awaitable[bool]` | ✅ | 1 |
| 13 | `acl_users` | `list[str]` | `Awaitable[list[str]]` | ✅ | 1 |
| 14 | `acl_whoami` | `str` | `Awaitable[str]` | ✅ | 1 |

### BATCH 2: ManagementCommands Part 1 (core.py) - Methods 15-50
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 15 | `auth` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 16 | `bgrewriteaof` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 17 | `bgsave` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 18 | `role` | `list` | `Awaitable[list]` | ✅ | 2 |
| 19 | `client_kill` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 20 | `client_kill_filter` | `int` | `Awaitable[int]` | ✅ | 2 |
| 21 | `client_info` | `dict` | `Awaitable[dict]` | ✅ | 2 |
| 22 | `client_list` | `list[dict]` | `Awaitable[list[dict]]` | ✅ | 2 |
| 23 | `client_getname` | `str \| None` | `Awaitable[str \| None]` | ✅ | 2 |
| 24 | `client_getredir` | `int` | `Awaitable[int]` | ✅ | 2 |
| 25 | `client_reply` | `str` | `Awaitable[str]` | ✅ | 2 |
| 26 | `client_id` | `int` | `Awaitable[int]` | ✅ | 2 |
| 27 | `client_tracking_on` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 28 | `client_tracking_off` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 29 | `client_tracking` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 30 | `client_trackinginfo` | `dict` | `Awaitable[dict]` | ✅ | 2 |
| 31 | `client_setname` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 32 | `client_setinfo` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 33 | `client_unblock` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 34 | `client_pause` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 35 | `client_unpause` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 36 | `client_no_evict` | `str` | `Awaitable[str]` | 📋 | 2 |
| 37 | `client_no_touch` | `str` | `Awaitable[str]` | 📋 | 2 |
| 38 | `command` | `list` | `Awaitable[list]` | ✅ | 2 |
| 39 | `command_info` | `None` | `None` | ⚠️ SKIP | 2 |
| 40 | `command_count` | `int` | `Awaitable[int]` | ✅ | 2 |
| 41 | `command_list` | `list[str]` | `Awaitable[list[str]]` | ✅ | 2 |
| 42 | `command_getkeysandflags` | `List[Union[str, List[str]]]` | `Awaitable[...]` | 📋 | 2 |
| 43 | `command_docs` | `dict` | `Awaitable[dict]` | ✅ | 2 |
| 44 | `config_get` | `dict` | `Awaitable[dict]` | ✅ | 2 |
| 45 | `config_set` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 46 | `config_resetstat` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 47 | `config_rewrite` | `bool` | `Awaitable[bool]` | ✅ | 2 |
| 48 | `dbsize` | `int` | `Awaitable[int]` | ✅ | 2 |
| 49 | `debug_object` | `str` | `Awaitable[str]` | ✅ | 2 |
| 50 | `debug_segfault` | `None` | `None` | ⚠️ SKIP | 2 |

### BATCH 3: ManagementCommands Part 2 (core.py) - Methods 51-93
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 51 | `echo` | `bytes` | `Awaitable[bytes]` | ✅ | 3 |
| 52 | `flushall` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 53 | `flushdb` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 54 | `sync` | `bytes` | `Awaitable[bytes]` | ✅ | 3 |
| 55 | `psync` | `bytes` | `Awaitable[bytes]` | ✅ | 3 |
| 56 | `swapdb` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 57 | `select` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 58 | `info` | `dict` | `Awaitable[dict]` | ✅ | 3 |
| 59 | `lastsave` | `datetime` | `Awaitable[datetime]` | ✅ | 3 |
| 60 | `latency_doctor` | `str` | `Awaitable[str]` | ✅ | 3 |
| 61 | `latency_graph` | `str` | `Awaitable[str]` | ✅ | 3 |
| 62 | `lolwut` | `str` | `Awaitable[str]` | ✅ | 3 |
| 63 | `reset` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 64 | `migrate` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 65 | `object` | `Any` | `Awaitable[Any]` | ✅ | 3 |
| 66 | `memory_doctor` | `None` | `None` | ⚠️ SKIP | 3 |
| 67 | `memory_help` | `None` | `None` | ⚠️ SKIP | 3 |
| 68 | `memory_stats` | `dict` | `Awaitable[dict]` | ✅ | 3 |
| 69 | `memory_malloc_stats` | `str` | `Awaitable[str]` | ✅ | 3 |
| 70 | `memory_usage` | `int` | `Awaitable[int]` | ✅ | 3 |
| 71 | `memory_purge` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 72 | `latency_histogram` | `dict` | `Awaitable[dict]` | ✅ | 3 |
| 73 | `latency_history` | `list` | `Awaitable[list]` | ✅ | 3 |
| 74 | `latency_latest` | `list` | `Awaitable[list]` | ✅ | 3 |
| 75 | `latency_reset` | `int` | `Awaitable[int]` | ✅ | 3 |
| 76 | `ping` | `bool` | `Awaitable[bool]` | 📋 | 3 |
| 77 | `quit` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 78 | `replicaof` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 79 | `save` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 80 | `shutdown` | `None` | `None` | ⚠️ SKIP | 3 |
| 81 | `slaveof` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 82 | `slowlog_get` | `list` | `Awaitable[list]` | ✅ | 3 |
| 83 | `slowlog_len` | `int` | `Awaitable[int]` | ✅ | 3 |
| 84 | `slowlog_reset` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 85 | `time` | `list` | `Awaitable[list]` | ✅ | 3 |
| 86 | `wait` | `int` | `Awaitable[int]` | ✅ | 3 |
| 87 | `waitaof` | `list` | `Awaitable[list]` | ✅ | 3 |
| 88 | `hello` | `dict` | `Awaitable[dict]` | ✅ | 3 |
| 89 | `failover` | `bool` | `Awaitable[bool]` | ✅ | 3 |
| 90 | `hotkeys_start` | `str \| bytes` | `Awaitable[str \| bytes]` | 📋 | 3 |
| 91 | `hotkeys_stop` | `str \| bytes` | `Awaitable[str \| bytes]` | 📋 | 3 |
| 92 | `hotkeys_reset` | `str \| bytes` | `Awaitable[str \| bytes]` | 📋 | 3 |
| 93 | `hotkeys_get` | `list[dict]` | `Awaitable[list[dict]]` | 📋 | 3 |

### BATCH 4: BasicKeyCommands Part 1 (core.py) - Methods 94-130
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 94 | `append` | `int` | `Awaitable[int]` | ✅ | 4 |
| 95 | `bitcount` | `int` | `Awaitable[int]` | ✅ | 4 |
| 96 | `bitfield` | `BitFieldOperation` | `BitFieldOperation` | 📋 | 4 |
| 97 | `bitfield_ro` | `list[int]` | `Awaitable[list[int]]` | ✅ | 4 |
| 98 | `bitop` | `int` | `Awaitable[int]` | ✅ | 4 |
| 99 | `bitpos` | `int` | `Awaitable[int]` | ✅ | 4 |
| 100 | `copy` | `bool` | `Awaitable[bool]` | ✅ | 4 |
| 101 | `decrby` | `int` | `Awaitable[int]` | ✅ | 4 |
| 102 | `delete` | `int` | `Awaitable[int]` | ✅ | 4 |
| 103 | `__delitem__` | `None` | N/A | ❌ SKIP | 4 |
| 104 | `delex` | `int` | `Awaitable[int]` | ✅ | 4 |
| 105 | `dump` | `bytes` | `Awaitable[bytes]` | ✅ | 4 |
| 106 | `exists` | `int` | `Awaitable[int]` | ✅ | 4 |
| 107 | `expire` | `bool` | `Awaitable[bool]` | ✅ | 4 |
| 108 | `expireat` | `bool` | `Awaitable[bool]` | ✅ | 4 |
| 109 | `expiretime` | `int` | `Awaitable[int]` | 📋 | 4 |
| 110 | `digest_local` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 | 4 |
| 111 | `digest` | `str \| bytes \| None` | `Awaitable[str \| bytes \| None]` | ✅ | 4 |
| 112 | `get` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ DONE | 4 |
| 113 | `getdel` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ | 4 |
| 114 | `getex` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ | 4 |
| 115 | `__getitem__` | `bytes` | N/A | ❌ SKIP | 4 |
| 116 | `getbit` | `int` | `Awaitable[int]` | ✅ | 4 |
| 117 | `getrange` | `bytes` | `Awaitable[bytes]` | ✅ | 4 |
| 118 | `getset` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ | 4 |
| 119 | `incrby` | `int` | `Awaitable[int]` | ✅ | 4 |
| 120 | `incrbyfloat` | `float` | `Awaitable[float]` | ✅ | 4 |
| 121 | `keys` | `list[bytes]` | `Awaitable[list[bytes]]` | ✅ | 4 |
| 122 | `lmove` | `bytes` | `Awaitable[bytes]` | ✅ | 4 |
| 123 | `blmove` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ | 4 |
| 124 | `mget` | `list` | `Awaitable[list]` | ✅ | 4 |
| 125 | `mset` | `bool` | `Awaitable[bool]` | ✅ | 4 |
| 126 | `msetex` | `int` | `Awaitable[int]` | ✅ | 4 |
| 127 | `msetnx` | `bool` | `Awaitable[bool]` | ✅ | 4 |
| 128 | `move` | `bool` | `Awaitable[bool]` | ✅ | 4 |
| 129 | `persist` | `bool` | `Awaitable[bool]` | ✅ | 4 |
| 130 | `pexpire` | `bool` | `Awaitable[bool]` | ✅ | 4 |

### BATCH 5: BasicKeyCommands Part 2 (core.py) - Methods 131-156
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 131 | `pexpireat` | `bool` | `Awaitable[bool]` | ✅ | 5 |
| 132 | `pexpiretime` | `int` | `Awaitable[int]` | 📋 | 5 |
| 133 | `psetex` | `bool` | `Awaitable[bool]` | ✅ | 5 |
| 134 | `pttl` | `int` | `Awaitable[int]` | ✅ | 5 |
| 135 | `hrandfield` | `bytes \| list` | `Awaitable[bytes \| list]` | ✅ | 5 |
| 136 | `randomkey` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ | 5 |
| 137 | `rename` | `bool` | `Awaitable[bool]` | ✅ | 5 |
| 138 | `renamenx` | `bool` | `Awaitable[bool]` | ✅ | 5 |
| 139 | `restore` | `bool` | `Awaitable[bool]` | ✅ | 5 |
| 140 | `set` | `bool \| None` | `Awaitable[bool \| None]` | ✅ DONE | 5 |
| 141 | `__setitem__` | `None` | N/A | ❌ SKIP | 5 |
| 142 | `setbit` | `int` | `Awaitable[int]` | ✅ | 5 |
| 143 | `setex` | `bool` | `Awaitable[bool]` | ✅ | 5 |
| 144 | `setnx` | `bool` | `Awaitable[bool]` | ✅ | 5 |
| 145 | `setrange` | `int` | `Awaitable[int]` | ✅ | 5 |
| 146 | `stralgo` | `Any` | `Awaitable[Any]` | ✅ | 5 |
| 147 | `strlen` | `int` | `Awaitable[int]` | ✅ | 5 |
| 148 | `substr` | `bytes` | `Awaitable[bytes]` | ✅ | 5 |
| 149 | `touch` | `int` | `Awaitable[int]` | ✅ | 5 |
| 150 | `ttl` | `int` | `Awaitable[int]` | ✅ | 5 |
| 151 | `type` | `str` | `Awaitable[str]` | ✅ | 5 |
| 152 | `watch` | `bool` | `Awaitable[bool]` | ⚠️ SKIP | 5 |
| 153 | `unwatch` | `bool` | `Awaitable[bool]` | ⚠️ SKIP | 5 |
| 154 | `unlink` | `int` | `Awaitable[int]` | ✅ | 5 |
| 155 | `lcs` | `str \| int \| list` | `Awaitable[str \| int \| list]` | ✅ | 5 |
| 156 | `__contains__` | `bool` | N/A | ❌ SKIP | 5 |

### BATCH 6: ListCommands (core.py) - Methods 157-178
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 157 | `blpop` | `list` | `Awaitable[list]` | ✅ | 6 |
| 158 | `brpop` | `list` | `Awaitable[list]` | ✅ | 6 |
| 159 | `brpoplpush` | `str \| None` | `Awaitable[str \| None]` | ✅ | 6 |
| 160 | `blmpop` | `list \| None` | `Awaitable[list \| None]` | ✅ | 6 |
| 161 | `lmpop` | `list` | `Awaitable[list]` | ✅ | 6 |
| 162 | `lindex` | `str \| None` | `Awaitable[str \| None]` | ✅ | 6 |
| 163 | `linsert` | `int` | `Awaitable[int]` | ✅ | 6 |
| 164 | `llen` | `int` | `Awaitable[int]` | ✅ | 6 |
| 165 | `lpop` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ | 6 |
| 166 | `lpush` | `int` | `Awaitable[int]` | ✅ | 6 |
| 167 | `lpushx` | `int` | `Awaitable[int]` | ✅ | 6 |
| 168 | `lrange` | `list` | `Awaitable[list]` | ✅ | 6 |
| 169 | `lrem` | `int` | `Awaitable[int]` | ✅ | 6 |
| 170 | `lset` | `str` | `Awaitable[str]` | ✅ | 6 |
| 171 | `ltrim` | `str` | `Awaitable[str]` | ✅ | 6 |
| 172 | `rpop` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ | 6 |
| 173 | `rpoplpush` | `str` | `Awaitable[str]` | ✅ | 6 |
| 174 | `rpush` | `int` | `Awaitable[int]` | ✅ | 6 |
| 175 | `rpushx` | `int` | `Awaitable[int]` | ✅ | 6 |
| 176 | `lpos` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ | 6 |
| 177 | `sort` | `list \| int` | `Awaitable[list \| int]` | ✅ | 6 |
| 178 | `sort_ro` | `list` | `Awaitable[list]` | ✅ | 6 |

### BATCH 7: ScanCommands + SetCommands (core.py) - Methods 179-202
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 179 | `scan` | `tuple[int, list]` | `Awaitable[tuple[int, list]]` | ✅ | 7 |
| 180 | `scan_iter` | `Iterator` | `AsyncIterator` | 🔄 SKIP | 7 |
| 181 | `sscan` | `tuple[int, list]` | `Awaitable[tuple[int, list]]` | ✅ | 7 |
| 182 | `sscan_iter` | `Iterator` | `AsyncIterator` | 🔄 SKIP | 7 |
| 183 | `hscan` | `tuple[int, dict]` | `Awaitable[tuple[int, dict]]` | ✅ | 7 |
| 184 | `hscan_iter` | `Iterator` | `AsyncIterator` | 🔄 SKIP | 7 |
| 185 | `zscan` | `tuple[int, list]` | `Awaitable[tuple[int, list]]` | ✅ | 7 |
| 186 | `zscan_iter` | `Iterator` | `AsyncIterator` | 🔄 SKIP | 7 |
| 187 | `sadd` | `int` | `Awaitable[int]` | ✅ | 7 |
| 188 | `scard` | `int` | `Awaitable[int]` | ✅ | 7 |
| 189 | `sdiff` | `list` | `Awaitable[list]` | ✅ | 7 |
| 190 | `sdiffstore` | `int` | `Awaitable[int]` | ✅ | 7 |
| 191 | `sinter` | `list` | `Awaitable[list]` | ✅ | 7 |
| 192 | `sintercard` | `int` | `Awaitable[int]` | ✅ | 7 |
| 193 | `sinterstore` | `int` | `Awaitable[int]` | ✅ | 7 |
| 194 | `sismember` | `Literal[0, 1]` | `Awaitable[Literal[0, 1]]` | ✅ | 7 |
| 195 | `smembers` | `Set` | `Awaitable[Set]` | ✅ | 7 |
| 196 | `smismember` | `list[Literal[0, 1]]` | `Awaitable[list[Literal[0, 1]]]` | ✅ | 7 |
| 197 | `smove` | `bool` | `Awaitable[bool]` | ✅ | 7 |
| 198 | `spop` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ | 7 |
| 199 | `srandmember` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ | 7 |
| 200 | `srem` | `int` | `Awaitable[int]` | ✅ | 7 |
| 201 | `sunion` | `list` | `Awaitable[list]` | ✅ | 7 |
| 202 | `sunionstore` | `int` | `Awaitable[int]` | ✅ | 7 |

### BATCH 8: StreamCommands (core.py) - Methods 203-226
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 203 | `xack` | `int` | `Awaitable[int]` | ✅ | 8 |
| 204 | `xackdel` | `int` | `Awaitable[int]` | ✅ | 8 |
| 205 | `xadd` | `bytes` | `Awaitable[bytes]` | ✅ | 8 |
| 206 | `xcfgset` | `bool` | `Awaitable[bool]` | ✅ | 8 |
| 207 | `xautoclaim` | `tuple` | `Awaitable[tuple]` | ✅ | 8 |
| 208 | `xclaim` | `list` | `Awaitable[list]` | ✅ | 8 |
| 209 | `xdel` | `int` | `Awaitable[int]` | ✅ | 8 |
| 210 | `xdelex` | `int` | `Awaitable[int]` | ✅ | 8 |
| 211 | `xgroup_create` | `bool` | `Awaitable[bool]` | ✅ | 8 |
| 212 | `xgroup_delconsumer` | `int` | `Awaitable[int]` | ✅ | 8 |
| 213 | `xgroup_destroy` | `int` | `Awaitable[int]` | ✅ | 8 |
| 214 | `xgroup_createconsumer` | `int` | `Awaitable[int]` | ✅ | 8 |
| 215 | `xgroup_setid` | `bool` | `Awaitable[bool]` | ✅ | 8 |
| 216 | `xinfo_consumers` | `list` | `Awaitable[list]` | ✅ | 8 |
| 217 | `xinfo_groups` | `list` | `Awaitable[list]` | ✅ | 8 |
| 218 | `xinfo_stream` | `dict` | `Awaitable[dict]` | ✅ | 8 |
| 219 | `xlen` | `int` | `Awaitable[int]` | ✅ | 8 |
| 220 | `xpending` | `dict` | `Awaitable[dict]` | ✅ | 8 |
| 221 | `xpending_range` | `list` | `Awaitable[list]` | ✅ | 8 |
| 222 | `xrange` | `list` | `Awaitable[list]` | ✅ | 8 |
| 223 | `xread` | `list` | `Awaitable[list]` | ✅ | 8 |
| 224 | `xreadgroup` | `list` | `Awaitable[list]` | ✅ | 8 |
| 225 | `xrevrange` | `list` | `Awaitable[list]` | ✅ | 8 |
| 226 | `xtrim` | `int` | `Awaitable[int]` | ✅ | 8 |

### BATCH 9: SortedSetCommands Part 1 (core.py) - Methods 227-260
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 227 | `zadd` | `int \| float` | `Awaitable[int \| float]` | ✅ | 9 |
| 228 | `zcard` | `int` | `Awaitable[int]` | ✅ | 9 |
| 229 | `zcount` | `int` | `Awaitable[int]` | ✅ | 9 |
| 230 | `zdiff` | `list` | `Awaitable[list]` | ✅ | 9 |
| 231 | `zdiffstore` | `int` | `Awaitable[int]` | ✅ | 9 |
| 232 | `zincrby` | `float` | `Awaitable[float]` | ✅ | 9 |
| 233 | `zinter` | `list` | `Awaitable[list]` | ✅ | 9 |
| 234 | `zinterstore` | `int` | `Awaitable[int]` | ✅ | 9 |
| 235 | `zintercard` | `int` | `Awaitable[int]` | ✅ | 9 |
| 236 | `zlexcount` | `int` | `Awaitable[int]` | ✅ | 9 |
| 237 | `zpopmax` | `list` | `Awaitable[list]` | ✅ | 9 |
| 238 | `zpopmin` | `list` | `Awaitable[list]` | ✅ | 9 |
| 239 | `zrandmember` | `str \| list` | `Awaitable[str \| list]` | ✅ | 9 |
| 240 | `bzpopmax` | `list` | `Awaitable[list]` | ✅ | 9 |
| 241 | `bzpopmin` | `list` | `Awaitable[list]` | ✅ | 9 |
| 242 | `zmpop` | `list` | `Awaitable[list]` | ✅ | 9 |
| 243 | `bzmpop` | `list \| None` | `Awaitable[list \| None]` | ✅ | 9 |
| 244 | `zrange` | `list` | `Awaitable[list]` | ✅ | 9 |
| 245 | `zrevrange` | `list` | `Awaitable[list]` | ✅ | 9 |
| 246 | `zrangestore` | `int` | `Awaitable[int]` | ✅ | 9 |
| 247 | `zrangebylex` | `list` | `Awaitable[list]` | ✅ | 9 |
| 248 | `zrevrangebylex` | `list` | `Awaitable[list]` | ✅ | 9 |
| 249 | `zrangebyscore` | `list` | `Awaitable[list]` | ✅ | 9 |
| 250 | `zrevrangebyscore` | `list` | `Awaitable[list]` | ✅ | 9 |
| 251 | `zrank` | `int \| None` | `Awaitable[int \| None]` | ✅ | 9 |
| 252 | `zrem` | `int` | `Awaitable[int]` | ✅ | 9 |
| 253 | `zremrangebylex` | `int` | `Awaitable[int]` | ✅ | 9 |
| 254 | `zremrangebyrank` | `int` | `Awaitable[int]` | ✅ | 9 |
| 255 | `zremrangebyscore` | `int` | `Awaitable[int]` | ✅ | 9 |
| 256 | `zrevrank` | `int \| None` | `Awaitable[int \| None]` | ✅ | 9 |
| 257 | `zscore` | `float \| None` | `Awaitable[float \| None]` | ✅ | 9 |
| 258 | `zunion` | `list` | `Awaitable[list]` | ✅ | 9 |
| 259 | `zunionstore` | `int` | `Awaitable[int]` | ✅ | 9 |
| 260 | `zmscore` | `list` | `Awaitable[list]` | ✅ | 9 |

### BATCH 10: HyperlogCommands + HashCommands (core.py) - Methods 261-289
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 261 | `pfadd` | `int` | `Awaitable[int]` | ✅ | 10 |
| 262 | `pfcount` | `int` | `Awaitable[int]` | ✅ | 10 |
| 263 | `pfmerge` | `bool` | `Awaitable[bool]` | ✅ | 10 |
| 264 | `hdel` | `int` | `Awaitable[int]` | ✅ | 10 |
| 265 | `hexists` | `bool` | `Awaitable[bool]` | ✅ | 10 |
| 266 | `hget` | `str \| None` | `Awaitable[str \| None]` | ✅ | 10 |
| 267 | `hgetall` | `dict` | `Awaitable[dict]` | ✅ | 10 |
| 268 | `hgetdel` | `list \| None` | `Awaitable[list \| None]` | ✅ | 10 |
| 269 | `hgetex` | `list \| None` | `Awaitable[list \| None]` | ✅ | 10 |
| 270 | `hincrby` | `int` | `Awaitable[int]` | ✅ | 10 |
| 271 | `hincrbyfloat` | `float` | `Awaitable[float]` | ✅ | 10 |
| 272 | `hkeys` | `list` | `Awaitable[list]` | ✅ | 10 |
| 273 | `hlen` | `int` | `Awaitable[int]` | ✅ | 10 |
| 274 | `hset` | `int` | `Awaitable[int]` | ✅ | 10 |
| 275 | `hsetex` | `int` | `Awaitable[int]` | ✅ | 10 |
| 276 | `hsetnx` | `bool` | `Awaitable[bool]` | ✅ | 10 |
| 277 | `hmset` | `str` | `Awaitable[str]` | ✅ | 10 |
| 278 | `hmget` | `list` | `Awaitable[list]` | ✅ | 10 |
| 279 | `hvals` | `list` | `Awaitable[list]` | ✅ | 10 |
| 280 | `hstrlen` | `int` | `Awaitable[int]` | ✅ | 10 |
| 281 | `hexpire` | `list` | `Awaitable[list]` | ✅ | 10 |
| 282 | `hpexpire` | `list` | `Awaitable[list]` | ✅ | 10 |
| 283 | `hexpireat` | `list` | `Awaitable[list]` | ✅ | 10 |
| 284 | `hpexpireat` | `list` | `Awaitable[list]` | ✅ | 10 |
| 285 | `hpersist` | `list` | `Awaitable[list]` | ✅ | 10 |
| 286 | `hexpiretime` | `list` | `Awaitable[list]` | ✅ | 10 |
| 287 | `hpexpiretime` | `list` | `Awaitable[list]` | ✅ | 10 |
| 288 | `httl` | `list` | `Awaitable[list]` | ✅ | 10 |
| 289 | `hpttl` | `list` | `Awaitable[list]` | ✅ | 10 |

### BATCH 11: PubSubCommands + ScriptCommands (core.py) - Methods 290-306
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 290 | `publish` | `int` | `Awaitable[int]` | ✅ | 11 |
| 291 | `spublish` | `int` | `Awaitable[int]` | ✅ | 11 |
| 292 | `pubsub_channels` | `list` | `Awaitable[list]` | ✅ | 11 |
| 293 | `pubsub_shardchannels` | `list` | `Awaitable[list]` | ✅ | 11 |
| 294 | `pubsub_numpat` | `int` | `Awaitable[int]` | ✅ | 11 |
| 295 | `pubsub_numsub` | `list` | `Awaitable[list]` | ✅ | 11 |
| 296 | `pubsub_shardnumsub` | `list` | `Awaitable[list]` | ✅ | 11 |
| 297 | `eval` | `str` | `Awaitable[str]` | ✅ | 11 |
| 298 | `eval_ro` | `str` | `Awaitable[str]` | ✅ | 11 |
| 299 | `evalsha` | `str` | `Awaitable[str]` | ✅ | 11 |
| 300 | `evalsha_ro` | `str` | `Awaitable[str]` | ✅ | 11 |
| 301 | `script_exists` | `list[bool]` | `Awaitable[list[bool]]` | ✅ | 11 |
| 302 | `script_debug` | `None` | `None` | ⚠️ SKIP | 11 |
| 303 | `script_flush` | `bool` | `Awaitable[bool]` | ✅ | 11 |
| 304 | `script_kill` | `bool` | `Awaitable[bool]` | ✅ | 11 |
| 305 | `script_load` | `str` | `Awaitable[str]` | ✅ | 11 |
| 306 | `register_script` | `Script` | `AsyncScript` | ⚠️ SKIP | 11 |

### BATCH 12: GeoCommands + ModuleCommands + ClusterCommands + FunctionCommands (core.py) - Methods 307-335
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 307 | `geoadd` | `int` | `Awaitable[int]` | ✅ | 12 |
| 308 | `geodist` | `float \| None` | `Awaitable[float \| None]` | ✅ | 12 |
| 309 | `geohash` | `list` | `Awaitable[list]` | ✅ | 12 |
| 310 | `geopos` | `list` | `Awaitable[list]` | ✅ | 12 |
| 311 | `georadius` | `list` | `Awaitable[list]` | ✅ | 12 |
| 312 | `georadiusbymember` | `list` | `Awaitable[list]` | ✅ | 12 |
| 313 | `geosearch` | `list` | `Awaitable[list]` | ✅ | 12 |
| 314 | `geosearchstore` | `int` | `Awaitable[int]` | ✅ | 12 |
| 315 | `module_load` | `bool` | `Awaitable[bool]` | ✅ | 12 |
| 316 | `module_loadex` | `bool` | `Awaitable[bool]` | ✅ | 12 |
| 317 | `module_unload` | `bool` | `Awaitable[bool]` | ✅ | 12 |
| 318 | `module_list` | `list` | `Awaitable[list]` | ✅ | 12 |
| 319 | `command_info` | `None` | `None` | ⚠️ SKIP | 12 |
| 320 | `command_count` | `int` | `Awaitable[int]` | ✅ | 12 |
| 321 | `command_getkeys` | `list` | `Awaitable[list]` | ✅ | 12 |
| 322 | `command` | `list` | `Awaitable[list]` | ✅ | 12 |
| 323 | `cluster` | `Any` | `Awaitable[Any]` | ✅ | 12 |
| 324 | `readwrite` | `bool` | `Awaitable[bool]` | ✅ | 12 |
| 325 | `readonly` | `bool` | `Awaitable[bool]` | ✅ | 12 |
| 326 | `function_load` | `str` | `Awaitable[str]` | ✅ | 12 |
| 327 | `function_delete` | `str` | `Awaitable[str]` | ✅ | 12 |
| 328 | `function_flush` | `str` | `Awaitable[str]` | ✅ | 12 |
| 329 | `function_list` | `list` | `Awaitable[list]` | ✅ | 12 |
| 330 | `fcall` | `str` | `Awaitable[str]` | ✅ | 12 |
| 331 | `fcall_ro` | `str` | `Awaitable[str]` | ✅ | 12 |
| 332 | `function_dump` | `str` | `Awaitable[str]` | ✅ | 12 |
| 333 | `function_restore` | `str` | `Awaitable[str]` | ✅ | 12 |
| 334 | `function_kill` | `str` | `Awaitable[str]` | ✅ | 12 |
| 335 | `function_stats` | `list` | `Awaitable[list]` | ✅ | 12 |

### BATCH 13: ClusterCommands (cluster.py) - Methods 336-378
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 336 | `mget_nonatomic` | `list` | `Awaitable[list]` | ⚠️ SKIP | 13 |
| 337 | `mset_nonatomic` | `list[bool]` | `Awaitable[list[bool]]` | ⚠️ SKIP | 13 |
| 338 | `exists` | `int` | `Awaitable[int]` | 📋 | 13 |
| 339 | `delete` | `int` | `Awaitable[int]` | 📋 | 13 |
| 340 | `touch` | `int` | `Awaitable[int]` | 📋 | 13 |
| 341 | `unlink` | `int` | `Awaitable[int]` | 📋 | 13 |
| 342 | `slaveof` | `NoReturn` | `NoReturn` | 📋 SKIP | 13 |
| 343 | `replicaof` | `NoReturn` | `NoReturn` | 📋 SKIP | 13 |
| 344 | `swapdb` | `NoReturn` | `NoReturn` | 📋 SKIP | 13 |
| 345 | `cluster_myid` | `str` | `Awaitable[str]` | ✅ | 13 |
| 346 | `cluster_addslots` | `bool` | `Awaitable[bool]` | ✅ | 13 |
| 347 | `cluster_addslotsrange` | `bool` | `Awaitable[bool]` | ✅ | 13 |
| 348 | `cluster_countkeysinslot` | `int` | `Awaitable[int]` | ✅ | 13 |
| 349 | `cluster_count_failure_report` | `int` | `Awaitable[int]` | ✅ | 13 |
| 350 | `cluster_delslots` | `list[bool]` | `Awaitable[list[bool]]` | ⚠️ SKIP | 13 |
| 351 | `cluster_delslotsrange` | `bool` | `Awaitable[bool]` | ✅ | 13 |
| 352 | `cluster_failover` | `bool` | `Awaitable[bool]` | ✅ | 13 |
| 353 | `cluster_info` | `dict` | `Awaitable[dict]` | ✅ | 13 |
| 354 | `cluster_keyslot` | `int` | `Awaitable[int]` | ✅ | 13 |
| 355 | `cluster_meet` | `bool` | `Awaitable[bool]` | ✅ | 13 |
| 356 | `cluster_nodes` | `str` | `Awaitable[str]` | ✅ | 13 |
| 357 | `cluster_replicate` | `bool` | `Awaitable[bool]` | ✅ | 13 |
| 358 | `cluster_reset` | `bool` | `Awaitable[bool]` | ✅ | 13 |
| 359 | `cluster_save_config` | `bool` | `Awaitable[bool]` | ✅ | 13 |
| 360 | `cluster_get_keys_in_slot` | `list` | `Awaitable[list]` | ✅ | 13 |
| 361 | `cluster_set_config_epoch` | `bool` | `Awaitable[bool]` | ✅ | 13 |
| 362 | `cluster_setslot` | `bool` | `Awaitable[bool]` | ✅ | 13 |
| 363 | `cluster_setslot_stable` | `bool` | `Awaitable[bool]` | ✅ | 13 |
| 364 | `cluster_replicas` | `list` | `Awaitable[list]` | ✅ | 13 |
| 365 | `cluster_slots` | `list` | `Awaitable[list]` | ✅ | 13 |
| 366 | `cluster_shards` | `list` | `Awaitable[list]` | ✅ | 13 |
| 367 | `cluster_myshardid` | `str` | `Awaitable[str]` | ✅ | 13 |
| 368 | `cluster_links` | `list` | `Awaitable[list]` | ✅ | 13 |
| 369 | `cluster_flushslots` | `None` | `Awaitable[None]` | ✅ | 13 |
| 370 | `cluster_bumpepoch` | `None` | `Awaitable[None]` | ✅ | 13 |
| 371 | `client_tracking_on` | `bool` | `Awaitable[bool]` | ⚠️ SKIP | 13 |
| 372 | `client_tracking_off` | `bool` | `Awaitable[bool]` | ⚠️ SKIP | 13 |
| 373 | `hotkeys_start` | `str \| bytes` | `Awaitable[str \| bytes]` | ⚠️ SKIP | 13 |
| 374 | `hotkeys_stop` | `str \| bytes` | `Awaitable[str \| bytes]` | ⚠️ SKIP | 13 |
| 375 | `hotkeys_reset` | `str \| bytes` | `Awaitable[str \| bytes]` | ⚠️ SKIP | 13 |
| 376 | `hotkeys_get` | `list[dict]` | `Awaitable[list[dict]]` | ⚠️ SKIP | 13 |
| 377 | `stralgo` | `Any` | `Awaitable[Any]` | ✅ | 13 |
| 378 | `scan_iter` | `Iterator` | `AsyncIterator` | 🔄 SKIP | 13 |

### BATCH 14: SentinelCommands (sentinel.py) - Methods 379-391
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 379 | `sentinel` | `Any` | `Awaitable[Any]` | ⚠️ SKIP | 14 |
| 380 | `sentinel_get_master_addr_by_name` | `tuple` | `Awaitable[tuple]` | ✅ | 14 |
| 381 | `sentinel_master` | `dict` | `Awaitable[dict]` | ✅ | 14 |
| 382 | `sentinel_masters` | `dict` | `Awaitable[dict]` | ✅ | 14 |
| 383 | `sentinel_monitor` | `bool` | `Awaitable[bool]` | ✅ | 14 |
| 384 | `sentinel_remove` | `bool` | `Awaitable[bool]` | ✅ | 14 |
| 385 | `sentinel_sentinels` | `list` | `Awaitable[list]` | ✅ | 14 |
| 386 | `sentinel_set` | `bool` | `Awaitable[bool]` | ✅ | 14 |
| 387 | `sentinel_slaves` | `list` | `Awaitable[list]` | ✅ | 14 |
| 388 | `sentinel_reset` | `bool` | `Awaitable[bool]` | ✅ | 14 |
| 389 | `sentinel_failover` | `bool` | `Awaitable[bool]` | ✅ | 14 |
| 390 | `sentinel_ckquorum` | `bool` | `Awaitable[bool]` | ✅ | 14 |
| 391 | `sentinel_flushconfig` | `bool` | `Awaitable[bool]` | ✅ | 14 |

### BATCH 15: SearchCommands (search/commands.py) - Methods 392-424
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 392 | `batch_indexer` | `BatchIndexer` | `BatchIndexer` | 📋 SKIP | 15 |
| 393 | `create_index` | `bool` | `Awaitable[bool]` | ✅ | 15 |
| 394 | `alter_schema_add` | `bool` | `Awaitable[bool]` | ✅ | 15 |
| 395 | `dropindex` | `bool` | `Awaitable[bool]` | ✅ | 15 |
| 396 | `add_document` | `bool` | `Awaitable[bool]` | ✅ | 15 |
| 397 | `add_document_hash` | `bool` | `Awaitable[bool]` | ✅ | 15 |
| 398 | `delete_document` | `bool` | `Awaitable[bool]` | ✅ | 15 |
| 399 | `load_document` | `Document` | `Awaitable[Document]` | ⚠️ SKIP | 15 |
| 400 | `get` | `Document` | `Awaitable[Document]` | ✅ | 15 |
| 401 | `info` | `dict` | `Awaitable[dict]` | ⚠️ SKIP | 15 |
| 402 | `get_params_args` | `list` | `list` | 📋 SKIP | 15 |
| 403 | `search` | `Result` | `Awaitable[Result]` | ⚠️ SKIP | 15 |
| 404 | `hybrid_search` | `HybridResult` | `Awaitable[HybridResult]` | ⚠️ SKIP | 15 |
| 405 | `explain` | `str` | `Awaitable[str]` | ✅ | 15 |
| 406 | `explain_cli` | `list` | `Awaitable[list]` | ✅ | 15 |
| 407 | `aggregate` | `AggregateResult` | `Awaitable[AggregateResult]` | ⚠️ SKIP | 15 |
| 408 | `profile` | `tuple` | `Awaitable[tuple]` | ✅ | 15 |
| 409 | `spellcheck` | `dict` | `Awaitable[dict]` | ⚠️ SKIP | 15 |
| 410 | `dict_add` | `int` | `Awaitable[int]` | ✅ | 15 |
| 411 | `dict_del` | `int` | `Awaitable[int]` | ✅ | 15 |
| 412 | `dict_dump` | `list` | `Awaitable[list]` | ✅ | 15 |
| 413 | `config_set` | `bool` | `Awaitable[bool]` | ⚠️ SKIP | 15 |
| 414 | `config_get` | `str` | `Awaitable[str]` | ⚠️ SKIP | 15 |
| 415 | `tagvals` | `list` | `Awaitable[list]` | ✅ | 15 |
| 416 | `aliasadd` | `bool` | `Awaitable[bool]` | ✅ | 15 |
| 417 | `aliasupdate` | `bool` | `Awaitable[bool]` | ✅ | 15 |
| 418 | `aliasdel` | `bool` | `Awaitable[bool]` | ✅ | 15 |
| 419 | `sugadd` | `int` | `Awaitable[int]` | ⚠️ SKIP | 15 |
| 420 | `suglen` | `int` | `Awaitable[int]` | ✅ | 15 |
| 421 | `sugdel` | `int` | `Awaitable[int]` | ✅ | 15 |
| 422 | `sugget` | `list` | `Awaitable[list]` | ⚠️ SKIP | 15 |
| 423 | `synupdate` | `bool` | `Awaitable[bool]` | ✅ | 15 |
| 424 | `syndump` | `dict` | `Awaitable[dict]` | ✅ | 15 |

### BATCH 16: JSONCommands (json/commands.py) - Methods 425-452
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 425 | `arrappend` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | 16 |
| 426 | `arrindex` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | 16 |
| 427 | `arrinsert` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | 16 |
| 428 | `arrlen` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | 16 |
| 429 | `arrpop` | `list[str \| None]` | `Awaitable[list[str \| None]]` | ✅ | 16 |
| 430 | `arrtrim` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | 16 |
| 431 | `type` | `list[str]` | `Awaitable[list[str]]` | ✅ | 16 |
| 432 | `resp` | `list` | `Awaitable[list]` | ✅ | 16 |
| 433 | `objkeys` | `list` | `Awaitable[list]` | ✅ | 16 |
| 434 | `objlen` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | 16 |
| 435 | `numincrby` | `str` | `Awaitable[str]` | ✅ | 16 |
| 436 | `nummultby` | `str` | `Awaitable[str]` | ✅ | 16 |
| 437 | `clear` | `int` | `Awaitable[int]` | ✅ | 16 |
| 438 | `delete` | `int` | `Awaitable[int]` | ✅ | 16 |
| 439 | `get` | `list \| None` | `Awaitable[list \| None]` | ✅ | 16 |
| 440 | `mget` | `list` | `Awaitable[list]` | ✅ | 16 |
| 441 | `set` | `str \| None` | `Awaitable[str \| None]` | ✅ | 16 |
| 442 | `mset` | `str \| None` | `Awaitable[str \| None]` | ✅ | 16 |
| 443 | `merge` | `str \| None` | `Awaitable[str \| None]` | ✅ | 16 |
| 444 | `set_file` | `str \| None` | `Awaitable[str \| None]` | 📋 | 16 |
| 445 | `set_path` | `dict[str, bool]` | `Awaitable[dict[str, bool]]` | 📋 | 16 |
| 446 | `strlen` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ | 16 |
| 447 | `toggle` | `bool \| list` | `Awaitable[bool \| list]` | ✅ | 16 |
| 448 | `strappend` | `int \| list` | `Awaitable[int \| list]` | ✅ | 16 |
| 449 | `debug` | `int \| list[str]` | `Awaitable[int \| list[str]]` | ✅ | 16 |
| 450 | `jsonget` | `Any` | `Awaitable[Any]` | ✅ | 16 |
| 451 | `jsonmget` | `Any` | `Awaitable[Any]` | ✅ | 16 |
| 452 | `jsonset` | `Any` | `Awaitable[Any]` | ✅ | 16 |

### BATCH 17: TimeSeriesCommands (timeseries/commands.py) - Methods 453-469
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 453 | `create` | `bool` | `Awaitable[bool]` | ✅ | 17 |
| 454 | `alter` | `bool` | `Awaitable[bool]` | ✅ | 17 |
| 455 | `add` | `int` | `Awaitable[int]` | ✅ | 17 |
| 456 | `madd` | `list[int]` | `Awaitable[list[int]]` | ✅ | 17 |
| 457 | `incrby` | `int` | `Awaitable[int]` | ✅ | 17 |
| 458 | `decrby` | `int` | `Awaitable[int]` | ✅ | 17 |
| 459 | `delete` | `int` | `Awaitable[int]` | ✅ | 17 |
| 460 | `createrule` | `bool` | `Awaitable[bool]` | ✅ | 17 |
| 461 | `deleterule` | `bool` | `Awaitable[bool]` | ✅ | 17 |
| 462 | `range` | `list` | `Awaitable[list]` | ✅ | 17 |
| 463 | `revrange` | `list` | `Awaitable[list]` | ✅ | 17 |
| 464 | `mrange` | `list` | `Awaitable[list]` | ✅ | 17 |
| 465 | `mrevrange` | `list` | `Awaitable[list]` | ✅ | 17 |
| 466 | `get` | `tuple` | `Awaitable[tuple]` | ✅ | 17 |
| 467 | `mget` | `list` | `Awaitable[list]` | ✅ | 17 |
| 468 | `info` | `TSInfo` | `Awaitable[TSInfo]` | 📋 | 17 |
| 469 | `queryindex` | `list` | `Awaitable[list]` | ✅ | 17 |

### BATCH 18: BloomFilter Commands (bf/commands.py) - Methods 470-491
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 470 | `create` (BF) | `bool` | `Awaitable[bool]` | ✅ | 18 |
| 471 | `add` (BF) | `bool` | `Awaitable[bool]` | ✅ | 18 |
| 472 | `madd` (BF) | `list[bool]` | `Awaitable[list[bool]]` | ✅ | 18 |
| 473 | `insert` (BF) | `list[bool]` | `Awaitable[list[bool]]` | ✅ | 18 |
| 474 | `exists` (BF) | `bool` | `Awaitable[bool]` | ✅ | 18 |
| 475 | `mexists` (BF) | `list[bool]` | `Awaitable[list[bool]]` | ✅ | 18 |
| 476 | `scandump` (BF) | `tuple` | `Awaitable[tuple]` | ✅ | 18 |
| 477 | `loadchunk` (BF) | `bool` | `Awaitable[bool]` | ✅ | 18 |
| 478 | `info` (BF) | `dict` | `Awaitable[dict]` | ✅ | 18 |
| 479 | `card` (BF) | `int` | `Awaitable[int]` | ✅ | 18 |
| 480 | `create` (CF) | `bool` | `Awaitable[bool]` | ✅ | 18 |
| 481 | `add` (CF) | `bool` | `Awaitable[bool]` | ✅ | 18 |
| 482 | `addnx` (CF) | `bool` | `Awaitable[bool]` | ✅ | 18 |
| 483 | `insert` (CF) | `list[bool]` | `Awaitable[list[bool]]` | ✅ | 18 |
| 484 | `insertnx` (CF) | `list[bool]` | `Awaitable[list[bool]]` | ✅ | 18 |
| 485 | `exists` (CF) | `bool` | `Awaitable[bool]` | ✅ | 18 |
| 486 | `mexists` (CF) | `list[bool]` | `Awaitable[list[bool]]` | ✅ | 18 |
| 487 | `delete` (CF) | `bool` | `Awaitable[bool]` | ✅ | 18 |
| 488 | `count` (CF) | `int` | `Awaitable[int]` | ✅ | 18 |
| 489 | `scandump` (CF) | `tuple` | `Awaitable[tuple]` | ✅ | 18 |
| 490 | `loadchunk` (CF) | `bool` | `Awaitable[bool]` | ✅ | 18 |
| 491 | `info` (CF) | `dict` | `Awaitable[dict]` | ✅ | 18 |

### BATCH 19: TOPKCommands + TDigestCommands + CMSCommands (bf/commands.py) - Methods 492-518
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 492 | `reserve` (TOPK) | `bool` | `Awaitable[bool]` | ✅ | 19 |
| 493 | `add` (TOPK) | `list` | `Awaitable[list]` | ✅ | 19 |
| 494 | `incrby` (TOPK) | `list` | `Awaitable[list]` | ✅ | 19 |
| 495 | `query` (TOPK) | `list[bool]` | `Awaitable[list[bool]]` | ✅ | 19 |
| 496 | `count` (TOPK) | `list[int]` | `Awaitable[list[int]]` | ✅ | 19 |
| 497 | `list` (TOPK) | `list` | `Awaitable[list]` | ✅ | 19 |
| 498 | `info` (TOPK) | `dict` | `Awaitable[dict]` | ✅ | 19 |
| 499 | `create` (TD) | `bool` | `Awaitable[bool]` | ✅ | 19 |
| 500 | `reset` (TD) | `bool` | `Awaitable[bool]` | ✅ | 19 |
| 501 | `add` (TD) | `bool` | `Awaitable[bool]` | ✅ | 19 |
| 502 | `merge` (TD) | `bool` | `Awaitable[bool]` | ✅ | 19 |
| 503 | `min` (TD) | `float` | `Awaitable[float]` | ✅ | 19 |
| 504 | `max` (TD) | `float` | `Awaitable[float]` | ✅ | 19 |
| 505 | `quantile` (TD) | `list[float]` | `Awaitable[list[float]]` | ✅ | 19 |
| 506 | `cdf` (TD) | `list[float]` | `Awaitable[list[float]]` | ✅ | 19 |
| 507 | `info` (TD) | `dict` | `Awaitable[dict]` | ✅ | 19 |
| 508 | `trimmed_mean` (TD) | `float` | `Awaitable[float]` | ✅ | 19 |
| 509 | `rank` (TD) | `list[int]` | `Awaitable[list[int]]` | ✅ | 19 |
| 510 | `revrank` (TD) | `list[int]` | `Awaitable[list[int]]` | ✅ | 19 |
| 511 | `byrank` (TD) | `list[float]` | `Awaitable[list[float]]` | ✅ | 19 |
| 512 | `byrevrank` (TD) | `list[float]` | `Awaitable[list[float]]` | ✅ | 19 |
| 513 | `initbydim` (CMS) | `bool` | `Awaitable[bool]` | ✅ | 19 |
| 514 | `initbyprob` (CMS) | `bool` | `Awaitable[bool]` | ✅ | 19 |
| 515 | `incrby` (CMS) | `list[int]` | `Awaitable[list[int]]` | ✅ | 19 |
| 516 | `query` (CMS) | `list[int]` | `Awaitable[list[int]]` | ✅ | 19 |
| 517 | `merge` (CMS) | `bool` | `Awaitable[bool]` | ✅ | 19 |
| 518 | `info` (CMS) | `dict` | `Awaitable[dict]` | ✅ | 19 |

### BATCH 20: VectorSetCommands (vectorset/commands.py) - Methods 519-530 ✅ DONE
| # | Method | Sync Type | Async Type | Status | Batch |
|---|--------|-----------|------------|--------|-------|
| 519 | `vadd` | `int` | `Awaitable[int]` | ✅ DONE | 20 |
| 520 | `vsim` | `VSimResult` | `Awaitable[VSimResult]` | 📋 DONE | 20 |
| 521 | `vdim` | `int` | `Awaitable[int]` | ✅ DONE | 20 |
| 522 | `vcard` | `int` | `Awaitable[int]` | ✅ DONE | 20 |
| 523 | `vrem` | `int` | `Awaitable[int]` | ✅ DONE | 20 |
| 524 | `vemb` | `VEmbResult` | `Awaitable[VEmbResult]` | 📋 DONE | 20 |
| 525 | `vlinks` | `VLinksResult` | `Awaitable[VLinksResult]` | 📋 DONE | 20 |
| 526 | `vinfo` | `dict` | `Awaitable[dict]` | ✅ DONE | 20 |
| 527 | `vsetattr` | `int` | `Awaitable[int]` | ✅ DONE | 20 |
| 528 | `vgetattr` | `VGetAttrResult` | `Awaitable[VGetAttrResult]` | 📋 DONE | 20 |
| 529 | `vrandmember` | `VRandMemberResult` | `Awaitable[VRandMemberResult]` | 📋 DONE | 20 |
| 530 | `vrange` | `list[str]` | `Awaitable[list[str]]` | ✅ DONE | 20 |

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
  - [ ] Verify return type against Redis docs
  - [ ] Add two `@overload` signatures
  - [ ] Run type checker
- [ ] Test changes
- [ ] Mark batch complete

---

## Notes

1. **Type verification is critical** - The types listed are inferred and may need adjustment
2. **Import considerations** - Make sure `TYPE_CHECKING`, `overload`, and `Awaitable` are imported
3. **Self-type discrimination** - Use `self: "Redis[bytes]"` and `self: "AsyncRedis[bytes]"` patterns
4. **Keep original method** - Only add overloads, don't modify the actual implementation

