# Redis-py Commands Overload Inventory

This document catalogs all command methods that need `@overload` signatures for type-safe sync/async support.

## Legend

- **Defined Return**: The actual return type annotation in the current code
- **Assumed Sync/Async Types**: Inferred types based on Redis documentation - **⚠️ REQUIRES MANUAL VERIFICATION**
- **Status**:
  - ✅ **Standard** - Can use overload pattern directly
  - ⚠️ **Separate Async** - Has separate async implementation (needs review)
  - 🔄 **Iterator** - Returns iterator/async iterator (cannot use simple overload)
  - ❌ **Dunder** - Dunder method that cannot be async (raises TypeError in async)
  - 📋 **Explicit** - Return type already explicitly defined (not ResponseT)

---

## Summary Statistics

| Category | Count |
|----------|-------|
| **Total Methods** | **530** |
| ✅ Standard (can use overload) | ~495 |
| ⚠️ Separate Async Implementation | ~25 |
| 🔄 Iterator Methods | 5 |
| ❌ Dunder Methods (async N/A) | 4 |

---

## Core Commands (`redis/commands/core.py`)

### ACLCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 1 | `acl_cat` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification |
| 2 | `acl_dryrun` | *(none)* | `str` | `Awaitable[str]` | ✅ Needs verification |
| 3 | `acl_deluser` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 4 | `acl_genpass` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 5 | `acl_getuser` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 6 | `acl_help` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification |
| 7 | `acl_list` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification |
| 8 | `acl_log` | `ResponseT` | `list[dict]` | `Awaitable[list[dict]]` | ✅ Needs verification |
| 9 | `acl_log_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 10 | `acl_load` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 11 | `acl_save` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 12 | `acl_setuser` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 13 | `acl_users` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification |
| 14 | `acl_whoami` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |

### ManagementCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 15 | `auth` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 16 | `bgrewriteaof` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 17 | `bgsave` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 18 | `role` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 19 | `client_kill` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 20 | `client_kill_filter` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 21 | `client_info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 22 | `client_list` | `ResponseT` | `list[dict]` | `Awaitable[list[dict]]` | ✅ Needs verification |
| 23 | `client_getname` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification |
| 24 | `client_getredir` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 25 | `client_reply` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 26 | `client_id` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 27 | `client_tracking_on` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 28 | `client_tracking_off` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 29 | `client_tracking` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 30 | `client_trackinginfo` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 31 | `client_setname` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 32 | `client_setinfo` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 33 | `client_unblock` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 34 | `client_pause` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 35 | `client_unpause` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 36 | `client_no_evict` | `Union[Awaitable[str], str]` | `str` | `Awaitable[str]` | 📋 Explicit |
| 37 | `client_no_touch` | `Union[Awaitable[str], str]` | `str` | `Awaitable[str]` | 📋 Explicit |
| 38 | `command` | *(none)* | `list` | `Awaitable[list]` | ✅ Needs verification |
| 39 | `command_info` | `None` | `None` | `None` | ⚠️ Separate Async |
| 40 | `command_count` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 41 | `command_list` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification |
| 42 | `command_getkeysandflags` | `List[Union[str, List[str]]]` | `List[Union[str, List[str]]]` | `Awaitable[List[Union[str, List[str]]]]` | 📋 Explicit |
| 43 | `command_docs` | *(none)* | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 44 | `config_get` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 45 | `config_set` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 46 | `config_resetstat` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 47 | `config_rewrite` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 48 | `dbsize` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 49 | `debug_object` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 50 | `debug_segfault` | `None` | `None` | `None` | ⚠️ Separate Async |
| 51 | `echo` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification |
| 52 | `flushall` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 53 | `flushdb` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 54 | `sync` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification |
| 55 | `psync` | *(none)* | `bytes` | `Awaitable[bytes]` | ✅ Needs verification |
| 56 | `swapdb` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 57 | `select` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 58 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 59 | `lastsave` | `ResponseT` | `datetime` | `Awaitable[datetime]` | ✅ Needs verification |
| 60 | `latency_doctor` | *(none)* | `str` | `Awaitable[str]` | ✅ Needs verification |
| 61 | `latency_graph` | *(none)* | `str` | `Awaitable[str]` | ✅ Needs verification |
| 62 | `lolwut` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 63 | `reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 64 | `migrate` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 65 | `object` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ Needs verification |
| 66 | `memory_doctor` | `None` | `None` | `None` | ⚠️ Separate Async |
| 67 | `memory_help` | `None` | `None` | `None` | ⚠️ Separate Async |
| 68 | `memory_stats` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 69 | `memory_malloc_stats` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 70 | `memory_usage` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 71 | `memory_purge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 72 | `latency_histogram` | *(none)* | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 73 | `latency_history` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 74 | `latency_latest` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 75 | `latency_reset` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 76 | `ping` | `Union[Awaitable[bool], bool]` | `bool` | `Awaitable[bool]` | 📋 Explicit |
| 77 | `quit` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 78 | `replicaof` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 79 | `save` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 80 | `shutdown` | `None` | `None` | `None` | ⚠️ Separate Async |
| 81 | `slaveof` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 82 | `slowlog_get` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 83 | `slowlog_len` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 84 | `slowlog_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 85 | `time` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 86 | `wait` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 87 | `waitaof` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 88 | `hello` | *(none)* | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 89 | `failover` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 90 | `hotkeys_start` | `Union[Awaitable[Union[str, bytes]], Union[str, bytes]]` | `str \| bytes` | `Awaitable[str \| bytes]` | 📋 Explicit |
| 91 | `hotkeys_stop` | `Union[Awaitable[Union[str, bytes]], Union[str, bytes]]` | `str \| bytes` | `Awaitable[str \| bytes]` | 📋 Explicit |
| 92 | `hotkeys_reset` | `Union[Awaitable[Union[str, bytes]], Union[str, bytes]]` | `str \| bytes` | `Awaitable[str \| bytes]` | 📋 Explicit |
| 93 | `hotkeys_get` | `Union[Awaitable[list[dict[...]]], list[dict[...]]]` | `list[dict]` | `Awaitable[list[dict]]` | 📋 Explicit |

### BasicKeyCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 94 | `append` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 95 | `bitcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 96 | `bitfield` | `BitFieldOperation` | `BitFieldOperation` | `BitFieldOperation` | 📋 Explicit |
| 97 | `bitfield_ro` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification |
| 98 | `bitop` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 99 | `bitpos` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 100 | `copy` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 101 | `decrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 102 | `delete` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 103 | `__delitem__` | `None` | `None` | N/A | ❌ Dunder - raises TypeError in async |
| 104 | `delex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 105 | `dump` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification |
| 106 | `exists` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 107 | `expire` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 108 | `expireat` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 109 | `expiretime` | `int` | `int` | `Awaitable[int]` | 📋 Explicit |
| 110 | `digest_local` | `bytes \| str` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 Explicit |
| 111 | `digest` | `ResponseT` | `str \| bytes \| None` | `Awaitable[str \| bytes \| None]` | ✅ Needs verification |
| 112 | `get` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification |
| 113 | `getdel` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification |
| 114 | `getex` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification |
| 115 | `__getitem__` | `bytes` | `bytes` | N/A | ❌ Dunder - raises TypeError in async |
| 116 | `getbit` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 117 | `getrange` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification |
| 118 | `getset` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification |
| 119 | `incrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 120 | `incrbyfloat` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification |
| 121 | `keys` | `ResponseT` | `list[bytes]` | `Awaitable[list[bytes]]` | ✅ Needs verification |
| 122 | `lmove` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification |
| 123 | `blmove` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification |
| 124 | `mget` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 125 | `mset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 126 | `msetex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 127 | `msetnx` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 128 | `move` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 129 | `persist` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 130 | `pexpire` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 131 | `pexpireat` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 132 | `pexpiretime` | `int` | `int` | `Awaitable[int]` | 📋 Explicit |
| 133 | `psetex` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 134 | `pttl` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 135 | `hrandfield` | `ResponseT` | `bytes \| list` | `Awaitable[bytes \| list]` | ✅ Needs verification |
| 136 | `randomkey` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification |
| 137 | `rename` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 138 | `renamenx` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 139 | `restore` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 140 | `set` | `ResponseT` | `bool \| None` | `Awaitable[bool \| None]` | ✅ Needs verification |
| 141 | `__setitem__` | `None` | `None` | N/A | ❌ Dunder - raises TypeError in async |
| 142 | `setbit` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 143 | `setex` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 144 | `setnx` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 145 | `setrange` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 146 | `stralgo` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ Needs verification |
| 147 | `strlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 148 | `substr` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification |
| 149 | `touch` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 150 | `ttl` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 151 | `type` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 152 | `watch` | `bool` | `bool` | `Awaitable[bool]` | ⚠️ Separate Async |
| 153 | `unwatch` | `bool` | `bool` | `Awaitable[bool]` | ⚠️ Separate Async |
| 154 | `unlink` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 155 | `lcs` | `ResponseT` | `str \| int \| list` | `Awaitable[str \| int \| list]` | ✅ Needs verification |
| 156 | `__contains__` | `bool` | `bool` | N/A | ❌ Dunder - raises TypeError in async |

### ListCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 157 | `blpop` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 158 | `brpop` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 159 | `brpoplpush` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification |
| 160 | `blmpop` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Needs verification |
| 161 | `lmpop` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 162 | `lindex` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification |
| 163 | `linsert` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 164 | `llen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 165 | `lpop` | `ResponseT` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ Needs verification |
| 166 | `lpush` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 167 | `lpushx` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 168 | `lrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 169 | `lrem` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 170 | `lset` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 171 | `ltrim` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 172 | `rpop` | `ResponseT` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ Needs verification |
| 173 | `rpoplpush` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 174 | `rpush` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 175 | `rpushx` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 176 | `lpos` | `ResponseT` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ Needs verification |
| 177 | `sort` | `ResponseT` | `list \| int` | `Awaitable[list \| int]` | ✅ Needs verification |
| 178 | `sort_ro` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |

### ScanCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 179 | `scan` | `ResponseT` | `tuple[int, list]` | `Awaitable[tuple[int, list]]` | ✅ Needs verification |
| 180 | `scan_iter` | `Iterator` | `Iterator` | `AsyncIterator` | 🔄 Iterator - separate impl |
| 181 | `sscan` | `ResponseT` | `tuple[int, list]` | `Awaitable[tuple[int, list]]` | ✅ Needs verification |
| 182 | `sscan_iter` | `Iterator` | `Iterator` | `AsyncIterator` | 🔄 Iterator - separate impl |
| 183 | `hscan` | `ResponseT` | `tuple[int, dict]` | `Awaitable[tuple[int, dict]]` | ✅ Needs verification |
| 184 | `hscan_iter` | `Iterator` | `Iterator` | `AsyncIterator` | 🔄 Iterator - separate impl |
| 185 | `zscan` | `ResponseT` | `tuple[int, list]` | `Awaitable[tuple[int, list]]` | ✅ Needs verification |
| 186 | `zscan_iter` | `Iterator` | `Iterator` | `AsyncIterator` | 🔄 Iterator - separate impl |

### SetCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 187 | `sadd` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 188 | `scard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 189 | `sdiff` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 190 | `sdiffstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 191 | `sinter` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 192 | `sintercard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 193 | `sinterstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 194 | `sismember` | `ResponseT` | `Literal[0, 1]` | `Awaitable[Literal[0, 1]]` | ✅ Needs verification |
| 195 | `smembers` | `ResponseT` | `Set` | `Awaitable[Set]` | ✅ Needs verification |
| 196 | `smismember` | `ResponseT` | `list[Literal[0, 1]]` | `Awaitable[list[Literal[0, 1]]]` | ✅ Needs verification |
| 197 | `smove` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 198 | `spop` | `ResponseT` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ Needs verification |
| 199 | `srandmember` | `ResponseT` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ Needs verification |
| 200 | `srem` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 201 | `sunion` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 202 | `sunionstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |

### StreamCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 203 | `xack` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 204 | `xackdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 205 | `xadd` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification |
| 206 | `xcfgset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 207 | `xautoclaim` | `ResponseT` | `tuple` | `Awaitable[tuple]` | ✅ Needs verification |
| 208 | `xclaim` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 209 | `xdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 210 | `xdelex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 211 | `xgroup_create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 212 | `xgroup_delconsumer` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 213 | `xgroup_destroy` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 214 | `xgroup_createconsumer` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 215 | `xgroup_setid` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 216 | `xinfo_consumers` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 217 | `xinfo_groups` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 218 | `xinfo_stream` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 219 | `xlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 220 | `xpending` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 221 | `xpending_range` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 222 | `xrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 223 | `xread` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 224 | `xreadgroup` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 225 | `xrevrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 226 | `xtrim` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |

### SortedSetCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 227 | `zadd` | `ResponseT` | `int \| float` | `Awaitable[int \| float]` | ✅ Needs verification |
| 228 | `zcard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 229 | `zcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 230 | `zdiff` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 231 | `zdiffstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 232 | `zincrby` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification |
| 233 | `zinter` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 234 | `zinterstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 235 | `zintercard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 236 | `zlexcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 237 | `zpopmax` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 238 | `zpopmin` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 239 | `zrandmember` | `ResponseT` | `str \| list` | `Awaitable[str \| list]` | ✅ Needs verification |
| 240 | `bzpopmax` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 241 | `bzpopmin` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 242 | `zmpop` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 243 | `bzmpop` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Needs verification |
| 244 | `zrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 245 | `zrevrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 246 | `zrangestore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 247 | `zrangebylex` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 248 | `zrevrangebylex` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 249 | `zrangebyscore` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 250 | `zrevrangebyscore` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 251 | `zrank` | `ResponseT` | `int \| None` | `Awaitable[int \| None]` | ✅ Needs verification |
| 252 | `zrem` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 253 | `zremrangebylex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 254 | `zremrangebyrank` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 255 | `zremrangebyscore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 256 | `zrevrank` | `ResponseT` | `int \| None` | `Awaitable[int \| None]` | ✅ Needs verification |
| 257 | `zscore` | `ResponseT` | `float \| None` | `Awaitable[float \| None]` | ✅ Needs verification |
| 258 | `zunion` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 259 | `zunionstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 260 | `zmscore` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |

### HyperlogCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 261 | `pfadd` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 262 | `pfcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 263 | `pfmerge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |

### HashCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 264 | `hdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 265 | `hexists` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 266 | `hget` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification |
| 267 | `hgetall` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 268 | `hgetdel` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Needs verification |
| 269 | `hgetex` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Needs verification |
| 270 | `hincrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 271 | `hincrbyfloat` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification |
| 272 | `hkeys` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 273 | `hlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 274 | `hset` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 275 | `hsetex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 276 | `hsetnx` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 277 | `hmset` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 278 | `hmget` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 279 | `hvals` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 280 | `hstrlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 281 | `hexpire` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 282 | `hpexpire` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 283 | `hexpireat` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 284 | `hpexpireat` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 285 | `hpersist` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 286 | `hexpiretime` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 287 | `hpexpiretime` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 288 | `httl` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 289 | `hpttl` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |

### PubSubCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 290 | `publish` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 291 | `spublish` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 292 | `pubsub_channels` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 293 | `pubsub_shardchannels` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 294 | `pubsub_numpat` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 295 | `pubsub_numsub` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 296 | `pubsub_shardnumsub` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |

### ScriptCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 297 | `eval` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 298 | `eval_ro` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 299 | `evalsha` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 300 | `evalsha_ro` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 301 | `script_exists` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification |
| 302 | `script_debug` | `None` | `None` | `None` | ⚠️ Separate Async |
| 303 | `script_flush` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 304 | `script_kill` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 305 | `script_load` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 306 | `register_script` | `Script` | `Script` | `AsyncScript` | ⚠️ Returns different class |

### GeoCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 307 | `geoadd` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 308 | `geodist` | `ResponseT` | `float \| None` | `Awaitable[float \| None]` | ✅ Needs verification |
| 309 | `geohash` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 310 | `geopos` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 311 | `georadius` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 312 | `georadiusbymember` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 313 | `geosearch` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 314 | `geosearchstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |

### ModuleCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 315 | `module_load` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 316 | `module_loadex` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 317 | `module_unload` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 318 | `module_list` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 319 | `command_info` | `None` | `None` | `None` | ⚠️ Separate Async |
| 320 | `command_count` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 321 | `command_getkeys` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 322 | `command` | *(none)* | `list` | `Awaitable[list]` | ✅ Needs verification |

### ClusterCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 323 | `cluster` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ Needs verification |
| 324 | `readwrite` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 325 | `readonly` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |

### FunctionCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 326 | `function_load` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 327 | `function_delete` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 328 | `function_flush` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 329 | `function_list` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 330 | `fcall` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 331 | `fcall_ro` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 332 | `function_dump` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 333 | `function_restore` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 334 | `function_kill` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 335 | `function_stats` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |

---

## Cluster Commands (`redis/commands/cluster.py`)

### ClusterMultiKeyCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 336 | `mget_nonatomic` | `list` | `list` | `Awaitable[list]` | ⚠️ Separate Async impl |
| 337 | `mset_nonatomic` | `list[bool]` | `list[bool]` | `Awaitable[list[bool]]` | ⚠️ Separate Async impl |
| 338 | `exists` | `int` | `int` | `Awaitable[int]` | 📋 Explicit |
| 339 | `delete` | `int` | `int` | `Awaitable[int]` | 📋 Explicit |
| 340 | `touch` | `int` | `int` | `Awaitable[int]` | 📋 Explicit |
| 341 | `unlink` | `int` | `int` | `Awaitable[int]` | 📋 Explicit |

### ClusterManagementCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 342 | `slaveof` | `NoReturn` | `NoReturn` | `NoReturn` | 📋 Explicit (raises) |
| 343 | `replicaof` | `NoReturn` | `NoReturn` | `NoReturn` | 📋 Explicit (raises) |
| 344 | `swapdb` | `NoReturn` | `NoReturn` | `NoReturn` | 📋 Explicit (raises) |
| 345 | `cluster_myid` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 346 | `cluster_addslots` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 347 | `cluster_addslotsrange` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 348 | `cluster_countkeysinslot` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 349 | `cluster_count_failure_report` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 350 | `cluster_delslots` | `list[bool]` | `list[bool]` | `Awaitable[list[bool]]` | ⚠️ Separate Async impl |
| 351 | `cluster_delslotsrange` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 352 | `cluster_failover` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 353 | `cluster_info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 354 | `cluster_keyslot` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 355 | `cluster_meet` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 356 | `cluster_nodes` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 357 | `cluster_replicate` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 358 | `cluster_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 359 | `cluster_save_config` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 360 | `cluster_get_keys_in_slot` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 361 | `cluster_set_config_epoch` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 362 | `cluster_setslot` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 363 | `cluster_setslot_stable` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 364 | `cluster_replicas` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 365 | `cluster_slots` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 366 | `cluster_shards` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 367 | `cluster_myshardid` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 368 | `cluster_links` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 369 | `cluster_flushslots` | `ResponseT` | `None` | `Awaitable[None]` | ✅ Needs verification |
| 370 | `cluster_bumpepoch` | `ResponseT` | `None` | `Awaitable[None]` | ✅ Needs verification |
| 371 | `client_tracking_on` | `Union[Awaitable[bool], bool]` | `bool` | `Awaitable[bool]` | ⚠️ Separate Async impl |
| 372 | `client_tracking_off` | `Union[Awaitable[bool], bool]` | `bool` | `Awaitable[bool]` | ⚠️ Separate Async impl |
| 373 | `hotkeys_start` | `Union[Awaitable[...], ...]` | `str \| bytes` | `Awaitable[str \| bytes]` | ⚠️ Separate Async impl |
| 374 | `hotkeys_stop` | `Union[Awaitable[...], ...]` | `str \| bytes` | `Awaitable[str \| bytes]` | ⚠️ Separate Async impl |
| 375 | `hotkeys_reset` | `Union[Awaitable[...], ...]` | `str \| bytes` | `Awaitable[str \| bytes]` | ⚠️ Separate Async impl |
| 376 | `hotkeys_get` | `Union[Awaitable[...], ...]` | `list[dict]` | `Awaitable[list[dict]]` | ⚠️ Separate Async impl |

### ClusterDataAccessCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 377 | `stralgo` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ Needs verification |
| 378 | `scan_iter` | `Iterator` | `Iterator` | `AsyncIterator` | 🔄 Iterator - separate impl |

---

## Sentinel Commands (`redis/commands/sentinel.py`)

### SentinelCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 379 | `sentinel` | `ResponseT` | `Any` | `Awaitable[Any]` | ⚠️ Separate Async impl |
| 380 | `sentinel_get_master_addr_by_name` | `ResponseT` | `tuple` | `Awaitable[tuple]` | ✅ Needs verification |
| 381 | `sentinel_master` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 382 | `sentinel_masters` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 383 | `sentinel_monitor` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 384 | `sentinel_remove` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 385 | `sentinel_sentinels` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 386 | `sentinel_set` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 387 | `sentinel_slaves` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 388 | `sentinel_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 389 | `sentinel_failover` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 390 | `sentinel_ckquorum` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 391 | `sentinel_flushconfig` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |

---

## Search Commands (`redis/commands/search/commands.py`)

### SearchCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 392 | `batch_indexer` | `BatchIndexer` | `BatchIndexer` | `BatchIndexer` | 📋 Explicit |
| 393 | `create_index` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 394 | `alter_schema_add` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 395 | `dropindex` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 396 | `add_document` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 397 | `add_document_hash` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 398 | `delete_document` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 399 | `load_document` | `Document` | `Document` | `Awaitable[Document]` | ⚠️ Separate Async impl |
| 400 | `get` | *(none)* | `Document` | `Awaitable[Document]` | ✅ Needs verification |
| 401 | `info` | *(none)* | `dict` | `Awaitable[dict]` | ⚠️ Separate Async impl |
| 402 | `get_params_args` | `list` | `list` | `list` | 📋 Explicit (helper) |
| 403 | `search` | `Result` | `Result` | `Awaitable[Result]` | ⚠️ Separate Async impl |
| 404 | `hybrid_search` | `HybridResult` | `HybridResult` | `Awaitable[HybridResult]` | ⚠️ Separate Async impl |
| 405 | `explain` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 406 | `explain_cli` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 407 | `aggregate` | `AggregateResult` | `AggregateResult` | `Awaitable[AggregateResult]` | ⚠️ Separate Async impl |
| 408 | `profile` | *(none)* | `tuple` | `Awaitable[tuple]` | ✅ Needs verification |
| 409 | `spellcheck` | *(none)* | `dict` | `Awaitable[dict]` | ⚠️ Separate Async impl |
| 410 | `dict_add` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 411 | `dict_del` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 412 | `dict_dump` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 413 | `config_set` | *(none)* | `bool` | `Awaitable[bool]` | ⚠️ Separate Async impl |
| 414 | `config_get` | *(none)* | `str` | `Awaitable[str]` | ⚠️ Separate Async impl |
| 415 | `tagvals` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 416 | `aliasadd` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 417 | `aliasupdate` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 418 | `aliasdel` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 419 | `sugadd` | `int` | `int` | `Awaitable[int]` | ⚠️ Separate Async impl |
| 420 | `suglen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 421 | `sugdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 422 | `sugget` | *(none)* | `list` | `Awaitable[list]` | ⚠️ Separate Async impl |
| 423 | `synupdate` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 424 | `syndump` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |

---

## JSON Commands (`redis/commands/json/commands.py`)

### JSONCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 425 | `arrappend` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification |
| 426 | `arrindex` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification |
| 427 | `arrinsert` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification |
| 428 | `arrlen` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification |
| 429 | `arrpop` | `ResponseT` | `list[str \| None]` | `Awaitable[list[str \| None]]` | ✅ Needs verification |
| 430 | `arrtrim` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification |
| 431 | `type` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification |
| 432 | `resp` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 433 | `objkeys` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 434 | `objlen` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification |
| 435 | `numincrby` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 436 | `nummultby` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification |
| 437 | `clear` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 438 | `delete` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 439 | `get` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Needs verification |
| 440 | `mget` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 441 | `set` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification |
| 442 | `mset` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification |
| 443 | `merge` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification |
| 444 | `set_file` | `Optional[str]` | `str \| None` | `Awaitable[str \| None]` | 📋 Explicit |
| 445 | `set_path` | `dict[str, bool]` | `dict[str, bool]` | `Awaitable[dict[str, bool]]` | 📋 Explicit |
| 446 | `strlen` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification |
| 447 | `toggle` | `ResponseT` | `bool \| list` | `Awaitable[bool \| list]` | ✅ Needs verification |
| 448 | `strappend` | `ResponseT` | `int \| list` | `Awaitable[int \| list]` | ✅ Needs verification |
| 449 | `debug` | `ResponseT` | `int \| list[str]` | `Awaitable[int \| list[str]]` | ✅ Needs verification |
| 450 | `jsonget` | *(none)* | `Any` | `Awaitable[Any]` | ✅ Needs verification (deprecated) |
| 451 | `jsonmget` | *(none)* | `Any` | `Awaitable[Any]` | ✅ Needs verification (deprecated) |
| 452 | `jsonset` | *(none)* | `Any` | `Awaitable[Any]` | ✅ Needs verification (deprecated) |

---

## TimeSeries Commands (`redis/commands/timeseries/commands.py`)

### TimeSeriesCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 453 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 454 | `alter` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 455 | `add` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 456 | `madd` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification |
| 457 | `incrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 458 | `decrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 459 | `delete` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 460 | `createrule` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 461 | `deleterule` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 462 | `range` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 463 | `revrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 464 | `mrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 465 | `mrevrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 466 | `get` | `ResponseT` | `tuple` | `Awaitable[tuple]` | ✅ Needs verification |
| 467 | `mget` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 468 | `info` | `TSInfo` | `TSInfo` | `Awaitable[TSInfo]` | 📋 Explicit |
| 469 | `queryindex` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |

---

## Bloom Filter Commands (`redis/commands/bf/commands.py`)

### BFCommands (Bloom Filter)

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 470 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 471 | `add` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 472 | `madd` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification |
| 473 | `insert` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification |
| 474 | `exists` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 475 | `mexists` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification |
| 476 | `scandump` | `ResponseT` | `tuple` | `Awaitable[tuple]` | ✅ Needs verification |
| 477 | `loadchunk` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 478 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 479 | `card` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |

### CFCommands (Cuckoo Filter)

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 480 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 481 | `add` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 482 | `addnx` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 483 | `insert` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification |
| 484 | `insertnx` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification |
| 485 | `exists` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 486 | `mexists` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification |
| 487 | `delete` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 488 | `count` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 489 | `scandump` | `ResponseT` | `tuple` | `Awaitable[tuple]` | ✅ Needs verification |
| 490 | `loadchunk` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 491 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |

### TOPKCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 492 | `reserve` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 493 | `add` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 494 | `incrby` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 495 | `query` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification |
| 496 | `count` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification |
| 497 | `list` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification |
| 498 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |

### TDigestCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 499 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 500 | `reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 501 | `add` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 502 | `merge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 503 | `min` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification |
| 504 | `max` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification |
| 505 | `quantile` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Needs verification |
| 506 | `cdf` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Needs verification |
| 507 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 508 | `trimmed_mean` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification |
| 509 | `rank` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification |
| 510 | `revrank` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification |
| 511 | `byrank` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Needs verification |
| 512 | `byrevrank` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Needs verification |

### CMSCommands (Count-Min Sketch)

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 513 | `initbydim` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 514 | `initbyprob` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 515 | `incrby` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification |
| 516 | `query` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification |
| 517 | `merge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification |
| 518 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |

---

## VectorSet Commands (`redis/commands/vectorset/commands.py`)

### VectorSetCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status |
|---|--------|----------------|--------------|---------------|--------|
| 519 | `vadd` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 520 | `vsim` | `VSimResult` | `VSimResult` | `Awaitable[VSimResult]` | 📋 Explicit |
| 521 | `vdim` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 522 | `vcard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 523 | `vrem` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 524 | `vemb` | `ResponseT` | `list \| dict \| None` | `Awaitable[list \| dict \| None]` | ✅ Needs verification |
| 525 | `vlinks` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Needs verification |
| 526 | `vinfo` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification |
| 527 | `vsetattr` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification |
| 528 | `vgetattr` | `ResponseT` | `dict \| None` | `Awaitable[dict \| None]` | ✅ Needs verification |
| 529 | `vrandmember` | `ResponseT` | `list[str] \| str \| None` | `Awaitable[list[str] \| str \| None]` | ✅ Needs verification |
| 530 | `vrange` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification |

---

## Summary by Category

### Methods That Can Use Standard Overload Pattern: ~495

These methods can directly use the `@overload` pattern with self-type discrimination.

**Note:** The "Assumed Sync" and "Assumed Async" columns contain types inferred from Redis documentation and need **manual verification** before implementation.

### Methods Requiring Separate Async Implementation: ~25

| File | Method | Reason |
|------|--------|--------|
| `core.py` | `command_info`, `debug_segfault`, `memory_doctor`, `memory_help`, `shutdown` | Returns `None`, async just awaits |
| `core.py` | `watch`, `unwatch` | Transaction-related, needs async await |
| `core.py` | `script_debug` | Returns `None` |
| `core.py` | `register_script` | Returns different class (`Script` vs `AsyncScript`) |
| `cluster.py` | `mget_nonatomic`, `mset_nonatomic` | Complex multi-node operations |
| `cluster.py` | `cluster_delslots`, `client_tracking_*`, `hotkeys_*` | Cluster-specific async handling |
| `search/commands.py` | `info`, `search`, `hybrid_search`, `aggregate`, `spellcheck`, `config_*`, `sugadd`, `sugget`, `load_document` | Result parsing after await |
| `sentinel.py` | `sentinel` | Async implementation differs |

### Iterator Methods (Cannot Use Simple Overload): 5

| File | Method | Notes |
|------|--------|-------|
| `core.py` | `scan_iter` | Returns `Iterator` / `AsyncIterator` |
| `core.py` | `sscan_iter` | Returns `Iterator` / `AsyncIterator` |
| `core.py` | `hscan_iter` | Returns `Iterator` / `AsyncIterator` |
| `core.py` | `zscan_iter` | Returns `Iterator` / `AsyncIterator` |
| `cluster.py` | `scan_iter` | Returns `Iterator` / `AsyncIterator` |

### Dunder Methods (Async Raises TypeError): 4

| File | Method | Notes |
|------|--------|-------|
| `core.py` | `__delitem__` | Cannot be async, raises `TypeError` |
| `core.py` | `__getitem__` | Cannot be async, raises `TypeError` |
| `core.py` | `__setitem__` | Cannot be async, raises `TypeError` |
| `core.py` | `__contains__` | Cannot be async, raises `TypeError` |

---

## Verification Checklist

Before implementing overloads for any method marked "✅ Needs verification", verify the return type by checking:

1. **Redis Documentation**: Check [redis.io/commands](https://redis.io/commands) for the official return type specification
2. **Existing Tests**: Look at test assertions to see what types are expected
3. **Response Callbacks**: Check if there are any response parsing callbacks that transform the raw response

### Return Type Mapping Reference (Redis → Python)

| Redis Type | Python Type |
|------------|-------------|
| Integer reply | `int` |
| Simple string reply ("OK") | `bool` (usually after callback) |
| Bulk string reply | `bytes` or `str` (depends on decode_responses) |
| Array reply | `list` |
| Null bulk string | `None` |
| Map reply (RESP3) | `dict` |

---

## Implementation Priority

### Phase 1: High-Impact Core Commands (~50 methods)
Most commonly used commands that would benefit users immediately:
- `get`, `set`, `delete`, `exists`, `expire`, `ttl`
- `hget`, `hset`, `hgetall`, `hdel`
- `lpush`, `rpush`, `lpop`, `rpop`, `lrange`
- `sadd`, `srem`, `smembers`, `sismember`
- `zadd`, `zrem`, `zrange`, `zscore`
- `publish`, `subscribe`

### Phase 2: Extended Core Commands (~150 methods)
Remaining core commands in `core.py`

### Phase 3: Module Commands (~100 methods)
- Search commands
- JSON commands
- TimeSeries commands
- Bloom filter commands
- VectorSet commands

### Phase 4: Cluster & Special Commands (~50 methods)
- Cluster management
- Sentinel commands
- Script commands
