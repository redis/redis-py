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

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 1 | `acl_cat` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification | 🔲 TODO |
| 2 | `acl_dryrun` | *(none)* | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 3 | `acl_deluser` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 4 | `acl_genpass` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 5 | `acl_getuser` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 6 | `acl_help` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification | 🔲 TODO |
| 7 | `acl_list` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification | 🔲 TODO |
| 8 | `acl_log` | `ResponseT` | `list[dict]` | `Awaitable[list[dict]]` | ✅ Needs verification | 🔲 TODO |
| 9 | `acl_log_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 10 | `acl_load` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 11 | `acl_save` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 12 | `acl_setuser` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 13 | `acl_users` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification | 🔲 TODO |
| 14 | `acl_whoami` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |

### ManagementCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 15 | `auth` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 16 | `bgrewriteaof` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 17 | `bgsave` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 18 | `role` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 19 | `client_kill` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 20 | `client_kill_filter` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 21 | `client_info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 22 | `client_list` | `ResponseT` | `list[dict]` | `Awaitable[list[dict]]` | ✅ Needs verification | 🔲 TODO |
| 23 | `client_getname` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification | 🔲 TODO |
| 24 | `client_getredir` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 25 | `client_reply` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 26 | `client_id` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 27 | `client_tracking_on` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 28 | `client_tracking_off` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 29 | `client_tracking` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 30 | `client_trackinginfo` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 31 | `client_setname` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 32 | `client_setinfo` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 33 | `client_unblock` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 34 | `client_pause` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 35 | `client_unpause` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 36 | `client_no_evict` | `Union[Awaitable[str], str]` | `str` | `Awaitable[str]` | 📋 Explicit | 🔲 TODO |
| 37 | `client_no_touch` | `Union[Awaitable[str], str]` | `str` | `Awaitable[str]` | 📋 Explicit | 🔲 TODO |
| 38 | `command` | *(none)* | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 39 | `command_info` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 40 | `command_count` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 41 | `command_list` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification | 🔲 TODO |
| 42 | `command_getkeysandflags` | `List[Union[str, List[str]]]` | `List[Union[str, List[str]]]` | `Awaitable[List[Union[str, List[str]]]]` | 📋 Explicit | 🔲 TODO |
| 43 | `command_docs` | *(none)* | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 44 | `config_get` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 45 | `config_set` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 46 | `config_resetstat` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 47 | `config_rewrite` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 48 | `dbsize` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 49 | `debug_object` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 50 | `debug_segfault` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 51 | `echo` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification | 🔲 TODO |
| 52 | `flushall` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 53 | `flushdb` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 54 | `sync` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification | 🔲 TODO |
| 55 | `psync` | *(none)* | `bytes` | `Awaitable[bytes]` | ✅ Needs verification | 🔲 TODO |
| 56 | `swapdb` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 57 | `select` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 58 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 59 | `lastsave` | `ResponseT` | `datetime` | `Awaitable[datetime]` | ✅ Needs verification | 🔲 TODO |
| 60 | `latency_doctor` | *(none)* | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 61 | `latency_graph` | *(none)* | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 62 | `lolwut` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 63 | `reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 64 | `migrate` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 65 | `object` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ Needs verification | 🔲 TODO |
| 66 | `memory_doctor` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 67 | `memory_help` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 68 | `memory_stats` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 69 | `memory_malloc_stats` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 70 | `memory_usage` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 71 | `memory_purge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 72 | `latency_histogram` | *(none)* | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 73 | `latency_history` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 74 | `latency_latest` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 75 | `latency_reset` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 76 | `ping` | `Union[Awaitable[bool], bool]` | `bool` | `Awaitable[bool]` | 📋 Explicit | 🔲 TODO |
| 77 | `quit` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 78 | `replicaof` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 79 | `save` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 80 | `shutdown` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 81 | `slaveof` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 82 | `slowlog_get` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 83 | `slowlog_len` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 84 | `slowlog_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 85 | `time` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 86 | `wait` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 87 | `waitaof` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 88 | `hello` | *(none)* | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 89 | `failover` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 90 | `hotkeys_start` | `Union[Awaitable[Union[str, bytes]], Union[str, bytes]]` | `str \| bytes` | `Awaitable[str \| bytes]` | 📋 Explicit | 🔲 TODO |
| 91 | `hotkeys_stop` | `Union[Awaitable[Union[str, bytes]], Union[str, bytes]]` | `str \| bytes` | `Awaitable[str \| bytes]` | 📋 Explicit | 🔲 TODO |
| 92 | `hotkeys_reset` | `Union[Awaitable[Union[str, bytes]], Union[str, bytes]]` | `str \| bytes` | `Awaitable[str \| bytes]` | 📋 Explicit | 🔲 TODO |
| 93 | `hotkeys_get` | `Union[Awaitable[list[dict[...]]], list[dict[...]]]` | `list[dict]` | `Awaitable[list[dict]]` | 📋 Explicit | 🔲 TODO |

### BasicKeyCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 94 | `append` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 95 | `bitcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 96 | `bitfield` | `BitFieldOperation` | `BitFieldOperation` | `BitFieldOperation` | 📋 Explicit | 🔲 TODO |
| 97 | `bitfield_ro` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification | 🔲 TODO |
| 98 | `bitop` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 99 | `bitpos` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 100 | `copy` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 101 | `decrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 102 | `delete` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 103 | `__delitem__` | `None` | `None` | N/A | ❌ Dunder - raises TypeError in async | ⏭️ N/A |
| 104 | `delex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 105 | `dump` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification | 🔲 TODO |
| 106 | `exists` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 107 | `expire` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 108 | `expireat` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 109 | `expiretime` | `int` | `int` | `Awaitable[int]` | 📋 Explicit | 🔲 TODO |
| 110 | `digest_local` | `bytes \| str` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 Explicit | 🔲 TODO |
| 111 | `digest` | `ResponseT` | `str \| bytes \| None` | `Awaitable[str \| bytes \| None]` | ✅ Needs verification | 🔲 TODO |
| 112 | `get` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification | ✅ Done |
| 113 | `getdel` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification | 🔲 TODO |
| 114 | `getex` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification | 🔲 TODO |
| 115 | `__getitem__` | `bytes` | `bytes` | N/A | ❌ Dunder - raises TypeError in async | ⏭️ N/A |
| 116 | `getbit` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 117 | `getrange` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification | 🔲 TODO |
| 118 | `getset` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification | 🔲 TODO |
| 119 | `incrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 120 | `incrbyfloat` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification | 🔲 TODO |
| 121 | `keys` | `ResponseT` | `list[bytes]` | `Awaitable[list[bytes]]` | ✅ Needs verification | 🔲 TODO |
| 122 | `lmove` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification | 🔲 TODO |
| 123 | `blmove` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification | 🔲 TODO |
| 124 | `mget` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 125 | `mset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 126 | `msetex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 127 | `msetnx` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 128 | `move` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 129 | `persist` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 130 | `pexpire` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 131 | `pexpireat` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 132 | `pexpiretime` | `int` | `int` | `Awaitable[int]` | 📋 Explicit | 🔲 TODO |
| 133 | `psetex` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 134 | `pttl` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 135 | `hrandfield` | `ResponseT` | `bytes \| list` | `Awaitable[bytes \| list]` | ✅ Needs verification | 🔲 TODO |
| 136 | `randomkey` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Needs verification | 🔲 TODO |
| 137 | `rename` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 138 | `renamenx` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 139 | `restore` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 140 | `set` | `ResponseT` | `bool \| None` | `Awaitable[bool \| None]` | ✅ Needs verification | ✅ Done |
| 141 | `__setitem__` | `None` | `None` | N/A | ❌ Dunder - raises TypeError in async | ⏭️ N/A |
| 142 | `setbit` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 143 | `setex` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 144 | `setnx` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 145 | `setrange` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 146 | `stralgo` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ Needs verification | 🔲 TODO |
| 147 | `strlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 148 | `substr` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification | 🔲 TODO |
| 149 | `touch` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 150 | `ttl` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 151 | `type` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 152 | `watch` | `bool` | `bool` | `Awaitable[bool]` | ⚠️ Separate Async | ⏭️ N/A |
| 153 | `unwatch` | `bool` | `bool` | `Awaitable[bool]` | ⚠️ Separate Async | ⏭️ N/A |
| 154 | `unlink` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 155 | `lcs` | `ResponseT` | `str \| int \| list` | `Awaitable[str \| int \| list]` | ✅ Needs verification | 🔲 TODO |
| 156 | `__contains__` | `bool` | `bool` | N/A | ❌ Dunder - raises TypeError in async | ⏭️ N/A |

### ListCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 157 | `blpop` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 158 | `brpop` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 159 | `brpoplpush` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification | 🔲 TODO |
| 160 | `blmpop` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Needs verification | 🔲 TODO |
| 161 | `lmpop` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 162 | `lindex` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification | 🔲 TODO |
| 163 | `linsert` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 164 | `llen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 165 | `lpop` | `ResponseT` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ Needs verification | 🔲 TODO |
| 166 | `lpush` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 167 | `lpushx` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 168 | `lrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 169 | `lrem` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 170 | `lset` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 171 | `ltrim` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 172 | `rpop` | `ResponseT` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ Needs verification | 🔲 TODO |
| 173 | `rpoplpush` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 174 | `rpush` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 175 | `rpushx` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 176 | `lpos` | `ResponseT` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ Needs verification | 🔲 TODO |
| 177 | `sort` | `ResponseT` | `list \| int` | `Awaitable[list \| int]` | ✅ Needs verification | 🔲 TODO |
| 178 | `sort_ro` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |

### ScanCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 179 | `scan` | `ResponseT` | `tuple[int, list]` | `Awaitable[tuple[int, list]]` | ✅ Needs verification | 🔲 TODO |
| 180 | `scan_iter` | `Iterator` | `Iterator` | `AsyncIterator` | 🔄 Iterator - separate impl | ⏭️ N/A |
| 181 | `sscan` | `ResponseT` | `tuple[int, list]` | `Awaitable[tuple[int, list]]` | ✅ Needs verification | 🔲 TODO |
| 182 | `sscan_iter` | `Iterator` | `Iterator` | `AsyncIterator` | 🔄 Iterator - separate impl | ⏭️ N/A |
| 183 | `hscan` | `ResponseT` | `tuple[int, dict]` | `Awaitable[tuple[int, dict]]` | ✅ Needs verification | 🔲 TODO |
| 184 | `hscan_iter` | `Iterator` | `Iterator` | `AsyncIterator` | 🔄 Iterator - separate impl | ⏭️ N/A |
| 185 | `zscan` | `ResponseT` | `tuple[int, list]` | `Awaitable[tuple[int, list]]` | ✅ Needs verification | 🔲 TODO |
| 186 | `zscan_iter` | `Iterator` | `Iterator` | `AsyncIterator` | 🔄 Iterator - separate impl | ⏭️ N/A |

### SetCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 187 | `sadd` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 188 | `scard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 189 | `sdiff` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 190 | `sdiffstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 191 | `sinter` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 192 | `sintercard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 193 | `sinterstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 194 | `sismember` | `ResponseT` | `Literal[0, 1]` | `Awaitable[Literal[0, 1]]` | ✅ Needs verification | 🔲 TODO |
| 195 | `smembers` | `ResponseT` | `Set` | `Awaitable[Set]` | ✅ Needs verification | 🔲 TODO |
| 196 | `smismember` | `ResponseT` | `list[Literal[0, 1]]` | `Awaitable[list[Literal[0, 1]]]` | ✅ Needs verification | 🔲 TODO |
| 197 | `smove` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 198 | `spop` | `ResponseT` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ Needs verification | 🔲 TODO |
| 199 | `srandmember` | `ResponseT` | `str \| list \| None` | `Awaitable[str \| list \| None]` | ✅ Needs verification | 🔲 TODO |
| 200 | `srem` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 201 | `sunion` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 202 | `sunionstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |

### StreamCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 203 | `xack` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 204 | `xackdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 205 | `xadd` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ Needs verification | 🔲 TODO |
| 206 | `xcfgset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 207 | `xautoclaim` | `ResponseT` | `tuple` | `Awaitable[tuple]` | ✅ Needs verification | 🔲 TODO |
| 208 | `xclaim` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 209 | `xdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 210 | `xdelex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 211 | `xgroup_create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 212 | `xgroup_delconsumer` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 213 | `xgroup_destroy` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 214 | `xgroup_createconsumer` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 215 | `xgroup_setid` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 216 | `xinfo_consumers` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 217 | `xinfo_groups` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 218 | `xinfo_stream` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 219 | `xlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 220 | `xpending` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 221 | `xpending_range` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 222 | `xrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 223 | `xread` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 224 | `xreadgroup` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 225 | `xrevrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 226 | `xtrim` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |

### SortedSetCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 227 | `zadd` | `ResponseT` | `int \| float` | `Awaitable[int \| float]` | ✅ Needs verification | 🔲 TODO |
| 228 | `zcard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 229 | `zcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 230 | `zdiff` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 231 | `zdiffstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 232 | `zincrby` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification | 🔲 TODO |
| 233 | `zinter` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 234 | `zinterstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 235 | `zintercard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 236 | `zlexcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 237 | `zpopmax` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 238 | `zpopmin` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 239 | `zrandmember` | `ResponseT` | `str \| list` | `Awaitable[str \| list]` | ✅ Needs verification | 🔲 TODO |
| 240 | `bzpopmax` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 241 | `bzpopmin` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 242 | `zmpop` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 243 | `bzmpop` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Needs verification | 🔲 TODO |
| 244 | `zrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 245 | `zrevrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 246 | `zrangestore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 247 | `zrangebylex` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 248 | `zrevrangebylex` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 249 | `zrangebyscore` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 250 | `zrevrangebyscore` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 251 | `zrank` | `ResponseT` | `int \| None` | `Awaitable[int \| None]` | ✅ Needs verification | 🔲 TODO |
| 252 | `zrem` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 253 | `zremrangebylex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 254 | `zremrangebyrank` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 255 | `zremrangebyscore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 256 | `zrevrank` | `ResponseT` | `int \| None` | `Awaitable[int \| None]` | ✅ Needs verification | 🔲 TODO |
| 257 | `zscore` | `ResponseT` | `float \| None` | `Awaitable[float \| None]` | ✅ Needs verification | 🔲 TODO |
| 258 | `zunion` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 259 | `zunionstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 260 | `zmscore` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |

### HyperlogCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 261 | `pfadd` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 262 | `pfcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 263 | `pfmerge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |

### HashCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 264 | `hdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 265 | `hexists` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 266 | `hget` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification | 🔲 TODO |
| 267 | `hgetall` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 268 | `hgetdel` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Needs verification | 🔲 TODO |
| 269 | `hgetex` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Needs verification | 🔲 TODO |
| 270 | `hincrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 271 | `hincrbyfloat` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification | 🔲 TODO |
| 272 | `hkeys` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 273 | `hlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 274 | `hset` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 275 | `hsetex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 276 | `hsetnx` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 277 | `hmset` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 278 | `hmget` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 279 | `hvals` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 280 | `hstrlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 281 | `hexpire` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 282 | `hpexpire` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 283 | `hexpireat` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 284 | `hpexpireat` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 285 | `hpersist` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 286 | `hexpiretime` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 287 | `hpexpiretime` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 288 | `httl` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 289 | `hpttl` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |

### PubSubCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 290 | `publish` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 291 | `spublish` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 292 | `pubsub_channels` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 293 | `pubsub_shardchannels` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 294 | `pubsub_numpat` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 295 | `pubsub_numsub` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 296 | `pubsub_shardnumsub` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |

### ScriptCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 297 | `eval` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 298 | `eval_ro` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 299 | `evalsha` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 300 | `evalsha_ro` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 301 | `script_exists` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification | 🔲 TODO |
| 302 | `script_debug` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 303 | `script_flush` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 304 | `script_kill` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 305 | `script_load` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 306 | `register_script` | `Script` | `Script` | `AsyncScript` | ⚠️ Returns different class | ⏭️ N/A |

### GeoCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 307 | `geoadd` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 308 | `geodist` | `ResponseT` | `float \| None` | `Awaitable[float \| None]` | ✅ Needs verification | 🔲 TODO |
| 309 | `geohash` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 310 | `geopos` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 311 | `georadius` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 312 | `georadiusbymember` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 313 | `geosearch` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 314 | `geosearchstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |

### ModuleCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 315 | `module_load` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 316 | `module_loadex` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 317 | `module_unload` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 318 | `module_list` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 319 | `command_info` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 320 | `command_count` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 321 | `command_getkeys` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 322 | `command` | *(none)* | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |

### ClusterCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 323 | `cluster` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ Needs verification | 🔲 TODO |
| 324 | `readwrite` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 325 | `readonly` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |

### FunctionCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 326 | `function_load` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 327 | `function_delete` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 328 | `function_flush` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 329 | `function_list` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 330 | `fcall` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 331 | `fcall_ro` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 332 | `function_dump` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 333 | `function_restore` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 334 | `function_kill` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 335 | `function_stats` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |

---

## Cluster Commands (`redis/commands/cluster.py`)

### ClusterMultiKeyCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 336 | `mget_nonatomic` | `list` | `list` | `Awaitable[list]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 337 | `mset_nonatomic` | `list[bool]` | `list[bool]` | `Awaitable[list[bool]]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 338 | `exists` | `int` | `int` | `Awaitable[int]` | 📋 Explicit | 🔲 TODO |
| 339 | `delete` | `int` | `int` | `Awaitable[int]` | 📋 Explicit | 🔲 TODO |
| 340 | `touch` | `int` | `int` | `Awaitable[int]` | 📋 Explicit | 🔲 TODO |
| 341 | `unlink` | `int` | `int` | `Awaitable[int]` | 📋 Explicit | 🔲 TODO |

### ClusterManagementCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 342 | `slaveof` | `NoReturn` | `NoReturn` | `NoReturn` | 📋 Explicit (raises) | ⏭️ N/A |
| 343 | `replicaof` | `NoReturn` | `NoReturn` | `NoReturn` | 📋 Explicit (raises) | ⏭️ N/A |
| 344 | `swapdb` | `NoReturn` | `NoReturn` | `NoReturn` | 📋 Explicit (raises) | ⏭️ N/A |
| 345 | `cluster_myid` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 346 | `cluster_addslots` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 347 | `cluster_addslotsrange` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 348 | `cluster_countkeysinslot` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 349 | `cluster_count_failure_report` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 350 | `cluster_delslots` | `list[bool]` | `list[bool]` | `Awaitable[list[bool]]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 351 | `cluster_delslotsrange` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 352 | `cluster_failover` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 353 | `cluster_info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 354 | `cluster_keyslot` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 355 | `cluster_meet` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 356 | `cluster_nodes` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 357 | `cluster_replicate` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 358 | `cluster_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 359 | `cluster_save_config` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 360 | `cluster_get_keys_in_slot` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 361 | `cluster_set_config_epoch` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 362 | `cluster_setslot` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 363 | `cluster_setslot_stable` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 364 | `cluster_replicas` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 365 | `cluster_slots` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 366 | `cluster_shards` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 367 | `cluster_myshardid` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 368 | `cluster_links` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 369 | `cluster_flushslots` | `ResponseT` | `None` | `Awaitable[None]` | ✅ Needs verification | 🔲 TODO |
| 370 | `cluster_bumpepoch` | `ResponseT` | `None` | `Awaitable[None]` | ✅ Needs verification | 🔲 TODO |
| 371 | `client_tracking_on` | `Union[Awaitable[bool], bool]` | `bool` | `Awaitable[bool]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 372 | `client_tracking_off` | `Union[Awaitable[bool], bool]` | `bool` | `Awaitable[bool]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 373 | `hotkeys_start` | `Union[Awaitable[...], ...]` | `str \| bytes` | `Awaitable[str \| bytes]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 374 | `hotkeys_stop` | `Union[Awaitable[...], ...]` | `str \| bytes` | `Awaitable[str \| bytes]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 375 | `hotkeys_reset` | `Union[Awaitable[...], ...]` | `str \| bytes` | `Awaitable[str \| bytes]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 376 | `hotkeys_get` | `Union[Awaitable[...], ...]` | `list[dict]` | `Awaitable[list[dict]]` | ⚠️ Separate Async impl | ⏭️ N/A |

### ClusterDataAccessCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 377 | `stralgo` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ Needs verification | 🔲 TODO |
| 378 | `scan_iter` | `Iterator` | `Iterator` | `AsyncIterator` | 🔄 Iterator - separate impl | ⏭️ N/A |

---

## Sentinel Commands (`redis/commands/sentinel.py`)

### SentinelCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 379 | `sentinel` | `ResponseT` | `Any` | `Awaitable[Any]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 380 | `sentinel_get_master_addr_by_name` | `ResponseT` | `tuple` | `Awaitable[tuple]` | ✅ Needs verification | 🔲 TODO |
| 381 | `sentinel_master` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 382 | `sentinel_masters` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 383 | `sentinel_monitor` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 384 | `sentinel_remove` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 385 | `sentinel_sentinels` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 386 | `sentinel_set` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 387 | `sentinel_slaves` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 388 | `sentinel_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 389 | `sentinel_failover` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 390 | `sentinel_ckquorum` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 391 | `sentinel_flushconfig` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |

---

## Search Commands (`redis/commands/search/commands.py`)

### SearchCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 392 | `batch_indexer` | `BatchIndexer` | `BatchIndexer` | `BatchIndexer` | 📋 Explicit | 🔲 TODO |
| 393 | `create_index` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 394 | `alter_schema_add` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 395 | `dropindex` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 396 | `add_document` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 397 | `add_document_hash` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 398 | `delete_document` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 399 | `load_document` | `Document` | `Document` | `Awaitable[Document]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 400 | `get` | *(none)* | `Document` | `Awaitable[Document]` | ✅ Needs verification | 🔲 TODO |
| 401 | `info` | *(none)* | `dict` | `Awaitable[dict]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 402 | `get_params_args` | `list` | `list` | `list` | 📋 Explicit (helper) | ⏭️ N/A |
| 403 | `search` | `Result` | `Result` | `Awaitable[Result]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 404 | `hybrid_search` | `HybridResult` | `HybridResult` | `Awaitable[HybridResult]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 405 | `explain` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 406 | `explain_cli` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 407 | `aggregate` | `AggregateResult` | `AggregateResult` | `Awaitable[AggregateResult]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 408 | `profile` | *(none)* | `tuple` | `Awaitable[tuple]` | ✅ Needs verification | 🔲 TODO |
| 409 | `spellcheck` | *(none)* | `dict` | `Awaitable[dict]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 410 | `dict_add` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 411 | `dict_del` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 412 | `dict_dump` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 413 | `config_set` | *(none)* | `bool` | `Awaitable[bool]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 414 | `config_get` | *(none)* | `str` | `Awaitable[str]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 415 | `tagvals` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 416 | `aliasadd` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 417 | `aliasupdate` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 418 | `aliasdel` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 419 | `sugadd` | `int` | `int` | `Awaitable[int]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 420 | `suglen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 421 | `sugdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 422 | `sugget` | *(none)* | `list` | `Awaitable[list]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 423 | `synupdate` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 424 | `syndump` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |

---

## JSON Commands (`redis/commands/json/commands.py`)

### JSONCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 425 | `arrappend` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification | 🔲 TODO |
| 426 | `arrindex` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification | 🔲 TODO |
| 427 | `arrinsert` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification | 🔲 TODO |
| 428 | `arrlen` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification | 🔲 TODO |
| 429 | `arrpop` | `ResponseT` | `list[str \| None]` | `Awaitable[list[str \| None]]` | ✅ Needs verification | 🔲 TODO |
| 430 | `arrtrim` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification | 🔲 TODO |
| 431 | `type` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ Needs verification | 🔲 TODO |
| 432 | `resp` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 433 | `objkeys` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 434 | `objlen` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification | 🔲 TODO |
| 435 | `numincrby` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 436 | `nummultby` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Needs verification | 🔲 TODO |
| 437 | `clear` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 438 | `delete` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 439 | `get` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Needs verification | 🔲 TODO |
| 440 | `mget` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 441 | `set` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification | 🔲 TODO |
| 442 | `mset` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification | 🔲 TODO |
| 443 | `merge` | `ResponseT` | `str \| None` | `Awaitable[str \| None]` | ✅ Needs verification | 🔲 TODO |
| 444 | `set_file` | `Optional[str]` | `str \| None` | `Awaitable[str \| None]` | 📋 Explicit | 🔲 TODO |
| 445 | `set_path` | `dict[str, bool]` | `dict[str, bool]` | `Awaitable[dict[str, bool]]` | 📋 Explicit | 🔲 TODO |
| 446 | `strlen` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Needs verification | 🔲 TODO |
| 447 | `toggle` | `ResponseT` | `bool \| list` | `Awaitable[bool \| list]` | ✅ Needs verification | 🔲 TODO |
| 448 | `strappend` | `ResponseT` | `int \| list` | `Awaitable[int \| list]` | ✅ Needs verification | 🔲 TODO |
| 449 | `debug` | `ResponseT` | `int \| list[str]` | `Awaitable[int \| list[str]]` | ✅ Needs verification | 🔲 TODO |
| 450 | `jsonget` | *(none)* | `Any` | `Awaitable[Any]` | ✅ Needs verification (deprecated) | 🔲 TODO |
| 451 | `jsonmget` | *(none)* | `Any` | `Awaitable[Any]` | ✅ Needs verification (deprecated) | 🔲 TODO |
| 452 | `jsonset` | *(none)* | `Any` | `Awaitable[Any]` | ✅ Needs verification (deprecated) | 🔲 TODO |

---

## TimeSeries Commands (`redis/commands/timeseries/commands.py`)

### TimeSeriesCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 453 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 454 | `alter` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 455 | `add` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 456 | `madd` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification | 🔲 TODO |
| 457 | `incrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 458 | `decrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 459 | `delete` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 460 | `createrule` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 461 | `deleterule` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 462 | `range` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 463 | `revrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 464 | `mrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 465 | `mrevrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 466 | `get` | `ResponseT` | `tuple` | `Awaitable[tuple]` | ✅ Needs verification | 🔲 TODO |
| 467 | `mget` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 468 | `info` | `TSInfo` | `TSInfo` | `Awaitable[TSInfo]` | 📋 Explicit | 🔲 TODO |
| 469 | `queryindex` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |

---

## Bloom Filter Commands (`redis/commands/bf/commands.py`)

### BFCommands (Bloom Filter)

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 470 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 471 | `add` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 472 | `madd` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification | 🔲 TODO |
| 473 | `insert` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification | 🔲 TODO |
| 474 | `exists` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 475 | `mexists` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification | 🔲 TODO |
| 476 | `scandump` | `ResponseT` | `tuple` | `Awaitable[tuple]` | ✅ Needs verification | 🔲 TODO |
| 477 | `loadchunk` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 478 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 479 | `card` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |

### CFCommands (Cuckoo Filter)

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 480 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 481 | `add` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 482 | `addnx` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 483 | `insert` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification | 🔲 TODO |
| 484 | `insertnx` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification | 🔲 TODO |
| 485 | `exists` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 486 | `mexists` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification | 🔲 TODO |
| 487 | `delete` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 488 | `count` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Needs verification | 🔲 TODO |
| 489 | `scandump` | `ResponseT` | `tuple` | `Awaitable[tuple]` | ✅ Needs verification | 🔲 TODO |
| 490 | `loadchunk` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 491 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |

### TOPKCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 492 | `reserve` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 493 | `add` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 494 | `incrby` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 495 | `query` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Needs verification | 🔲 TODO |
| 496 | `count` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification | 🔲 TODO |
| 497 | `list` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Needs verification | 🔲 TODO |
| 498 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |

### TDigestCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 499 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 500 | `reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 501 | `add` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 502 | `merge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 503 | `min` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification | 🔲 TODO |
| 504 | `max` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification | 🔲 TODO |
| 505 | `quantile` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Needs verification | 🔲 TODO |
| 506 | `cdf` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Needs verification | 🔲 TODO |
| 507 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |
| 508 | `trimmed_mean` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Needs verification | 🔲 TODO |
| 509 | `rank` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification | 🔲 TODO |
| 510 | `revrank` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification | 🔲 TODO |
| 511 | `byrank` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Needs verification | 🔲 TODO |
| 512 | `byrevrank` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Needs verification | 🔲 TODO |

### CMSCommands (Count-Min Sketch)

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 513 | `initbydim` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 514 | `initbyprob` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 515 | `incrby` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification | 🔲 TODO |
| 516 | `query` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Needs verification | 🔲 TODO |
| 517 | `merge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Needs verification | 🔲 TODO |
| 518 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Needs verification | 🔲 TODO |

---

## VectorSet Commands (`redis/commands/vectorset/commands.py`)

### VectorSetCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 519 | `vadd` | `int` | `int` | `Awaitable[int]` | ✅ Standard | ✅ Done |
| 520 | `vsim` | `VSimResult` | `VSimResult` | `Awaitable[VSimResult]` | 📋 Explicit | ✅ Done |
| 521 | `vdim` | `int` | `int` | `Awaitable[int]` | ✅ Standard | ✅ Done |
| 522 | `vcard` | `int` | `int` | `Awaitable[int]` | ✅ Standard | ✅ Done |
| 523 | `vrem` | `int` | `int` | `Awaitable[int]` | ✅ Standard | ✅ Done |
| 524 | `vemb` | `VEmbResult` | `VEmbResult` | `Awaitable[VEmbResult]` | 📋 Explicit | ✅ Done |
| 525 | `vlinks` | `VLinksResult` | `VLinksResult` | `Awaitable[VLinksResult]` | 📋 Explicit | ✅ Done |
| 526 | `vinfo` | `dict` | `dict` | `Awaitable[dict]` | ✅ Standard | ✅ Done |
| 527 | `vsetattr` | `int` | `int` | `Awaitable[int]` | ✅ Standard | ✅ Done |
| 528 | `vgetattr` | `VGetAttrResult` | `VGetAttrResult` | `Awaitable[VGetAttrResult]` | 📋 Explicit | ✅ Done |
| 529 | `vrandmember` | `VRandMemberResult` | `VRandMemberResult` | `Awaitable[VRandMemberResult]` | 📋 Explicit | ✅ Done |
| 530 | `vrange` | `list[str]` | `list[str]` | `Awaitable[list[str]]` | ✅ Standard | ✅ Done |

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
