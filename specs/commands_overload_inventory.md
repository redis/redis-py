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
| 1 | `acl_cat` | `ResponseT` | `list[str]` (RESP2) / `list[bytes \| str]` (RESP3) | `Awaitable[...]` | ✅ RESP2: str_if_bytes / RESP3: no callback (raw) | ✅ Done |
| 2 | `acl_dryrun` | *(none)* | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 3 | `acl_deluser` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 4 | `acl_genpass` | `ResponseT` | `str` (RESP2) / `bytes \| str` (RESP3) | `Awaitable[...]` | ✅ RESP2: str_if_bytes / RESP3: no callback (raw) | ✅ Done |
| 5 | `acl_getuser` | `ResponseT` | `dict[str, str \| list[str] \| list[list[str]] \| list[dict[str, str]]] \| None` | `Awaitable[...]` | ✅ Base: parse_acl_getuser (str keys/values) | ✅ Done |
| 6 | `acl_help` | `ResponseT` | `list[str]` (RESP2) / `list[bytes \| str]` (RESP3) | `Awaitable[...]` | ✅ RESP2: str_if_bytes / RESP3: no callback (raw) | ✅ Done |
| 7 | `acl_list` | `ResponseT` | `list[str]` (RESP2) / `list[bytes \| str]` (RESP3) | `Awaitable[...]` | ✅ RESP2: str_if_bytes / RESP3: no callback (raw) | ✅ Done |
| 8 | `acl_log` | `ResponseT` | `list[dict[str, str \| float \| dict[str, str \| int]]]` | `Awaitable[...]` | ✅ Base: parse_acl_log / RESP3: lambda (all str keys/values) | ✅ Done |
| 9 | `acl_log_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Via acl_log with RESET → bool_ok | ✅ Done |
| 10 | `acl_load` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 11 | `acl_save` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 12 | `acl_setuser` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 13 | `acl_users` | `ResponseT` | `list[str]` (RESP2) / `list[bytes \| str]` (RESP3) | `Awaitable[...]` | ✅ RESP2: str_if_bytes / RESP3: no callback (raw) | ✅ Done |
| 14 | `acl_whoami` | `ResponseT` | `str` (RESP2) / `bytes \| str` (RESP3) | `Awaitable[...]` | ✅ RESP2: str_if_bytes / RESP3: no callback (raw) | ✅ Done |

### ManagementCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 15 | `auth` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 16 | `bgrewriteaof` | *(none)* | `bool` (RESP2) / `bytes \| str` (RESP3) | `Awaitable[...]` | ✅ RESP2: lambda r: True / RESP3: no callback | ✅ Done |
| 17 | `bgsave` | `ResponseT` | `bool` (RESP2) / `bytes \| str` (RESP3) | `Awaitable[...]` | ✅ RESP2: lambda r: True / RESP3: no callback | ✅ Done |
| 18 | `role` | `ResponseT` | `list[Any]` | `Awaitable[list[Any]]` | ✅ No callback - mixed types | ✅ Done |
| 19 | `client_kill` | `ResponseT` | `bool \| int` | `Awaitable[bool \| int]` | ✅ Base: parse_client_kill | ✅ Done |
| 20 | `client_kill_filter` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 21 | `client_info` | `ResponseT` | `dict[str, str \| int]` | `Awaitable[dict[str, str \| int]]` | ✅ Base: parse_client_info - mixed str/int values | ✅ Done |
| 22 | `client_list` | `ResponseT` | `list[dict[str, str]]` | `Awaitable[list[dict[str, str]]]` | ✅ Base: parse_client_list - str keys/values | ✅ Done |
| 23 | `client_getname` | `ResponseT` | `str \| None` (RESP2) / `bytes \| str \| None` (RESP3) | `Awaitable[...]` | ✅ RESP2: str_if_bytes / RESP3: no callback | ✅ Done |
| 24 | `client_getredir` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 25 | `client_reply` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 26 | `client_id` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 27 | `client_tracking_on` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - returns raw OK | ✅ Done |
| 28 | `client_tracking_off` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - returns raw OK | ✅ Done |
| 29 | `client_tracking` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - returns raw OK | ✅ Done |
| 30 | `client_trackinginfo` | `ResponseT` | `list[str]` (RESP2) / `list[bytes \| str]` (RESP3) | `Awaitable[...]` | ✅ RESP2: str_if_bytes / RESP3: no callback | ✅ Done |
| 31 | `client_setname` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 32 | `client_setinfo` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 33 | `client_unblock` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool (converts int to bool) | ✅ Done |
| 34 | `client_pause` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 35 | `client_unpause` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - returns raw OK | ✅ Done |
| 36 | `client_no_evict` | `Union[Awaitable[str], str]` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 No callback - depends on decode_responses | ✅ Done |
| 37 | `client_no_touch` | `Union[Awaitable[str], str]` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 No callback - depends on decode_responses | ✅ Done |
| 38 | `command` | *(none)* | `dict[str, dict[str, Any]]` | `Awaitable[dict[str, dict[str, Any]]]` | ✅ Base: parse_command / RESP3: parse_command_resp3 | ✅ Done |
| 39 | `command_info` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 40 | `command_count` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 41 | `command_list` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 42 | `command_getkeysandflags` | `List[Union[str, List[str]]]` | `list[list[bytes \| str \| list[bytes \| str]]]` | `Awaitable[list[list[bytes \| str \| list[bytes \| str]]]]` | ✅ No callback - mixed [key, flags] shape | ✅ Done |
| 43 | `command_docs` | *(none)* | `dict` | `Awaitable[dict]` | ✅ Intentionally unsupported in `core.py` | ⏭️ N/A (NotImplementedError) |
| 44 | `config_get` | `ResponseT` | `dict[str \| None, str \| None]` | `Awaitable[dict[str \| None, str \| None]]` | ✅ RESP2: parse_config_get / RESP3: lambda | ✅ Done |
| 45 | `config_set` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 46 | `config_resetstat` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 47 | `config_rewrite` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - returns raw OK | ✅ Done |
| 48 | `dbsize` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 49 | `debug_object` | `ResponseT` | `dict[str, str \| int]` (RESP2) / `bytes \| str` (RESP3) | `Awaitable[...]` | ✅ RESP2: parse_debug_object / RESP3: no callback | ✅ Done |
| 50 | `debug_segfault` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 51 | `echo` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 52 | `flushall` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 53 | `flushdb` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 54 | `sync` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ No callback - always bytes (RDB dump) | ✅ Done |
| 55 | `psync` | *(none)* | `bytes` | `Awaitable[bytes]` | ✅ No callback - always bytes (RDB dump) | ✅ Done |
| 56 | `swapdb` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 57 | `select` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 58 | `info` | `ResponseT` | `dict[str, Any]` | `Awaitable[dict[str, Any]]` | ✅ Base: parse_info - str keys | ✅ Done |
| 59 | `lastsave` | `ResponseT` | `datetime` | `Awaitable[datetime]` | ✅ Base: timestamp_to_datetime | ✅ Done |
| 60 | `latency_doctor` | *(none)* | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ Intentionally unsupported in `core.py` | ⏭️ N/A (NotImplementedError) |
| 61 | `latency_graph` | *(none)* | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ Intentionally unsupported in `core.py` | ⏭️ N/A (NotImplementedError) |
| 62 | `lolwut` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 63 | `reset` | `ResponseT` | `str` (RESP2) / `bytes \| str` (RESP3) | `Awaitable[...]` | ✅ RESP2: str_if_bytes / RESP3: no callback | ✅ Done |
| 64 | `migrate` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - returns raw `OK` / `NOKEY` | ✅ Done |
| 65 | `object` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ Varies by subcommand | ✅ Done |
| 66 | `memory_doctor` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 67 | `memory_help` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 68 | `memory_stats` | `ResponseT` | `dict[str, Any]` | `Awaitable[dict[str, Any]]` | ✅ RESP2: parse_memory_stats / RESP3: lambda (str keys) | ✅ Done |
| 69 | `memory_malloc_stats` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 70 | `memory_usage` | `ResponseT` | `int \| None` | `Awaitable[int \| None]` | ✅ Integer reply or None - no callback | ✅ Done |
| 71 | `memory_purge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 72 | `latency_histogram` | *(none)* | `dict` | `Awaitable[dict]` | ✅ Intentionally unsupported in `core.py` | ⏭️ N/A (NotImplementedError) |
| 73 | `latency_history` | `ResponseT` | `list[list[int]]` | `Awaitable[list[list[int]]]` | ✅ Raw array reply - no callback | ✅ Done |
| 74 | `latency_latest` | `ResponseT` | `list[list[bytes \| str \| int]]` | `Awaitable[list[list[bytes \| str \| int]]]` | ✅ Raw array reply - no callback | ✅ Done |
| 75 | `latency_reset` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 76 | `ping` | `Union[Awaitable[bool], bool]` | `bool` | `Awaitable[bool]` | 📋 Base: lambda (str_if_bytes == "PONG") | ✅ Done |
| 77 | `quit` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 78 | `replicaof` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - returns raw OK | ✅ Done |
| 79 | `save` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 80 | `shutdown` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 81 | `slaveof` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 82 | `slowlog_get` | `ResponseT` | `list[dict[str, Any]]` | `Awaitable[list[dict[str, Any]]]` | ✅ Base: parse_slowlog_get | ✅ Done |
| 83 | `slowlog_len` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 84 | `slowlog_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 85 | `time` | `ResponseT` | `tuple[int, int]` | `Awaitable[tuple[int, int]]` | ✅ Base: lambda (int(x[0]), int(x[1])) | ✅ Done |
| 86 | `wait` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 87 | `waitaof` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers - no callback | ✅ Done |
| 88 | `hello` | *(none)* | `dict` | `Awaitable[dict]` | ✅ Intentionally unsupported in `core.py` | ⏭️ N/A (NotImplementedError) |
| 89 | `failover` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Intentionally unsupported in `core.py` | ⏭️ N/A (NotImplementedError) |
| 90 | `hotkeys_start` | `Union[Awaitable[Union[str, bytes]], Union[str, bytes]]` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 No callback - depends on decode_responses | ✅ Done |
| 91 | `hotkeys_stop` | `Union[Awaitable[Union[str, bytes]], Union[str, bytes]]` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 No callback - depends on decode_responses | ✅ Done |
| 92 | `hotkeys_reset` | `Union[Awaitable[Union[str, bytes]], Union[str, bytes]]` | `bytes \| str` | `Awaitable[bytes \| str]` | 📋 No callback - depends on decode_responses | ✅ Done |
| 93 | `hotkeys_get` | `Union[Awaitable[list[dict[...]]], list[dict[...]]]` | `list[dict[str \| bytes, Any]]` | `Awaitable[list[dict[str \| bytes, Any]]]` | 📋 Base callback for `HOTKEYS GET` | ✅ Done |

### BasicKeyCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 94 | `append` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 95 | `bitcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 96 | `bitfield` | `BitFieldOperation` | `BitFieldOperation` | `BitFieldOperation` | 📋 Explicit - builder pattern | ✅ Done |
| 97 | `bitfield_ro` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers - no callback | ✅ Done |
| 98 | `bitop` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 99 | `bitpos` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 100 | `copy` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 101 | `decrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 102 | `delete` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 103 | `__delitem__` | `None` | `None` | N/A | ❌ Dunder - raises TypeError in async | ⏭️ N/A |
| 104 | `delex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 105 | `dump` | `ResponseT` | `bytes \| None` | `Awaitable[bytes \| None]` | ✅ Always bytes (serialized format) - no callback | ✅ Done |
| 106 | `exists` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 107 | `expire` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 108 | `expireat` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 109 | `expiretime` | `int` | `int` | `Awaitable[int]` | 📋 Integer reply - no callback | ✅ Done |
| 110 | `digest_local` | `bytes \| str` | `bytes \| str` | `bytes \| str` | 📋 Explicit - local computation | ✅ Done |
| 111 | `digest` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 112 | `get` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 113 | `getdel` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 114 | `getex` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 115 | `__getitem__` | `bytes \| str` | `bytes \| str` | N/A | ❌ Dunder - depends on decode_responses | ⏭️ N/A |
| 116 | `getbit` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply (0 or 1) - no callback | ✅ Done |
| 117 | `getrange` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 118 | `getset` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 119 | `incrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 120 | `incrbyfloat` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Base: float | ✅ Done |
| 121 | `keys` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 122 | `lmove` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 123 | `blmove` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 124 | `mget` | `ResponseT` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 125 | `mset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 126 | `msetex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 127 | `msetnx` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 128 | `move` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 129 | `persist` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 130 | `pexpire` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 131 | `pexpireat` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 132 | `pexpiretime` | `int` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 133 | `psetex` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 134 | `pttl` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 135 | `hrandfield` | `ResponseT` | `bytes \| str \| list[bytes \| str] \| None` | `Awaitable[bytes \| str \| list[bytes \| str] \| None]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 136 | `randomkey` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 137 | `rename` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | ✅ Done |
| 138 | `renamenx` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 139 | `restore` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - returns raw OK | ✅ Done |
| 140 | `set` | `ResponseT` | `bool \| bytes \| str \| None` | `Awaitable[bool \| bytes \| str \| None]` | ✅ Base: parse_set_result - bool or value with GET | ✅ Done |
| 141 | `__setitem__` | `None` | `None` | N/A | ❌ Dunder - raises TypeError in async | ⏭️ N/A |
| 142 | `setbit` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply (0 or 1) - no callback | ✅ Done |
| 143 | `setex` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 144 | `setnx` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | ✅ Done |
| 145 | `setrange` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 146 | `stralgo` | `ResponseT` | `str \| int \| dict[str, int \| list[list[int \| tuple[int, int]]]]` | `Awaitable[...]` | ✅ RESP2: parse_stralgo / RESP3: lambda | ✅ Done |
| 147 | `strlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 148 | `substr` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 149 | `touch` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 150 | `ttl` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 151 | `type` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | ✅ Done |
| 152 | `watch` | `bool` | `bool` | `Awaitable[bool]` | ⚠️ Base: bool_ok - Separate Async | ⏭️ N/A |
| 153 | `unwatch` | `bool` | `bool` | `Awaitable[bool]` | ⚠️ Base: bool_ok - Separate Async | ⏭️ N/A |
| 154 | `unlink` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | ✅ Done |
| 155 | `lcs` | `ResponseT` | `bytes \| str \| int \| list[Any] \| dict[Any, Any]` | `Awaitable[...]` | ✅ Raw reply varies by options and protocol | ✅ Done |
| 156 | `__contains__` | `bool` | `bool` | N/A | ❌ Dunder - raises TypeError in async | ⏭️ N/A |

### ListCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 157 | `blpop` | `ResponseT` | `tuple[bytes \| str, bytes \| str] \| None` | `Awaitable[...]` | ✅ RESP2: lambda (tuple or None) / RESP3: no callback | 🔲 TODO |
| 158 | `brpop` | `ResponseT` | `tuple[bytes \| str, bytes \| str] \| None` | `Awaitable[...]` | ✅ RESP2: lambda (tuple or None) / RESP3: no callback | 🔲 TODO |
| 159 | `brpoplpush` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 160 | `blmpop` | `ResponseT` | `list[bytes \| str] \| None` | `Awaitable[list[bytes \| str] \| None]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 161 | `lmpop` | `ResponseT` | `list[bytes \| str] \| None` | `Awaitable[list[bytes \| str] \| None]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 162 | `lindex` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 163 | `linsert` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 164 | `llen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 165 | `lpop` | `ResponseT` | `bytes \| str \| list[bytes \| str] \| None` | `Awaitable[...]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 166 | `lpush` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 167 | `lpushx` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 168 | `lrange` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 169 | `lrem` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 170 | `lset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 171 | `ltrim` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 172 | `rpop` | `ResponseT` | `bytes \| str \| list[bytes \| str] \| None` | `Awaitable[...]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 173 | `rpoplpush` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 174 | `rpush` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 175 | `rpushx` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 176 | `lpos` | `ResponseT` | `int \| list[int] \| None` | `Awaitable[int \| list[int] \| None]` | ✅ Integer(s) or None - no callback | 🔲 TODO |
| 177 | `sort` | `ResponseT` | `list[bytes \| str] \| int` | `Awaitable[list[bytes \| str] \| int]` | ✅ Base: sort_return_tuples - depends on decode_responses | 🔲 TODO |
| 178 | `sort_ro` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |

### ScanCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 179 | `scan` | `ResponseT` | `tuple[int, list[bytes \| str]]` | `Awaitable[tuple[int, list[bytes \| str]]]` | ✅ Base: parse_scan - depends on decode_responses | 🔲 TODO |
| 180 | `scan_iter` | `Iterator` | `Iterator[bytes \| str]` | `AsyncIterator[bytes \| str]` | 🔄 Iterator - separate impl | ⏭️ N/A |
| 181 | `sscan` | `ResponseT` | `tuple[int, list[bytes \| str]]` | `Awaitable[tuple[int, list[bytes \| str]]]` | ✅ Base: parse_scan - depends on decode_responses | 🔲 TODO |
| 182 | `sscan_iter` | `Iterator` | `Iterator[bytes \| str]` | `AsyncIterator[bytes \| str]` | 🔄 Iterator - separate impl | ⏭️ N/A |
| 183 | `hscan` | `ResponseT` | `tuple[int, dict[bytes \| str, bytes \| str]]` | `Awaitable[tuple[int, dict[bytes \| str, bytes \| str]]]` | ✅ Base: parse_hscan - depends on decode_responses | 🔲 TODO |
| 184 | `hscan_iter` | `Iterator` | `Iterator[tuple[bytes \| str, bytes \| str]]` | `AsyncIterator[tuple[bytes \| str, bytes \| str]]` | 🔄 Iterator - separate impl | ⏭️ N/A |
| 185 | `zscan` | `ResponseT` | `tuple[int, list[tuple[bytes \| str, float]]]` | `Awaitable[tuple[int, list[tuple[bytes \| str, float]]]]` | ✅ Base: parse_zscan - depends on decode_responses | 🔲 TODO |
| 186 | `zscan_iter` | `Iterator` | `Iterator[tuple[bytes \| str, float]]` | `AsyncIterator[tuple[bytes \| str, float]]` | 🔄 Iterator - separate impl | ⏭️ N/A |

### SetCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 187 | `sadd` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 188 | `scard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 189 | `sdiff` | `ResponseT` | `set[bytes \| str]` | `Awaitable[set[bytes \| str]]` | ✅ RESP2 & RESP3: lambda (set or empty set) | 🔲 TODO |
| 190 | `sdiffstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 191 | `sinter` | `ResponseT` | `set[bytes \| str]` | `Awaitable[set[bytes \| str]]` | ✅ RESP2 & RESP3: lambda (set or empty set) | 🔲 TODO |
| 192 | `sintercard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 193 | `sinterstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 194 | `sismember` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply (0 or 1) - no callback | 🔲 TODO |
| 195 | `smembers` | `ResponseT` | `set[bytes \| str]` | `Awaitable[set[bytes \| str]]` | ✅ RESP2 & RESP3: lambda (set or empty set) | 🔲 TODO |
| 196 | `smismember` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers (0 or 1) - no callback | 🔲 TODO |
| 197 | `smove` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | 🔲 TODO |
| 198 | `spop` | `ResponseT` | `bytes \| str \| list[bytes \| str] \| None` | `Awaitable[...]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 199 | `srandmember` | `ResponseT` | `bytes \| str \| list[bytes \| str] \| None` | `Awaitable[...]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 200 | `srem` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 201 | `sunion` | `ResponseT` | `set[bytes \| str]` | `Awaitable[set[bytes \| str]]` | ✅ RESP2 & RESP3: lambda (set or empty set) | 🔲 TODO |
| 202 | `sunionstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |

### StreamCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 203 | `xack` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 204 | `xackdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 205 | `xadd` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - stream ID depends on decode_responses | 🔲 TODO |
| 206 | `xcfgset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Returns OK - no specific callback | 🔲 TODO |
| 207 | `xautoclaim` | `ResponseT` | `tuple` | `Awaitable[tuple]` | ✅ Base: parse_xautoclaim - mixed types | 🔲 TODO |
| 208 | `xclaim` | `ResponseT` | `list[tuple[bytes \| str, dict]]` | `Awaitable[...]` | ✅ Base: parse_xclaim - depends on decode_responses | 🔲 TODO |
| 209 | `xdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 210 | `xdelex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 211 | `xgroup_create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 212 | `xgroup_delconsumer` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 213 | `xgroup_destroy` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | 🔲 TODO |
| 214 | `xgroup_createconsumer` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 215 | `xgroup_setid` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 216 | `xinfo_consumers` | `ResponseT` | `list[dict[str, Any]]` | `Awaitable[list[dict[str, Any]]]` | ✅ RESP2: parse_list_of_dicts / RESP3: lambda (str keys) | 🔲 TODO |
| 217 | `xinfo_groups` | `ResponseT` | `list[dict[str, Any]]` | `Awaitable[list[dict[str, Any]]]` | ✅ RESP2: parse_list_of_dicts / RESP3: lambda (str keys) | 🔲 TODO |
| 218 | `xinfo_stream` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Base: parse_xinfo_stream | 🔲 TODO |
| 219 | `xlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 220 | `xpending` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Base: parse_xpending | 🔲 TODO |
| 221 | `xpending_range` | `ResponseT` | `list[dict]` | `Awaitable[list[dict]]` | ✅ No specific callback - custom parsing | 🔲 TODO |
| 222 | `xrange` | `ResponseT` | `list[tuple[bytes \| str, dict]]` | `Awaitable[...]` | ✅ Base: parse_stream_list - depends on decode_responses | 🔲 TODO |
| 223 | `xread` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Base: parse_xread / RESP3: parse_xread_resp3 | 🔲 TODO |
| 224 | `xreadgroup` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Base: parse_xread / RESP3: parse_xread_resp3 | 🔲 TODO |
| 225 | `xrevrange` | `ResponseT` | `list[tuple[bytes \| str, dict]]` | `Awaitable[...]` | ✅ Base: parse_stream_list - depends on decode_responses | 🔲 TODO |
| 226 | `xtrim` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |

### SortedSetCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 227 | `zadd` | `ResponseT` | `int \| float \| None` | `Awaitable[int \| float \| None]` | ✅ RESP2: parse_zadd / RESP3: no callback - int or float with INCR | 🔲 TODO |
| 228 | `zcard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 229 | `zcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 230 | `zdiff` | `ResponseT` | `list[bytes \| str] \| list[tuple[bytes \| str, float]]` | `Awaitable[...]` | ✅ RESP2: zset_score_pairs / RESP3: no callback | 🔲 TODO |
| 231 | `zdiffstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 232 | `zincrby` | `ResponseT` | `float` | `Awaitable[float]` | ✅ RESP2: float_or_none / RESP3: no callback (returns float directly) | 🔲 TODO |
| 233 | `zinter` | `ResponseT` | `list[bytes \| str] \| list[tuple[bytes \| str, float]]` | `Awaitable[...]` | ✅ RESP2: zset_score_pairs / RESP3: identity | 🔲 TODO |
| 234 | `zinterstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 235 | `zintercard` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 236 | `zlexcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 237 | `zpopmax` | `ResponseT` | `list[tuple[bytes \| str, float]]` | `Awaitable[list[tuple[bytes \| str, float]]]` | ✅ RESP2: zset_score_pairs / RESP3: identity | 🔲 TODO |
| 238 | `zpopmin` | `ResponseT` | `list[tuple[bytes \| str, float]]` | `Awaitable[list[tuple[bytes \| str, float]]]` | ✅ RESP2: zset_score_pairs / RESP3: identity | 🔲 TODO |
| 239 | `zrandmember` | `ResponseT` | `bytes \| str \| list[bytes \| str] \| None` | `Awaitable[...]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 240 | `bzpopmax` | `ResponseT` | `tuple[bytes \| str, bytes \| str, float] \| None` | `Awaitable[...]` | ✅ RESP2: lambda (tuple with float) / RESP3: no callback | 🔲 TODO |
| 241 | `bzpopmin` | `ResponseT` | `tuple[bytes \| str, bytes \| str, float] \| None` | `Awaitable[...]` | ✅ RESP2: lambda (tuple with float) / RESP3: no callback | 🔲 TODO |
| 242 | `zmpop` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Array reply - no callback | 🔲 TODO |
| 243 | `bzmpop` | `ResponseT` | `list \| None` | `Awaitable[list \| None]` | ✅ Array reply or None - no callback | 🔲 TODO |
| 244 | `zrange` | `ResponseT` | `list[bytes \| str] \| list[tuple[bytes \| str, float]]` | `Awaitable[...]` | ✅ RESP2: zset_score_pairs / RESP3: zset_score_pairs_resp3 | 🔲 TODO |
| 245 | `zrevrange` | `ResponseT` | `list[bytes \| str] \| list[tuple[bytes \| str, float]]` | `Awaitable[...]` | ✅ RESP2: zset_score_pairs / RESP3: zset_score_pairs_resp3 | 🔲 TODO |
| 246 | `zrangestore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 247 | `zrangebylex` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 248 | `zrevrangebylex` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 249 | `zrangebyscore` | `ResponseT` | `list[bytes \| str] \| list[tuple[bytes \| str, float]]` | `Awaitable[...]` | ✅ RESP2: zset_score_pairs / RESP3: zset_score_pairs_resp3 | 🔲 TODO |
| 250 | `zrevrangebyscore` | `ResponseT` | `list[bytes \| str] \| list[tuple[bytes \| str, float]]` | `Awaitable[...]` | ✅ RESP2: zset_score_pairs / RESP3: zset_score_pairs_resp3 | 🔲 TODO |
| 251 | `zrank` | `ResponseT` | `int \| tuple[int, float] \| None` | `Awaitable[...]` | ✅ RESP2: zset_score_for_rank / RESP3: zset_score_for_rank_resp3 | 🔲 TODO |
| 252 | `zrem` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 253 | `zremrangebylex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 254 | `zremrangebyrank` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 255 | `zremrangebyscore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 256 | `zrevrank` | `ResponseT` | `int \| tuple[int, float] \| None` | `Awaitable[...]` | ✅ RESP2: zset_score_for_rank / RESP3: zset_score_for_rank_resp3 | 🔲 TODO |
| 257 | `zscore` | `ResponseT` | `float \| None` | `Awaitable[float \| None]` | ✅ RESP2: float_or_none / RESP3: no callback (returns float directly) | 🔲 TODO |
| 258 | `zunion` | `ResponseT` | `list[bytes \| str] \| list[tuple[bytes \| str, float]]` | `Awaitable[...]` | ✅ RESP2: zset_score_pairs / RESP3: zset_score_pairs_resp3 | 🔲 TODO |
| 259 | `zunionstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 260 | `zmscore` | `ResponseT` | `list[float \| None]` | `Awaitable[list[float \| None]]` | ✅ RESP2: parse_zmscore / RESP3: no callback | 🔲 TODO |

### HyperlogCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 261 | `pfadd` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply (0 or 1) - no callback | 🔲 TODO |
| 262 | `pfcount` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 263 | `pfmerge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |

### HashCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 264 | `hdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 265 | `hexists` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | 🔲 TODO |
| 266 | `hget` | `ResponseT` | `bytes \| str \| None` | `Awaitable[bytes \| str \| None]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 267 | `hgetall` | `ResponseT` | `dict[bytes \| str, bytes \| str]` | `Awaitable[dict[bytes \| str, bytes \| str]]` | ✅ RESP2: pairs_to_dict / RESP3: identity | 🔲 TODO |
| 268 | `hgetdel` | `ResponseT` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 269 | `hgetex` | `ResponseT` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 270 | `hincrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 271 | `hincrbyfloat` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Base: float | 🔲 TODO |
| 272 | `hkeys` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 273 | `hlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 274 | `hset` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 275 | `hsetex` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 276 | `hsetnx` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply (0 or 1) - no callback | 🔲 TODO |
| 277 | `hmset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool | 🔲 TODO |
| 278 | `hmget` | `ResponseT` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 279 | `hvals` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 280 | `hstrlen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 281 | `hexpire` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers - no callback | 🔲 TODO |
| 282 | `hpexpire` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers - no callback | 🔲 TODO |
| 283 | `hexpireat` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers - no callback | 🔲 TODO |
| 284 | `hpexpireat` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers - no callback | 🔲 TODO |
| 285 | `hpersist` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers - no callback | 🔲 TODO |
| 286 | `hexpiretime` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers - no callback | 🔲 TODO |
| 287 | `hpexpiretime` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers - no callback | 🔲 TODO |
| 288 | `httl` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers - no callback | 🔲 TODO |
| 289 | `hpttl` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Array of integers - no callback | 🔲 TODO |

### PubSubCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 290 | `publish` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 291 | `spublish` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 292 | `pubsub_channels` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 293 | `pubsub_shardchannels` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 294 | `pubsub_numpat` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 295 | `pubsub_numsub` | `ResponseT` | `list[tuple[bytes \| str, int]]` | `Awaitable[...]` | ✅ Base: parse_pubsub_numsub - depends on decode_responses | 🔲 TODO |
| 296 | `pubsub_shardnumsub` | `ResponseT` | `list[tuple[bytes \| str, int]]` | `Awaitable[...]` | ✅ Base: parse_pubsub_numsub - depends on decode_responses | 🔲 TODO |

### ScriptCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 297 | `eval` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ No callback - return depends on script | 🔲 TODO |
| 298 | `eval_ro` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ No callback - return depends on script | 🔲 TODO |
| 299 | `evalsha` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ No callback - return depends on script | 🔲 TODO |
| 300 | `evalsha_ro` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ No callback - return depends on script | 🔲 TODO |
| 301 | `script_exists` | `ResponseT` | `list[bool]` | `Awaitable[list[bool]]` | ✅ Base: lambda (list(map(bool, r))) | 🔲 TODO |
| 302 | `script_debug` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 303 | `script_flush` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 304 | `script_kill` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 305 | `script_load` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Base: str_if_bytes - always str | 🔲 TODO |
| 306 | `register_script` | `Script` | `Script` | `AsyncScript` | ⚠️ Returns different class | ⏭️ N/A |

### GeoCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 307 | `geoadd` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 308 | `geodist` | `ResponseT` | `float \| None` | `Awaitable[float \| None]` | ✅ Base: float_or_none | 🔲 TODO |
| 309 | `geohash` | `ResponseT` | `list[str]` | `Awaitable[list[str]]` | ✅ RESP2: lambda (str_if_bytes) / RESP3: no callback | 🔲 TODO |
| 310 | `geopos` | `ResponseT` | `list[tuple[float, float] \| None]` | `Awaitable[...]` | ✅ RESP2: lambda (float tuples) / RESP3: no callback | 🔲 TODO |
| 311 | `georadius` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Base: parse_geosearch_generic - depends on decode_responses | 🔲 TODO |
| 312 | `georadiusbymember` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Base: parse_geosearch_generic - depends on decode_responses | 🔲 TODO |
| 313 | `geosearch` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Base: parse_geosearch_generic - depends on decode_responses | 🔲 TODO |
| 314 | `geosearchstore` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |

### ModuleCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 315 | `module_load` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool (MODULE LOAD) | 🔲 TODO |
| 316 | `module_loadex` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Returns OK - no specific callback | 🔲 TODO |
| 317 | `module_unload` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool (MODULE UNLOAD) | 🔲 TODO |
| 318 | `module_list` | `ResponseT` | `list[dict]` | `Awaitable[list[dict]]` | ✅ RESP2: lambda (pairs_to_dict) / RESP3: no callback | 🔲 TODO |
| 319 | `command_info` | `None` | `None` | `None` | ⚠️ Separate Async | ⏭️ N/A |
| 320 | `command_count` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 321 | `command_getkeys` | `ResponseT` | `list[str]` (RESP2) / `list[bytes \| str]` (RESP3) | `Awaitable[...]` | ✅ RESP2: lambda (str_if_bytes) / RESP3: no callback | 🔲 TODO |
| 322 | `command` | *(none)* | `dict` | `Awaitable[dict]` | ✅ Base: parse_command / RESP3: parse_command_resp3 | 🔲 TODO |

### ClusterCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 323 | `cluster` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ Generic cluster command - varies | 🔲 TODO |
| 324 | `readwrite` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 325 | `readonly` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |

### FunctionCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 326 | `function_load` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 327 | `function_delete` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 328 | `function_flush` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 329 | `function_list` | `ResponseT` | `list` | `Awaitable[list]` | ✅ No specific callback - custom parsing | 🔲 TODO |
| 330 | `fcall` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ No callback - return depends on function | 🔲 TODO |
| 331 | `fcall_ro` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ No callback - return depends on function | 🔲 TODO |
| 332 | `function_dump` | `ResponseT` | `bytes` | `Awaitable[bytes]` | ✅ No callback - binary dump | 🔲 TODO |
| 333 | `function_restore` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 334 | `function_kill` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Returns OK - no specific callback | 🔲 TODO |
| 335 | `function_stats` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ No specific callback - custom parsing | 🔲 TODO |

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
| 345 | `cluster_myid` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 346 | `cluster_addslots` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 347 | `cluster_addslotsrange` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 348 | `cluster_countkeysinslot` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 349 | `cluster_count_failure_report` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 350 | `cluster_delslots` | `list[bool]` | `list[bool]` | `Awaitable[list[bool]]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 351 | `cluster_delslotsrange` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 352 | `cluster_failover` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 353 | `cluster_info` | `ResponseT` | `dict[str, str]` | `Awaitable[dict[str, str]]` | ✅ Base: parse_cluster_info - str keys | 🔲 TODO |
| 354 | `cluster_keyslot` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply - no callback | 🔲 TODO |
| 355 | `cluster_meet` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 356 | `cluster_nodes` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Base: parse_cluster_nodes - returns str | 🔲 TODO |
| 357 | `cluster_replicate` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 358 | `cluster_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 359 | `cluster_save_config` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 360 | `cluster_get_keys_in_slot` | `ResponseT` | `list[str]` (RESP2) / `list[bytes \| str]` (RESP3) | `Awaitable[...]` | ✅ RESP2: lambda (str_if_bytes) / RESP3: no callback | 🔲 TODO |
| 361 | `cluster_set_config_epoch` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 362 | `cluster_setslot` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 363 | `cluster_setslot_stable` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 364 | `cluster_replicas` | `ResponseT` | `str` | `Awaitable[str]` | ✅ Base: parse_cluster_nodes - returns str | 🔲 TODO |
| 365 | `cluster_slots` | `ResponseT` | `list` | `Awaitable[list]` | ✅ No callback - complex structure | 🔲 TODO |
| 366 | `cluster_shards` | `ResponseT` | `list` | `Awaitable[list]` | ✅ No callback - complex structure | 🔲 TODO |
| 367 | `cluster_myshardid` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 368 | `cluster_links` | `ResponseT` | `list` | `Awaitable[list]` | ✅ No callback - complex structure | 🔲 TODO |
| 369 | `cluster_flushslots` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Returns OK - no specific callback | 🔲 TODO |
| 370 | `cluster_bumpepoch` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 371 | `client_tracking_on` | `Union[Awaitable[bool], bool]` | `bool` | `Awaitable[bool]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 372 | `client_tracking_off` | `Union[Awaitable[bool], bool]` | `bool` | `Awaitable[bool]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 373 | `hotkeys_start` | `Union[Awaitable[...], ...]` | `bytes \| str` | `Awaitable[bytes \| str]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 374 | `hotkeys_stop` | `Union[Awaitable[...], ...]` | `bytes \| str` | `Awaitable[bytes \| str]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 375 | `hotkeys_reset` | `Union[Awaitable[...], ...]` | `bytes \| str` | `Awaitable[bytes \| str]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 376 | `hotkeys_get` | `Union[Awaitable[...], ...]` | `list[dict]` | `Awaitable[list[dict]]` | ⚠️ Separate Async impl | ⏭️ N/A |

### ClusterDataAccessCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 377 | `stralgo` | `ResponseT` | `str \| int \| dict[str, str]` | `Awaitable[...]` | ✅ RESP2: parse_stralgo / RESP3: lambda (str keys/values) | 🔲 TODO |
| 378 | `scan_iter` | `Iterator` | `Iterator[bytes \| str]` | `AsyncIterator[bytes \| str]` | 🔄 Iterator - separate impl | ⏭️ N/A |

---

## Sentinel Commands (`redis/commands/sentinel.py`)

### SentinelCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 379 | `sentinel` | `ResponseT` | `Any` | `Awaitable[Any]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 380 | `sentinel_get_master_addr_by_name` | `ResponseT` | `tuple[str, int] \| None` | `Awaitable[tuple[str, int] \| None]` | ✅ Base: parse_sentinel_get_master - str + int | 🔲 TODO |
| 381 | `sentinel_master` | `ResponseT` | `dict[str, Any]` | `Awaitable[dict[str, Any]]` | ✅ RESP2: parse_sentinel_master / RESP3: parse_sentinel_state_resp3 | 🔲 TODO |
| 382 | `sentinel_masters` | `ResponseT` | `dict[str, dict] \| list[dict]` | `Awaitable[...]` | ✅ RESP2: parse_sentinel_masters / RESP3: parse_sentinel_masters_resp3 | 🔲 TODO |
| 383 | `sentinel_monitor` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 384 | `sentinel_remove` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 385 | `sentinel_sentinels` | `ResponseT` | `list[dict[str, Any]]` | `Awaitable[list[dict[str, Any]]]` | ✅ RESP2: parse_sentinel_slaves_and_sentinels / RESP3: parse_sentinel_slaves_and_sentinels_resp3 | 🔲 TODO |
| 386 | `sentinel_set` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 387 | `sentinel_slaves` | `ResponseT` | `list[dict[str, Any]]` | `Awaitable[list[dict[str, Any]]]` | ✅ RESP2: parse_sentinel_slaves_and_sentinels / RESP3: parse_sentinel_slaves_and_sentinels_resp3 | 🔲 TODO |
| 388 | `sentinel_reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 389 | `sentinel_failover` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 390 | `sentinel_ckquorum` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |
| 391 | `sentinel_flushconfig` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Base: bool_ok | 🔲 TODO |

---

## Search Commands (`redis/commands/search/commands.py`)

### SearchCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 392 | `batch_indexer` | `BatchIndexer` | `BatchIndexer` | `BatchIndexer` | 📋 Explicit | 🔲 TODO |
| 393 | `create_index` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Returns OK - module command | 🔲 TODO |
| 394 | `alter_schema_add` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Returns OK - module command | 🔲 TODO |
| 395 | `dropindex` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Returns OK - module command | 🔲 TODO |
| 396 | `add_document` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Returns OK - module command | 🔲 TODO |
| 397 | `add_document_hash` | *(none)* | `bool` | `Awaitable[bool]` | ✅ Returns OK - module command | 🔲 TODO |
| 398 | `delete_document` | *(none)* | `int` | `Awaitable[int]` | ✅ Integer reply | 🔲 TODO |
| 399 | `load_document` | `Document` | `Document` | `Awaitable[Document]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 400 | `get` | *(none)* | `Document \| None` | `Awaitable[Document \| None]` | ✅ Module-specific Document type | 🔲 TODO |
| 401 | `info` | *(none)* | `dict` | `Awaitable[dict]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 402 | `get_params_args` | `list` | `list` | `list` | 📋 Explicit (helper) | ⏭️ N/A |
| 403 | `search` | `Result` | `Result` | `Awaitable[Result]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 404 | `hybrid_search` | `HybridResult` | `HybridResult` | `Awaitable[HybridResult]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 405 | `explain` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 406 | `explain_cli` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 407 | `aggregate` | `AggregateResult` | `AggregateResult` | `Awaitable[AggregateResult]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 408 | `profile` | *(none)* | `tuple` | `Awaitable[tuple]` | ✅ Module-specific structure | 🔲 TODO |
| 409 | `spellcheck` | *(none)* | `dict` | `Awaitable[dict]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 410 | `dict_add` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply | 🔲 TODO |
| 411 | `dict_del` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply | 🔲 TODO |
| 412 | `dict_dump` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 413 | `config_set` | *(none)* | `bool` | `Awaitable[bool]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 414 | `config_get` | *(none)* | `dict` | `Awaitable[dict]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 415 | `tagvals` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |
| 416 | `aliasadd` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Returns OK - module command | 🔲 TODO |
| 417 | `aliasupdate` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Returns OK - module command | 🔲 TODO |
| 418 | `aliasdel` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Returns OK - module command | 🔲 TODO |
| 419 | `sugadd` | `int` | `int` | `Awaitable[int]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 420 | `suglen` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply | 🔲 TODO |
| 421 | `sugdel` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Integer reply | 🔲 TODO |
| 422 | `sugget` | *(none)* | `list` | `Awaitable[list]` | ⚠️ Separate Async impl | ⏭️ N/A |
| 423 | `synupdate` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Returns OK - module command | 🔲 TODO |
| 424 | `syndump` | `ResponseT` | `dict[bytes \| str, list]` | `Awaitable[dict[bytes \| str, list]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |

---

## JSON Commands (`redis/commands/json/commands.py`)

### JSONCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 425 | `arrappend` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Module command - int array | 🔲 TODO |
| 426 | `arrindex` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Module command - int array | 🔲 TODO |
| 427 | `arrinsert` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Module command - int array | 🔲 TODO |
| 428 | `arrlen` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Module command - int array | 🔲 TODO |
| 429 | `arrpop` | `ResponseT` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ Module command - depends on decode_responses | 🔲 TODO |
| 430 | `arrtrim` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Module command - int array | 🔲 TODO |
| 431 | `type` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ Module command - depends on decode_responses | 🔲 TODO |
| 432 | `resp` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Module command - JSON structure | 🔲 TODO |
| 433 | `objkeys` | `ResponseT` | `list[list[bytes \| str] \| None]` | `Awaitable[list[list[bytes \| str] \| None]]` | ✅ Module command - depends on decode_responses | 🔲 TODO |
| 434 | `objlen` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Module command - int array | 🔲 TODO |
| 435 | `numincrby` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ Module command - depends on decode_responses | 🔲 TODO |
| 436 | `nummultby` | `ResponseT` | `bytes \| str` | `Awaitable[bytes \| str]` | ✅ Module command - depends on decode_responses | 🔲 TODO |
| 437 | `clear` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - int reply | 🔲 TODO |
| 438 | `delete` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - int reply | 🔲 TODO |
| 439 | `get` | `ResponseT` | `Any` | `Awaitable[Any]` | ✅ Module command - JSON parsed | 🔲 TODO |
| 440 | `mget` | `ResponseT` | `list[Any]` | `Awaitable[list[Any]]` | ✅ Module command - JSON parsed | 🔲 TODO |
| 441 | `set` | `ResponseT` | `bool \| None` | `Awaitable[bool \| None]` | ✅ Module command - OK or None | 🔲 TODO |
| 442 | `mset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 443 | `merge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 444 | `set_file` | `Optional[str]` | `bool \| None` | `Awaitable[bool \| None]` | 📋 Explicit | 🔲 TODO |
| 445 | `set_path` | `dict[str, bool]` | `dict[str, bool]` | `Awaitable[dict[str, bool]]` | 📋 Explicit | 🔲 TODO |
| 446 | `strlen` | `ResponseT` | `list[int \| None]` | `Awaitable[list[int \| None]]` | ✅ Module command - int array | 🔲 TODO |
| 447 | `toggle` | `ResponseT` | `bool \| list[bool]` | `Awaitable[bool \| list[bool]]` | ✅ Module command - bool | 🔲 TODO |
| 448 | `strappend` | `ResponseT` | `int \| list[int \| None]` | `Awaitable[int \| list[int \| None]]` | ✅ Module command - int | 🔲 TODO |
| 449 | `debug` | `ResponseT` | `int \| list[bytes \| str]` | `Awaitable[int \| list[bytes \| str]]` | ✅ Module command - mixed | 🔲 TODO |
| 450 | `jsonget` | *(none)* | `Any` | `Awaitable[Any]` | ⚠️ Deprecated alias for get | 🔲 TODO |
| 451 | `jsonmget` | *(none)* | `Any` | `Awaitable[Any]` | ⚠️ Deprecated alias for mget | 🔲 TODO |
| 452 | `jsonset` | *(none)* | `Any` | `Awaitable[Any]` | ⚠️ Deprecated alias for set | 🔲 TODO |

---

## TimeSeries Commands (`redis/commands/timeseries/commands.py`)

### TimeSeriesCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 453 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 454 | `alter` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 455 | `add` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - timestamp | 🔲 TODO |
| 456 | `madd` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - timestamps | 🔲 TODO |
| 457 | `incrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - timestamp | 🔲 TODO |
| 458 | `decrby` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - timestamp | 🔲 TODO |
| 459 | `delete` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - count | 🔲 TODO |
| 460 | `createrule` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 461 | `deleterule` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 462 | `range` | `ResponseT` | `list[tuple[int, float]]` | `Awaitable[list[tuple[int, float]]]` | ✅ Module command - parsed samples | 🔲 TODO |
| 463 | `revrange` | `ResponseT` | `list[tuple[int, float]]` | `Awaitable[list[tuple[int, float]]]` | ✅ Module command - parsed samples | 🔲 TODO |
| 464 | `mrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Module command - complex structure | 🔲 TODO |
| 465 | `mrevrange` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Module command - complex structure | 🔲 TODO |
| 466 | `get` | `ResponseT` | `tuple[int, float] \| list` | `Awaitable[tuple[int, float] \| list]` | ✅ Module command - parsed sample | 🔲 TODO |
| 467 | `mget` | `ResponseT` | `list` | `Awaitable[list]` | ✅ Module command - complex structure | 🔲 TODO |
| 468 | `info` | `TSInfo` | `TSInfo` | `Awaitable[TSInfo]` | 📋 Explicit | 🔲 TODO |
| 469 | `queryindex` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ No callback - depends on decode_responses | 🔲 TODO |

---

## Bloom Filter Commands (`redis/commands/bf/commands.py`)

### BFCommands (Bloom Filter)

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 470 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 471 | `add` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - 0 or 1 | 🔲 TODO |
| 472 | `madd` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - 0s and 1s | 🔲 TODO |
| 473 | `insert` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - 0s and 1s | 🔲 TODO |
| 474 | `exists` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - 0 or 1 | 🔲 TODO |
| 475 | `mexists` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - 0s and 1s | 🔲 TODO |
| 476 | `scandump` | `ResponseT` | `tuple[int, bytes \| None]` | `Awaitable[tuple[int, bytes \| None]]` | ✅ Module command - cursor + data | 🔲 TODO |
| 477 | `loadchunk` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 478 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Module command - parsed info | 🔲 TODO |
| 479 | `card` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - int | 🔲 TODO |

### CFCommands (Cuckoo Filter)

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 480 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 481 | `add` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - 0 or 1 | 🔲 TODO |
| 482 | `addnx` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - 0 or 1 | 🔲 TODO |
| 483 | `insert` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - 0s and 1s | 🔲 TODO |
| 484 | `insertnx` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - 0s and 1s | 🔲 TODO |
| 485 | `exists` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - 0 or 1 | 🔲 TODO |
| 486 | `mexists` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - 0s and 1s | 🔲 TODO |
| 487 | `delete` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - 0 or 1 | 🔲 TODO |
| 488 | `count` | `ResponseT` | `int` | `Awaitable[int]` | ✅ Module command - int | 🔲 TODO |
| 489 | `scandump` | `ResponseT` | `tuple[int, bytes \| None]` | `Awaitable[tuple[int, bytes \| None]]` | ✅ Module command - cursor + data | 🔲 TODO |
| 490 | `loadchunk` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 491 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Module command - parsed info | 🔲 TODO |

### TOPKCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 492 | `reserve` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 493 | `add` | `ResponseT` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ Module command - depends on decode_responses | 🔲 TODO |
| 494 | `incrby` | `ResponseT` | `list[bytes \| str \| None]` | `Awaitable[list[bytes \| str \| None]]` | ✅ Module command - depends on decode_responses | 🔲 TODO |
| 495 | `query` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - 0s and 1s | 🔲 TODO |
| 496 | `count` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - ints | 🔲 TODO |
| 497 | `list` | `ResponseT` | `list[bytes \| str]` | `Awaitable[list[bytes \| str]]` | ✅ Module command - depends on decode_responses | 🔲 TODO |
| 498 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Module command - parsed info | 🔲 TODO |

### TDigestCommands

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 499 | `create` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 500 | `reset` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 501 | `add` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 502 | `merge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 503 | `min` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Module command - float | 🔲 TODO |
| 504 | `max` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Module command - float | 🔲 TODO |
| 505 | `quantile` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Module command - floats | 🔲 TODO |
| 506 | `cdf` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Module command - floats | 🔲 TODO |
| 507 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Module command - parsed info | 🔲 TODO |
| 508 | `trimmed_mean` | `ResponseT` | `float` | `Awaitable[float]` | ✅ Module command - float | 🔲 TODO |
| 509 | `rank` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - ints | 🔲 TODO |
| 510 | `revrank` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - ints | 🔲 TODO |
| 511 | `byrank` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Module command - floats | 🔲 TODO |
| 512 | `byrevrank` | `ResponseT` | `list[float]` | `Awaitable[list[float]]` | ✅ Module command - floats | 🔲 TODO |

### CMSCommands (Count-Min Sketch)

| # | Method | Defined Return | Assumed Sync | Assumed Async | Status | Implementation |
|---|--------|----------------|--------------|---------------|--------|----------------|
| 513 | `initbydim` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 514 | `initbyprob` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 515 | `incrby` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - ints | 🔲 TODO |
| 516 | `query` | `ResponseT` | `list[int]` | `Awaitable[list[int]]` | ✅ Module command - ints | 🔲 TODO |
| 517 | `merge` | `ResponseT` | `bool` | `Awaitable[bool]` | ✅ Module command - OK | 🔲 TODO |
| 518 | `info` | `ResponseT` | `dict` | `Awaitable[dict]` | ✅ Module command - parsed info | 🔲 TODO |

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
