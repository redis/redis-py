# Redis-py Type Hints Guide

## Purpose

This document provides comprehensive guidance for:
1. **Identifying correct type hints** for existing redis-py command methods
2. **Implementing new commands** with proper type hints and `@overload` signatures

---

# Section 1: Algorithm for Identifying Correct Type Hints

This section describes how to determine the correct type hint for any Redis command and how to fix incorrect type hints.

## Algorithm for Discovering Correct Type Hints

Follow these steps in order to determine the correct return type for a command:

### Step 1: Check Official Redis Documentation

Visit https://redis.io/commands and look up the command. The documentation specifies return types for both RESP2 and RESP3 protocols. Pay attention to:
- The "Return" section which describes what the command returns
- Any differences between RESP2 and RESP3 return formats
- Whether the return type varies based on command options/flags

### Step 2: Check Local Response Callbacks

The `redis-py` library transforms raw Redis responses using callback functions. Check these callback dictionaries in the following order:

| Location | Dictionary | Purpose |
|----------|------------|---------|
| `redis/_parsers/helpers.py` | `_RedisCallbacks` | **Base callbacks** - shared by both RESP2 and RESP3 |
| `redis/_parsers/helpers.py` | `_RedisCallbacksRESP2` | **RESP2-specific** overrides/additions |
| `redis/_parsers/helpers.py` | `_RedisCallbacksRESP3` | **RESP3-specific** overrides/additions |
| Module `__init__.py` files | `_MODULE_CALLBACKS` | **Module-specific** callbacks for Redis modules |

**Important for Modules**: Redis module commands (Bloom filters, JSON, TimeSeries, Search, etc.) often define their callbacks in their respective `__init__.py` files. For example:
- `redis/commands/bf/__init__.py` - Bloom filter, CMS, TopK, TDigest callbacks
- `redis/commands/json/__init__.py` - JSON module callbacks
- `redis/commands/timeseries/__init__.py` - TimeSeries callbacks
- `redis/commands/search/__init__.py` - Search module callbacks

These module callbacks may also have RESP2 and RESP3 specific variants (e.g., `_RESP2_MODULE_CALLBACKS`, `_RESP3_MODULE_CALLBACKS`).

**If no callback exists** - Return type depends on `decode_responses` setting:
- Bulk string: `bytes | str`
- Integer: `int`
- Array: `list[...]`
- Null: `None`

### Step 3: Check Test Assertions

Examine how commands are tested and what types the responses are asserted against:

| Test Location | Purpose |
|---------------|---------|
| `tests/test_commands.py` | Sync client command tests |
| `tests/test_asyncio/test_commands.py` | Async client command tests |
| `tests/test_bloom.py` | Bloom filter module tests |
| `tests/test_json.py` | JSON module tests |
| `tests/test_timeseries.py` | TimeSeries module tests |
| `tests/test_search.py` | Search module tests |

Look for:
- Type assertions (e.g., `assert isinstance(result, dict)`)
- Value comparisons that reveal expected types
- RESP2 vs RESP3 conditional test logic (often using `is_resp2_connection()`)

---

## How to Fix Incorrect Type Hints

Once you've identified the correct type using the algorithm above, follow these steps to fix the type hints:

### Step 1: Add `@overload` Signatures

Add two `@overload` signatures before the method when the sync and async return types differ:
- One for sync (`self: SyncClientProtocol`) returning the sync type
- One for async (`self: AsyncClientProtocol`) returning `Awaitable[sync_type]`
- If both clients return the same concrete type, keep a single typed method instead of adding redundant overloads

### Step 2: Mirror the Input Signature

Mirror the full input signature exactly in both overloads:
- Same parameter names, order, defaults, positional/keyword shape, `*args`, and `**kwargs` as the implementation
- Use modern annotation style: prefer `X | Y` and `T | None` over `Union[...]` / `Optional[...]`

### Step 3: Keep Original Implementation

Keep the original implementation unchanged except for signature annotation normalization:
- For implementation return unions, prefer the readable order `SyncType | Awaitable[SyncType]`
- When the sync side is itself a union, group it in parentheses before the async branch
  - Example: `(SentinelMastersResponse | bool) | Awaitable[SentinelMastersResponse | bool]`
- Runtime decorators (deprecation, experimental) must stay on the real implementation, not on `@overload` stubs

### Step 4: Run Type Checker

Run `mypy` to verify no type errors were introduced.

---

## Recommended Typing Strategy

For commands with protocol-specific differences, use the **most permissive union type**:
- If RESP2 returns `str` and RESP3 returns `bytes | str`, use `bytes | str`
- This ensures type safety regardless of protocol version

---

# Section 2: Algorithm for New Command Implementation

This section describes how to implement new Redis commands with proper function definitions and type hints.

## Step 1: Research the Command

Before implementing, gather information using the type hint discovery algorithm:
1. Check https://redis.io/commands for the command specification
2. Identify return types for RESP2 and RESP3
3. Understand all command options and how they affect the return type

## Step 2: Define the Function Signature

### Parameter Types

Use appropriate type aliases from `redis/typing.py`:
- `KeyT` - Main redis key space (`bytes | str | memoryview`)
- `FieldT` - Fields within hash tables, streams, geo commands
- `EncodableT` - Any encodable value
- `ExpiryT` - Expiry duration (`int | timedelta`)
- `AbsExpiryT` - Absolute expiry (`int | datetime`)
- `PatternT` - Patterns for matching keys/fields

### Return Type

Determine the correct return type based on:
1. Redis documentation for RESP2/RESP3
2. Whether a response callback will be registered
3. Whether the return varies based on options

## Step 3: Implement the Overload Pattern

Use this template for commands that work with both sync and async clients:

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
        # implementation
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

## Step 4: Register Response Callback (if needed)

If the command requires response transformation:

1. **For core commands**: Add to appropriate dictionary in `redis/_parsers/helpers.py`:
   - `_RedisCallbacks` - For callbacks shared by RESP2 and RESP3
   - `_RedisCallbacksRESP2` - For RESP2-specific callbacks
   - `_RedisCallbacksRESP3` - For RESP3-specific callbacks

2. **For module commands**: Add to the module's `__init__.py`:
   - `_MODULE_CALLBACKS` - Shared callbacks
   - `_RESP2_MODULE_CALLBACKS` - RESP2-specific
   - `_RESP3_MODULE_CALLBACKS` - RESP3-specific

## Step 5: Add Tests

Add tests that verify:
- The command executes correctly
- The return type matches expectations
- Both RESP2 and RESP3 protocols work correctly (if applicable)

---

## Reference: Response Callback System

Understanding the callback system is essential for determining accurate return types.

### Three-Tier Callback Architecture

The `redis-py` library uses three callback dictionaries in `redis/_parsers/helpers.py`:

| Dictionary | Purpose |
|------------|---------|
| `_RedisCallbacks` | **Base callbacks** - shared by both RESP2 and RESP3 |
| `_RedisCallbacksRESP2` | **RESP2-specific** overrides/additions |
| `_RedisCallbacksRESP3` | **RESP3-specific** overrides/additions |

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

