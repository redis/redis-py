# Sync/Async Command Deduplication Analysis for redis-py

## Problem Summary

Based on GitHub issues [#2933](https://github.com/redis/redis-py/issues/2933), [#3169](https://github.com/redis/redis-py/issues/3169), [#2399](https://github.com/redis/redis-py/issues/2399), [#3195](https://github.com/redis/redis-py/issues/3195), and [#2897](https://github.com/redis/redis-py/issues/2897):

### The Core Problem: Type Safety

The current `ResponseT = Union[Awaitable[Any], Any]` return type **breaks static analysis**:

```python
# Current approach - type checker cannot help!
def get(self, key: str) -> ResponseT:  # ResponseT = Union[Awaitable[Any], Any]
    return self.execute_command("GET", key)

# When using async client:
result = await client.get("key")  # IDE/mypy can't verify this needs await
result = client.get("key")         # No warning! Will fail at runtime
```

**What we need:**
- Single source of truth for command logic (no duplication like `search` module)
- Async client methods typed as `-> Awaitable[X]`
- Sync client methods typed as `-> X`
- Static analysis tools correctly understand the types

### What NOT to do (Search Module Anti-Pattern)

The search module duplicates every method:
```python
class SearchCommands:
    def search(self, query) -> Result:
        res = self.execute_command(SEARCH_CMD, *args)
        return self._parse_results(res)

class AsyncSearchCommands(SearchCommands):
    async def search(self, query) -> Result:  # Complete duplication!
        res = await self.execute_command(SEARCH_CMD, *args)
        return self._parse_results(res)
```

---

## Solution Comparison

| Solution | Initial Effort | Maintenance | Type Safety | Breaking Changes | Runtime Performance |
|----------|---------------|-------------|-------------|------------------|---------------------|
| **1. @overload + Self-Type** | Medium | Low | ✅ Full | None | ✅ Zero overhead |
| **2. Type Stub Files (.pyi)** | Medium | Medium | ✅ Full | None | ✅ Zero overhead |
| **3. Generic Protocol Pattern** | Medium-High | Low | ✅ Full | Low | ✅ Zero overhead |
| **4. Metaclass/Decorator Gen** | High | Medium | ⚠️ Partial | None | ⚠️ Import-time cost |

---

## Detailed Solutions

### 1. @overload with Self-Type Discrimination ⭐ RECOMMENDED
**Effort: Medium | Maintenance: Low | Type Safety: Full**

Use `typing.overload` with self-type to give different return types based on which client class is using the mixin.

```python
from typing import TYPE_CHECKING, overload, Protocol

class SyncClient(Protocol):
    def execute_command(self, *args, **kwargs) -> Any: ...

class AsyncClient(Protocol):
    def execute_command(self, *args, **kwargs) -> Awaitable[Any]: ...

class HashCommands:
    @overload
    def hget(self: SyncClient, name: str, key: str) -> bytes | None: ...
    @overload
    def hget(self: AsyncClient, name: str, key: str) -> Awaitable[bytes | None]: ...

    def hget(self, name: str, key: str):
        return self.execute_command("HGET", name, key)
```

**How it works:**
- Type checker sees which protocol `self` matches
- Returns precise type based on sync vs async client
- Single implementation, no duplication
- Zero runtime overhead (overloads are for type checker only)

**Pros:**
- ✅ Full type safety for both sync and async
- ✅ Single method body - no duplication
- ✅ No build tools or code generation
- ✅ IDE autocompletion works correctly
- ✅ Supported by mypy, pyright, and other type checkers

**Cons:**
- Requires adding overload signatures for each method
- Initial effort to add overloads to ~200+ methods
- Can be automated with a script

**Performance Impact:** ✅ **Zero runtime overhead**
- `@overload` decorators are completely ignored at runtime (they're no-ops)
- The actual implementation method is called directly, exactly as it would be without overloads
- No additional function calls, no wrapper functions, no type checking at runtime
- Memory footprint is unchanged - overload signatures are not stored at runtime

---

### 2. Type Stub Files (.pyi)
**Effort: Medium | Maintenance: Medium | Type Safety: Full**

Keep implementation simple, use separate `.pyi` stub files for precise typing.

**Implementation file (`commands/core.py`):**
```python
class HashCommands:
    def hget(self, name, key):
        return self.execute_command("HGET", name, key)
```

**Stub file for sync (`commands/core.pyi` or via overloads):**
```python
class HashCommands:
    def hget(self, name: str, key: str) -> bytes | None: ...
```

**Stub file for async (separate or conditional):**
```python
class AsyncHashCommands:
    def hget(self, name: str, key: str) -> Awaitable[bytes | None]: ...
```

**Pros:**
- ✅ Full type safety
- ✅ Implementation stays clean and simple
- ✅ Type stubs are the standard Python approach

**Cons:**
- Must maintain stubs separately from implementation
- Risk of stubs getting out of sync
- More files to manage

**Performance Impact:** ✅ **Zero runtime overhead**
- `.pyi` stub files are **never loaded at runtime** - they exist only for type checkers
- Python interpreter completely ignores stub files during execution
- No impact on import time, memory usage, or method call performance
- The implementation files remain unchanged and execute exactly as before

---

### 3. Generic Protocol with TypeVar Pattern
**Effort: Medium-High | Maintenance: Low | Type Safety: Full**

Use generics to parameterize the return type wrapper.

```python
from typing import TypeVar, Generic, Callable, Awaitable

T = TypeVar('T')
R = TypeVar('R')  # Return wrapper type

class CommandsMixin(Generic[R]):
    execute_command: Callable[..., R]

    def hget(self, name: str, key: str) -> R:
        return self.execute_command("HGET", name, key)

class Redis(CommandsMixin[bytes | None]):  # Sync: returns direct value
    ...

class AsyncRedis(CommandsMixin[Awaitable[bytes | None]]):  # Async: returns Awaitable
    ...
```

**Limitation:** This simple form doesn't capture the actual return type `T` inside the `Awaitable[T]`. A more sophisticated approach uses higher-kinded types or callback protocols.

**Alternative with Callable:**
```python
from typing import Protocol, TypeVar, Generic

T = TypeVar('T', covariant=True)

class SyncExecutor(Protocol):
    def __call__(self, *args: Any) -> Any: ...

class AsyncExecutor(Protocol):
    def __call__(self, *args: Any) -> Awaitable[Any]: ...
```

**Pros:**
- Conceptually clean
- Type-safe

**Cons:**
- Python's type system doesn't fully support higher-kinded types
- May require complex workarounds
- Less IDE support than overloads

**Performance Impact:** ✅ **Zero runtime overhead**
- Python's generic types use **type erasure** at runtime - `Generic[T]` adds no runtime cost
- `TypeVar` and `Protocol` are purely static constructs, not evaluated during execution
- Method calls go directly to the implementation with no indirection
- No additional memory for type parameters (they don't exist at runtime)

---

### 4. Metaclass/Decorator-Based Generation
**Effort: High | Maintenance: Medium | Type Safety: Partial**

Generate sync/async variants at class creation time.

```python
def command(cmd_name: str, return_type: type):
    def decorator(func):
        func._cmd = cmd_name
        func._return_type = return_type
        return func
    return decorator

class CommandsMeta(type):
    def __new__(mcs, name, bases, namespace, is_async=False):
        for attr_name, attr in namespace.items():
            if hasattr(attr, '_cmd'):
                if is_async:
                    namespace[attr_name] = mcs._make_async(attr)
                else:
                    namespace[attr_name] = mcs._make_sync(attr)
        return super().__new__(mcs, name, bases, namespace)
```

**Pros:**
- Single source of truth
- No external tools

**Cons:**
- Type checkers struggle with metaclass magic
- Harder to debug
- IDE support is poor

**Performance Impact:** ⚠️ **Import-time overhead, potential runtime cost**
- **Import time:** Metaclass `__new__` runs when module is imported, processing all methods
  - For ~200 commands, this could add 10-50ms to import time
  - Decorators are evaluated at class definition time
- **Memory overhead:** Dynamically generated methods may create additional function objects
- **Runtime overhead:** Depends on implementation:
  - If decorators wrap methods: extra function call per command (~50-100ns overhead)
  - If metaclass replaces methods directly: minimal overhead after import
- **Debugging impact:** Stack traces may show generated code, making debugging harder

---

## Recommendation

**Solution #1: @overload with Self-Type Discrimination**

### Why this is the best approach:

1. **Full type safety** - mypy/pyright understand exactly what type each client returns
2. **No duplication** - single method body, overloads are type-only
3. **No magic** - standard Python typing, well-documented pattern
4. **No build tools** - works with existing Python tooling
5. **Incremental adoption** - can migrate method by method

### Implementation Strategy:

1. **Define protocols** for sync and async clients:
```python
class SyncCommandsProtocol(Protocol):
    def execute_command(self, *args, **options) -> Any: ...

class AsyncCommandsProtocol(Protocol):
    def execute_command(self, *args, **options) -> Awaitable[Any]: ...
```

2. **Add overloads** to command methods:
```python
class ACLCommands:
    @overload
    def acl_cat(self: SyncCommandsProtocol, category: str | None = None) -> list[str]: ...
    @overload
    def acl_cat(self: AsyncCommandsProtocol, category: str | None = None) -> Awaitable[list[str]]: ...

    def acl_cat(self, category: str | None = None):
        pieces = [category] if category else []
        return self.execute_command("ACL CAT", *pieces)
```

3. **Automate** - write a script to generate overload signatures from existing method signatures

### Migration Path:
1. Add the protocol definitions
2. Start with high-usage commands (get, set, hget, etc.)
3. Gradually add overloads to remaining ~200 methods
4. Can be done incrementally without breaking changes

