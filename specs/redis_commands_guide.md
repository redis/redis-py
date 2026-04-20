# Redis commands guide

## Commands API specification

The Redis API is specified in the [Redis documentation](https://redis.io/commands). This is a source of the truth
for all command related information. However, Redis is a living project and new commands are added all the time.

New command might be not yet available in the documentation. In this case the developer needs to create a new
command specification from `.claude/command-specification-template.md` template.

## Files structure

```
redis/
├── commands/            # Base directory
│   ├── module/          # Module commands directory
│   │   ├── commands.py  # Module commands public API
│   │   └── file.py      # Custom helpers
│   │...                 # Other modules   
│   ├── __init__.py      # Module exports
│   ├── cluster.py       # Cluster commands public API 
│   ├── core.py          # Core commands public API
│   ├── helpers.py       # Helpers for commands modules
│   ├── redismodules.py  # Trait for all Redis modules
│   └── sentinel.py      # Sentinel commands public API
└── 
```

## Commands Public API

Commands public API exposed through the different types of clients (Standalone, Cluster, Sentinel), inheriting trait
objects defined in the directories according to the Files structure section above. Each command implementation at the
end calls generic `execute_command(*args, **kwargs)` defined by `CommandProtocol`.

SDK expose sync and async commands API through methods overloading, see `.agent/sync_async_type_hints_overload_guide.md`
for more information.

### Binary representation

Our SDK can be configured with `decode_response=False` option which means that command response will be returned
as bytes, as well as we allow to pass bytes arguments instead of strings. For this purpose, we define a custom types
like `KeyT`, `EncodableT`, etc. in `redis/typing.py`.

## Protocols compatibility

SDK supports two types of Redis wire protocol: RESP2 and RESP3. And aims to provide a compatibility between them
for seamless user experience. However, there are some differences in types that are defined by these protocols.

Because, RESP3 introduce new types that weren't previously supported by RESP2, we're aiming for forward compatibility
and ensure that all new types supported by RESP3 can be used with RESP2. In most cases the semantic of RESP3 can be
easily recognized and converting existing RESP2 response in RESP3 is a matter of parsing strategy.

All new commands should be added with RESP3 in mind and by default should hide protocol incompatibility from user.

To understand how does Redis types defined by RESP2 and RESP3 protocol maps to Python types,
see `redis/_parsers/resp2.py` and `redis/_parsers/resp3.py`.

Parsers are responsible for RESP protocol parsing. However, protocols compatibility is achieved by 2nd layer
parsing as `response_callbacks` defined in `redis/_parsers/helpers.py` for core or `redis/commands/$module/utils.py` 
for module commands and can be extended/updated on client level.

## Arguments definition

Some of the Redis commands may have a complex API structure, so we need to make it user-friendly and apply
some transformation within specific command method. For example, some commands may need to have
a COUNT argument for aggregated types followed by number of arguments and arguments itself. In this case
we hide this complexity from user and instead expose argument as list in public API and transform it
to Redis-friendly format.

```python
def scan(
    self,
    cursor: int = 0,
    count: int | None = None,
    _type: str | None = None,
    **kwargs,
):
    pieces: list = [cursor]
    
    if count is not None:
        pieces.extend([b"COUNT", count])
```

In terms of required and optional arguments we follow the specification and trying to reflect it as close
as possible.

### Testing

Command tests are located in `tests/test_*command_type*.py` and `tests/test_asyncio/test_*command_type*.py` for async
commands. So it's important to indentify command type upfront to resolve correct test file.

We usually provide only integration testing for commands with defined version constraint (if required). It's controlled
by custom annotations `@skip_if_server_version_lt()` and `@skip_if_server_version_gte()` defined in `tests/conftest.py`.
