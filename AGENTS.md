# ✅ Instructions for AI agents contributing to redis-py

This file is the single source of guidance for AI agents working in this repository. It combines project reference material (commands, architecture, dependency notes) with the contributor process rules you must follow.

## Common commands

Development tasks are driven by `invoke` (see `tasks.py`). Activate `.venv` first.

- `invoke devenv` — start the docker-compose test environment (standalone, replica, sentinel, cluster, redis-stack, stunnel). Profiles in `docker-compose.yml` control which containers come up; `--profile all` is the default.
- `invoke clean` — stop all dockers and remove `build/` and `dist/`.
- `invoke tests` — run `fixed_client`, `standalone`, then `cluster` test suites in sequence. Accepts `--uvloop`, `--protocol=2|3|""`, `--legacy-responses=true|false`, `--profile`.
- `invoke standalone-tests` / `invoke cluster-tests` — run just one suite. Cluster suite targets `redis://localhost:16379/0` (`rediss://localhost:27379/0` for TLS).
- `invoke fixed-client-tests` — runs only tests marked `fixed_client` (tests that pin client config and must not be reconfigured by the global `--protocol` / `--legacy-responses` knobs).
- `invoke run-test-matrix` — full `protocol` × `legacy_responses` matrix; what CI does.
- `invoke linters` / `invoke linters-fix` — `ruff check`, `ruff format`, and `vulture redis whitelist.py --min-confidence 80` (use `whitelist.py` to suppress vulture false positives).
- `invoke all-tests` — linters + tests.
- `invoke build-docs` — sphinx HTML under `docs/`.

Running a single test directly with pytest:

```
pytest tests/test_commands.py::TestRedisCommands::test_set -x
pytest tests/test_asyncio/test_commands.py -k "test_set and not cluster"
pytest --redis-url=redis://localhost:16379/0 -m onlycluster tests/test_cluster.py
```

The pytest plugin in `tests/conftest.py` adds these options (also exposed via invoke wrappers): `--redis-url`, `--redis-ssl-url`, `--redis-mod-url`, `--protocol` (`""` = library default, currently RESP3), `--legacy-responses` (`true|false|default`), `--redis-cluster-nodes`, `--uvloop`.

### pytest markers

Defined in `pyproject.toml` and used to scope test runs:

- `onlycluster` / `onlynoncluster` — limit a test to one topology.
- `redismod` — requires Redis modules (Stack image); skipped against cluster.
- `fixed_client` — test fixes its own client/config; excluded from the protocol/legacy matrix runs.
- `experimental`, `replica`, `ssl`, `pipeline`, `cp_integration`, `no_mock_connections`.

`tests/test_scenario/` and `tests/test_asyncio/test_scenario/` are scenario tests that require external infrastructure (Redis Enterprise, EntraID, etc.); they are excluded from the default `standalone-tests` / `cluster-tests` runs via `--ignore`.

## Architecture

### Sync and async mirrors

The library ships two parallel client stacks that must stay in lockstep:

- Sync top-level: `redis/client.py`, `redis/cluster.py`, `redis/connection.py`, `redis/sentinel.py`, `redis/lock.py`, `redis/retry.py`.
- Async mirrors under `redis/asyncio/`: `client.py`, `cluster.py`, `connection.py`, `sentinel.py`, etc.

When changing behavior in one stack, the equivalent change almost always belongs in the other. `specs/sync_async_deduplication_analysis.md` documents the deliberate duplication. The process rules under "Sync / Async Consistency" below are mandatory whenever you touch either stack.

### Commands layer (mixins)

Command surface is composed via mixin classes in `redis/commands/`:

- `core.py` — `CoreCommands` / `AsyncCoreCommands` (every standard Redis command).
- `cluster.py` — `RedisClusterCommands` / `AsyncRedisClusterCommands`; also exports `READ_COMMANDS` (the set of commands that can be routed to replicas).
- `sentinel.py` — sentinel-specific commands.
- `redismodules.py` — aggregates module mixins (`bf/` Bloom, `json/`, `search/`, `timeseries/`, `vectorset/`). Each module subpackage owns its own command surface and response parsing.
- `policies.py` — `PolicyResolver` / `StaticPolicyResolver` and the `STATIC_POLICIES` table used by the cluster client to decide routing and aggregation per command. The `RequestPolicy` / `ResponsePolicy` enums themselves live in `redis/_parsers/commands.py` alongside `CommandsParser`.
- `helpers.py` — shared utilities (`list_or_args`, pubsub subscription partitioning, etc.).

`Redis` (sync) and `redis.asyncio.Redis` are assembled by inheriting these mixins plus the connection/pool plumbing. Adding a new command means editing both the sync and async mixin classes and, for cluster routing, possibly `READ_COMMANDS` and the policy resolver. There is a `/add-new-command` skill (`.claude/commands/add-new-command.md`) that follows the project's expected workflow; use the spec template at `.agents/commands/add-new-command/command-specification-template.md`. Before adding commands, also read this file (AGENTS.md) and `.agents/sync_async_type_hints_overload_guide.md` — they document the sync/async type-overload convention that command additions must follow. The `/sync-claude-md` skill audits and refreshes this file when the project structure changes.

### Wire protocol and response parsing

`redis/_parsers/` handles RESP framing and response shaping:

- `resp2.py`, `resp3.py` — pure-Python parsers.
- `hiredis.py` — optional C-accelerated parser; auto-used if `hiredis>=3.2.0` is installed.
- `response_callbacks.py` — per-command response post-processing (the place where the library reshapes raw protocol output into Python types).
- `commands.py` — `CommandsParser` and policy types used by the cluster client to learn command metadata via `COMMAND INFO`.
- `encoders.py` — request encoding.

RESP3 is now the default on the wire. Two knobs interact:

- `protocol=2|3` chooses the wire protocol.
- `legacy_responses=True|False` chooses the *Python* response shape. `True` (current default) preserves RESP2-era shapes regardless of wire protocol; `False` returns the new unified shapes. The test matrix exercises both axes; see `specs/unified_responses_migration_guide.md`.

### Cluster client

`redis/cluster.py` (`redis/asyncio/cluster.py`) implements topology discovery, slot mapping (`redis/crc.py`, `key_slot`), per-node connection pools, MOVED/ASK redirection, and routing using `RequestPolicy`/`ResponsePolicy` enums from `redis/_parsers/commands.py` resolved via `redis/commands/policies.py`. `READ_COMMANDS` controls replica-eligibility for read routing.

### Multi-database (Active-Active) client

`redis/multidb/` (and `redis/asyncio/multidb/`) implements a client that fronts multiple Redis deployments with health checks, failure detection, and failover. Key modules: `client.py`, `command_executor.py`, `database.py`, `failover.py`, `failure_detector.py`, `config.py`, plus `circuit.py` (pybreaker) and `exception.py` on the sync side and `healthcheck.py` on the async side. Mirror sync/async changes here too.

### Cross-cutting subsystems

- `redis/auth/` — credential providers and token-based auth (EntraID via `redis-entraid`; JWT support).
- `redis/cache.py` — client-side caching configuration.
- `redis/maint_notifications.py` — server-pushed maintenance notifications and the handler that reacts to them.
- `redis/keyspace_notifications.py` — keyspace/keyevent subscription helpers (sync and async variants live side by side).
- `redis/observability/` (and `redis/asyncio/observability/`) — OpenTelemetry instrumentation. Modules: `attributes.py` (span/metric attribute keys), `config.py`, `metrics.py`, `providers.py`, `recorder.py` (the API the rest of the code calls — `record_operation_duration`, `record_error_count`, `record_pubsub_message`, `record_streaming_lag_from_response`), `registry.py`. Optional, gated by the `otel` extra. The async stack has only `recorder.py` and delegates the rest to the sync package.
- `redis/event.py` — `EventDispatcher` used to notify subscribers on connection lifecycle events.
- `redis/http/` — HTTP client used by some auth flows and scenario tests.
- `redis/retry.py`, `redis/backoff.py` — retry/backoff strategy (`ExponentialWithJitterBackoff` is the default).

### Docker test images

`dockers/` contains config for standalone, cluster, sentinel, and redis-stack containers. The compose file pulls `redislabs/client-libs-test:<tag>` images parameterized by `CLIENT_LIBS_TEST_IMAGE_TAG` / `CLIENT_LIBS_TEST_STACK_IMAGE_TAG`. CI pins these via `CURRENT_REDIS_VERSION` in `.github/workflows/integration.yaml`.

## Python and dependency notes

- `requires-python = ">=3.10"`; `pyproject.toml` lists the supported versions.
- Optional extras: `hiredis`, `xxhash`, `ocsp`, `jwt`, `circuit_breaker`, `otel`.
- Ruff is the only formatter/linter (`target-version = "py310"`, `line-length = 88`). `tests/*` has relaxed naming rules; module command packages (`bf`, `timeseries`, `json`, `search`) opt out of pep8 naming.
- `whitelist.py` is the vulture allowlist — extend it instead of silencing vulture inline.

---

## MUST
- Preserve public API compatibility (signatures, argument names, defaults, return types)
- Keep sync and async implementations fully aligned (behavior, signatures, type hints) when possible
- Run the full test suite and fix all failures
- Run linting and fix all issues
- Follow existing code patterns and conventions exactly
- Maintain Redis command semantics and behavior precisely
- Optimize for performance and memory efficiency

## MUST NOT
- Introduce breaking API changes without explicit need
- Modify unrelated code
- Change Redis behavior, edge cases, or error semantics
- Introduce new dependencies without strong justification
- Add unnecessary abstractions or new patterns
- Weaken or remove existing tests to make them pass

## General Principles
- Make minimal, surgical changes
- Prefer consistency over innovation
- If unsure, follow existing implementations in the codebase
- Prioritize correctness first, then performance

## Type Hints & Overloads
- Use PEP 604 (`X | Y`) syntax
- Follow `.agents/sync_async_type_hints_overload_guide.md`
- Keep type hints identical between sync and async APIs
- Do not change public type signatures unnecessarily

## Imports
- Import all types at the top of the file
- Avoid function-level imports unless absolutely necessary
- Keep imports minimal and consistent

## Sync / Async Consistency
- Any change in sync must be mirrored in async (and vice versa)
- Ensure identical signatures, return types, and behavior
- Ensure overloads remain aligned

## Performance & Memory
- Prioritize correctness first, then optimize for performance and memory efficiency
- Never trade correctness for performance
- Avoid unnecessary allocations in hot paths
- Avoid creating temporary objects when not needed
- Reuse existing helpers/utilities instead of duplicating logic
- Be careful with large data structures

## Redis Semantics
- Match Redis server behavior exactly
- Preserve error types and messages
- Do not introduce silent behavior changes
- Handle edge cases explicitly and correctly

## Testing
- Add or update tests for every behavior change
- Cover edge cases and failure scenarios
- Keep tests deterministic and stable
- Do not relax assertions to pass tests

## Linting & Code Quality
- Run all linters and fix issues
- Ensure code is clean, readable, and consistent
- Perform a full code review after changes
- Refactor if needed to meet project standards

## Documentation
- Update docstrings when behavior changes
- Keep docstrings consistent with type hints
- Avoid redundant or outdated documentation

## Dependencies
- Do not introduce new dependencies unless absolutely necessary
- Prefer existing utilities within the project

---

# 🔁 Self-Check Loop (MANDATORY)

Before finalizing changes, perform this loop:

1. **Correctness Check**
   - Does the change preserve Redis semantics exactly?
   - Are edge cases handled correctly?

2. **API Check**
   - Did any public function signature change?
   - If yes, revert or justify explicitly

3. **Sync/Async Check**
   - Are both implementations updated and identical in behavior?
   - Are type hints and overloads aligned?

4. **Performance Check**
   - Did this introduce extra allocations or unnecessary work?
   - Can this be simplified or reused?

5. **Consistency Check**
   - Does this match existing patterns in the codebase?
   - Are naming and structure consistent?

6. **Test Check**
   - Are tests added/updated for the change?
   - Do all tests pass without weakening assertions?

Repeat this loop until all answers are satisfactory.

---

# 🔍 Diff Validation (MANDATORY)

Before submitting changes, review the full diff:

- Ensure only relevant files are modified
- Ensure no unrelated lines were changed
- Ensure no accidental deletions of logic
- Ensure no debug code or comments remain
- Ensure imports were not unnecessarily altered
- Ensure public APIs were not unintentionally modified

If any issue is found:
- Fix it
- Re-run validation

---

## 🔎 Code Review (MANDATORY)

After implementing changes, perform a thorough code review of the final code.

### Verify:

- **Correctness**
  - Logic is correct and complete
  - Edge cases are handled properly
  - Redis semantics are preserved exactly

- **API Stability**
  - No unintended changes to public APIs
  - Function signatures, defaults, and return types are unchanged

- **Sync/Async Parity**
  - Sync and async implementations are fully aligned
  - Type hints and overloads match

- **Performance**
  - No unnecessary allocations or inefficiencies introduced
  - No redundant work or duplicate logic

- **Consistency**
  - Code follows existing patterns and conventions
  - Naming and structure match the rest of the codebase

- **Simplicity**
  - No unnecessary abstractions added
  - Implementation is as simple as possible

- **Tests**
  - Tests cover the change adequately
  - No weakened assertions

### If any issue is found:
- Fix it
- Re-run tests and linting
- Repeat the review

---

# ✅ Final Checklist (must all be true)

- Tests pass
- Lint passes
- Sync/async parity verified
- No API breakage
- No unnecessary allocations introduced
- Behavior matches Redis exactly
- Code follows existing patterns
- Diff is minimal and clean
