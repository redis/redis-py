---
description: Audit CLAUDE.md against the current state of the repo and update it directly. Run when wrapping up a feature or after significant structural changes.
---

# Execute: Sync CLAUDE.md with current repo state

Audit `CLAUDE.md` at the repo root and update it to reflect the current state of the codebase. Edit the file directly — do not ask for confirmation first.

## Inputs

- Current `CLAUDE.md` (read it before doing anything else — it's the baseline you're updating against).
- Recent git history. Read `git log --oneline -40` and `git log --diff-filter=A --name-only --since="3 months ago"` to find recently added subsystems.
- Current file/directory layout of the repo, especially `redis/`, `redis/asyncio/`, `redis/commands/`, `redis/_parsers/`, `tests/`, `.github/workflows/`, `.claude/`.

## What to verify

Walk through each section of `CLAUDE.md` and check:

### 1. Commands section
- Cross-reference every `invoke` task mentioned against `tasks.py`. Catch wrong defaults, renamed flags, removed/added tasks, changed conftest options.
- Cross-reference pytest options against the `pytest_addoption` calls in `tests/conftest.py`.
- Cross-reference the pytest marker list against `[tool.pytest.ini_options].markers` in `pyproject.toml`.

### 2. Architecture section
- Verify each subpackage list matches what's actually in the directory (e.g., the modules under `redis/commands/`, `redis/_parsers/`, `redis/multidb/`, `redis/asyncio/`, `redis/auth/`, `redis/observability/`, `redis/http/`).
- Check whether any new top-level subsystem under `redis/` was added since the CLAUDE.md was last touched and is not mentioned. Use `git log --diff-filter=A --name-only` scoped to `redis/` to find newly added files.
- Verify any specific symbols I reference (e.g., `READ_COMMANDS`, `RequestPolicy`, `ResponsePolicy`, `CommandsParser`, `ExponentialWithJitterBackoff`) still exist — grep for them.
- Confirm sync/async mirror claim: spot-check that the async equivalents listed exist under `redis/asyncio/`.

### 3. Python/dependency notes
- Cross-reference `requires-python`, `[project.optional-dependencies]`, and ruff config in `pyproject.toml`.

### 4. Pointers to other docs
- Verify referenced files still exist.

## What to add (only if genuinely missing)

If a new subsystem, workflow, or developer-facing convention was introduced and is non-obvious from reading the directory tree, add a short note. Examples of things worth adding:

- A new top-level package under `redis/` with its own architectural role.
- A new `invoke` task or a meaningful change to an existing one.
- A new pytest marker or a new convention about how tests are organized.
- A newly added `.claude/commands/` skill that future Claude instances should know about.
- A new spec file under `specs/` that documents a load-bearing design decision.

## What NOT to add

- Per-file descriptions or anything discoverable by `ls`. CLAUDE.md is an orienting document, not a directory listing.
- Generic Python or git advice.
- Recent bugfixes or commit-level changes. CLAUDE.md describes the repo, not its history.
- Anything obvious from reading `README.md` or `CONTRIBUTING.md`.

## Output

1. Apply edits directly to `CLAUDE.md` using the `Edit` tool. Prefer small, targeted edits over rewrites.
2. After editing, print a short summary of what changed and why (under 200 words). Group by section: Commands / Architecture / Other.
3. If nothing needs to change, say so explicitly — do not edit the file just to look productive.
