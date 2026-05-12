# ✅ Instructions for AI agents contributing to redis-py

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
- Follow `.agent/sync_async_type_hints_overload_guide.md`
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