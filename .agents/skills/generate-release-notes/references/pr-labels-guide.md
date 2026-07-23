# PR Labels Guide for Release Notes

This guide is the **authoritative, self-contained mapping** from a PR's labels (or, when there is no
useful label, its actual change) to a release-notes category. Use it to categorize every commit/PR in
the release range. We do **not** rely on any automated release-drafter output to decide what goes
where or which PRs are included — categorize manually using the rules below.

## Category labels (drive a release-notes section)

These are the only labels that place a commit into a section. When a PR carries labels from more than
one category, **list it under every matching section** — one line per section (e.g. a PR labeled both
`deprecation` and `breakingchange` appears in both ⚠️ Deprecations and 🔥 Breaking Changes). The
priority order in this table is used only to pick a single section when a PR has **no** category
label and you must infer one (see "Inferring a category when there is no label" below).

| Section | Mapped labels | Classify a PR here when it... |
| --- | --- | --- |
| 🚀 New Features | `feature`, `enhancement` | Adds a new Redis command, client capability, public option, or a user-visible enhancement to existing behavior. |
| 🧪 Experimental Features | `experimental` | Adds a preview/experimental capability not yet covered by compatibility guarantees (often also tagged `experimental` in code/tests). |
| 🔥 Breaking Changes | `breakingchange` | Changes or removes any public contract: a function/method signature, argument name, default value, return type, raised exception type, wire-protocol/response shape, or persisted/connection config — anything that can break a user upgrading from the latest released version. |
| ⚠️ Deprecations | `deprecation` | Marks a public API, option, or behavior as deprecated while keeping it working this release (typically emitting a `DeprecationWarning`). If the same PR *also* removes/changes another public contract now, it additionally appears in 🔥 Breaking Changes (list it in both). |
| 🐛 Bug Fixes | `fix`, `bugfix`, `bug`, `BUG` | Corrects incorrect behavior, a crash, a leak, or a regression in supported functionality. |
| 🧰 Maintenance | `maintenance`, `dependencies`, `documentation`, `docs`, `testing` | CI/release tooling, dependency bumps, docs, tests, refactors, type-hint/lint cleanups — no user-facing behavior change. |

## Repo labels that need an explicit decision

These labels exist in the redis-py repo but are **not** one of the category labels above, so a PR
labeled only with one of them does not name its own section. Route them as follows, and add the
proper category label to the PR when possible so the labeling is correct for future reference:

- `bug-fix` — treat as **🐛 Bug Fixes** (synonym of `bug`/`fix`; prefer also adding `bug`).
- `security` — place under **🐛 Bug Fixes** by default; if the fix changes a public contract, it is
  **🔥 Breaking Changes** instead. Strongly consider calling it out in `## ✨ Highlights` with the
  advisory/CVE reference regardless of section.
- `techdebt` — **🧰 Maintenance**.
- `deprecation` — route to the **⚠️ Deprecations** section (see the category table above). If the PR
  removes or changes a public contract *in this release* (not just warns), it belongs in **🔥 Breaking
  Changes** instead.
- Topic/area labels — `async`, `cluster`, `windows`, `redis-7`, `redis-py-5`, `RediSearch`,
  `RedisJSON`, `RedisGraph` — describe *where* a change applies, not *what kind* it is. They never
  select a section; a PR carrying only these still needs a category decision from the rules below.

## Excluded and non-release labels

- `skip-changelog` — **exclude the PR from the release notes entirely.**
- Process/triage labels never appear in release notes and are ignored for categorization:
  `triage`, `question`, `discussion`, `duplicate`, `stale`, `help-wanted`, `good first issue`,
  `need more info`, `needs-information`, `ready-for-merge`, `changes-requested`,
  `waiting-for-response`, `docs-review`, `update-docs`, `run-benchmark`, `hacktoberfest-accepted`,
  `tracking-issue`.

## Inferring a category when there is no label

When a PR/commit has no category label, infer the section from the change itself. Apply **every** rule
that matches — a single change can land in more than one section (e.g. it both deprecates one API and
breaks another, so it appears in Deprecations and Breaking Changes):

1. **New Features** — adds a new Redis command, client/connection capability, or public option, or
   a clearly user-visible enhancement. Title often starts with `feat`/`feature`/`add`. Label:
   `feature`.
2. **Experimental Features** — the change is explicitly a preview/experimental feature (marked
   experimental in code, tests under the `experimental` marker, or stated as preview). Label:
   `experimental`.
3. **Breaking Changes** — the diff changes/removes a public signature, argument name, default,
   return type, raised exception type, or wire-protocol/response shape against the latest release, or
   the PR title/body says "breaking", "remove", "drop support". Suggested label: `breakingchange`.
4. **Deprecations** — the change marks a public API/option/behavior as deprecated but keeps it
   working (e.g. adds a `DeprecationWarning`, a "deprecated" note in a docstring, or a
   `@deprecated` marker) without removing it this release. Suggested label: `deprecation`.
5. **Bug Fixes** — fixes incorrect behavior, a crash, a connection/resource leak, or a regression.
   Title often starts with `fix`. Branch named `bug-*`. Label: `bug`.
6. **Maintenance** — everything else with no user-facing behavior change: touches only `*.md`,
   `.github/*`, `docs/`, `tests/`, `pyproject.toml`/`dev_requirements.txt`/`docker-compose.yml`/
   `dockers/`/`tasks.py`, dependency bumps, refactors, type-hint/lint cleanups, or CI. Branch named
   `maintenance-*`. Label: `maintenance`.

Branch-naming hints that help inference: branch `bug-*` → Bug Fixes; branch `maintenance-*` →
Maintenance; branch `feature-*` → New Features; changes touching only `*.md` or `.github/*` →
Maintenance.

When the right category is genuinely ambiguous, record the uncertainty in the draft (a `<!-- TODO:
confirm category for #N -->` comment) rather than silently guessing, and surface it to the user so
the PR can be labeled correctly.

## sync/async note

A single behavior change is frequently delivered as paired sync/async work, usually in a single PR.
If two separate PRs (one sync, one async) implement the same change, treat them as one change: group
them onto a single line under the same category, with both PR numbers in one bracket group
(`- <title> (#3434 #3456)`), per the grouping rule in `release-notes-template.md`.
