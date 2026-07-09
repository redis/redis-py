# Release Notes Template

This is the canonical structure for a redis-py GitHub release: the label-driven category sections
plus the hand-authored sections maintainers add before publishing. Use it as the skeleton when
assembling a draft. The label→category rules live in `pr-labels-guide.md`; this file owns the section
order, titles, line format, and full skeleton.

## Section order

The canonical sections, in the order they must appear, and the PR labels that route a commit into
each (see `pr-labels-guide.md` for the full classification rules):

| Order | Section title | PR labels that route a commit here |
| --- | --- | --- |
| 1 | `## 🚀 New Features` | `feature`, `enhancement` |
| 2 | `## 🧪 Experimental Features` | `experimental` |
| 3 | `## 🔥 Breaking Changes` | `breakingchange` |
| 4 | `## ⚠️ Deprecations` | `deprecation` |
| 5 | `## 🐛 Bug Fixes` | `fix`, `bugfix`, `bug`, `BUG` |
| 6 | `## 🧰 Maintenance` | `maintenance`, `dependencies`, `documentation`, `docs`, `testing` |

Rules:

- **Omit empty sections.** A category with no commits is not rendered (see the patch releases below).
- **Each line** uses the change template `- $TITLE (#$NUMBER)` — the PR title verbatim, then a space, then `(#<pr-number>)`.
- **Group PRs that deliver the same feature/change onto a single line**, with every related PR number
  in one space-separated bracket group: `- <title> (#3434 #3456 #3467)`. Use the clearest title for
  the combined change (usually the primary PR's). This is common for paired sync/async PRs, a
  feature split across follow-ups, or a fix plus its test/typing follow-up. List the PR numbers in
  ascending order. Only group PRs that are genuinely the same change — do not merge unrelated work to
  shorten the list. If the grouped PRs would land in different categories, place the combined line in
  each matching section (same one-line-per-section rule below).
- **Exclude** any PR labeled `skip-changelog`.
- A commit whose PR carries labels from more than one category is listed under **every matching section** — one line per section (e.g. a PR labeled both `deprecation` and `breakingchange` appears in both ⚠️ Deprecations and 🔥 Breaking Changes). The priority order is used only to infer a single section for a PR that has no category label.
- For any commit/PR with **no category label**, infer the section using `pr-labels-guide.md` before placing it.

## Optional hand-authored sections

These are written by maintainers, not derived from labels. Include them only when the release warrants it.

- `## ✨ Highlights` — include this **only when the release has something significant worth focusing
  the reader on**: a notable new feature, or a breaking change that may affect a large share of users.
  Use `###` subsections with prose describing each highlight, with code/API names and links to
  docs/specs. Routine patch releases have no Highlights.
- A lead prose paragraph immediately under `# Changes` (no heading) summarizing a headline change and
  linking the migration guide. (See the 8.0.0b2 pre-release.)
- Highlights/lead prose go **above** the generated category sections.

## Contributors footer

End with the thank-you line followed by the de-duplicated list of contributor GitHub handles
(`@handle`), space-separated, for everyone whose PR is included:

```
We'd like to thank all the contributors who worked on this release!
@handle1 @handle2 @handle3
```

## Full skeleton

```markdown
# Changes

## ✨ Highlights

### <Notable feature name>

<Prose: what it is, the public API/class/option names, and links to docs or specs. Omit this whole
section for routine patch releases.>

## 🚀 New Features

- <PR title> (#<n>)

## 🧪 Experimental Features

- <PR title> (#<n>)

## 🔥 Breaking Changes

- <PR title> (#<n>)

## ⚠️ Deprecations

- <PR title> (#<n>)

## 🐛 Bug Fixes

- <PR title> (#<n>)

## 🧰 Maintenance

- <PR title> (#<n>)

We'd like to thank all the contributors who worked on this release!
@handle1 @handle2
```

## Worked examples from published releases

### Minor/major release (8.0.0-style)

`# Changes` → `## ✨ Highlights` (several `###` prose subsections covering async cluster pubsub,
keyspace notifications, RESP3-by-default, type-hint overloads, connection/retry defaults) →
category sections → contributors footer.

### Pre-release (8.0.0b2-style)

`# Changes` → lead prose paragraph on the headline breaking change with a migration-guide link →
New Features → Breaking Changes → Maintenance → contributors footer.

### Patch release (8.0.1-style)

`# Changes` → `## 🐛 Bug Fixes` → `## 🧰 Maintenance` → contributors footer. No Highlights, and
empty categories (Breaking, Experimental, New Features) are omitted entirely.
