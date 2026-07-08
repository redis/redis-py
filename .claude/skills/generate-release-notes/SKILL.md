---
name: generate-release-notes
description: Generate a redis-py GitHub release-notes draft and save it as a Markdown file. Collects the merged PRs/commits since the previous release, maps each commit to its PR labels, categorizes them into the project's release sections (Breaking Changes, Deprecations, Experimental Features, New Features, Bug Fixes, Maintenance) using the skill's pr-labels-guide, infers a category for unlabeled PRs, and assembles the notes with an optional Highlights section and a contributors footer. Trigger this skill whenever the user says "release notes", "create release notes", "generate release notes", "draft release notes", "prepare release notes", "changelog", or asks for the release notes/changelog of a specific version or branch (e.g. "release notes for 8.0", "generate release notes for the 8.0 branch", "what changed since the last release"). A phrase like "release notes for 8.0" means: use branch `8.0` as the release-branch input. Also use when asked to summarize what changed since the last tag.
---

The full instructions for this skill live in
`.agents/skills/generate-release-notes/SKILL.md` (relative to the redis-py repo root),
with its helper files under `.agents/skills/generate-release-notes/references/`.

**Read `.agents/skills/generate-release-notes/SKILL.md` in full now and follow it
exactly.** Treat its contents as the body of this skill.
