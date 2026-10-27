# Release documentation report

## Sections written

- Version ownership and version bump rules.
- Changelog preparation, release commit, build, publish, failure recovery, and private-repository token handling.
- Release notes format, all 8 attachments, consumer destinations, and post-publish verification.

## Sources read

- Main-tree `CLAUDE.md`: sections `Релизы`, `Changelog`, and `Релиз и вложения`.
- Main-tree `scripts/release.sh`, read in full.
- Main-tree `Makefile`, `scripts/check_docs.sh`, and `README.md` documentation list.
- Main-tree `.claude/orchestration-facts.md`.

## Checks

| Check | Result |
| --- | --- |
| `bash scripts/check_docs.sh` | `docs check: clean` |
| Cyrillic scan of `docs/releasing.md` | clean |
| `git diff --check` | clean |

No builds or tests were run.
