# Notes for AI coding agents

## Comments

- Be concise. A comment should explain _why_ in a line or two, not narrate _what_ the code does.
- No multi-paragraph essays in code or CI config.
- Match the brevity and style of the surrounding code.
- Don't embed issue/PR numbers in inline code comments; they belong in commit messages.

## Changes

- Keep diffs minimal and focused on the task; don't refactor unrelated code.
- Prefer existing tools, helpers, and patterns over introducing new ones.

## This repo specifically

- Extraction logic belongs in `circex` (the PyPI package), not here. This repo is
  the SkyPortal binding: resolving an event to a `dateobs` and writing to the API.
- `import circex` is deliberately lazy inside `pipeline.build_extractor` so the
  module imports for tests without a model server or the archive.
- Every write goes through `SkyPortalClient`, which is dry-run unless `live=True`
  AND a token is set. Never bypass it with a bare `requests.post`.
