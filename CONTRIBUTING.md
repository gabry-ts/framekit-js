# Contributing to FrameKit

## Local Setup

```bash
npm ci
npm run typecheck
npm test
```

## Required Checks

- `npm test`
- `npm run typecheck`
- `npm run lint`

## Branch and Commit Conventions

- Branch from `main` with focused topic branches.
- Commit message style: `feat:`, `bugfix:`, `hotfix:`, `chore:`, `docs:`.
- Keep commit titles short and scoped to one change.

## Feature Policy

- New features must include unit tests.
- Public API changes must update docs under `docs/reference/`.
- Behavior changes must include migration notes in `CHANGELOG.md`.

## Documentation Policy

- Add or update at least one guide/cookbook/reference entry for user-facing features.
- Keep examples runnable and aligned with current API.

## SemVer and Releases

FrameKit follows Semantic Versioning:

- `MAJOR`: breaking API/behavior changes.
- `MINOR`: backward-compatible features.
- `PATCH`: backward-compatible fixes.

Release notes must include:

- Breaking changes
- Migration notes
- Performance notes
- Known limitations
