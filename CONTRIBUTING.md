# Contributing

Keep each PR to one reviewable outcome, with checks that demonstrate its behavior.
StreamSkope uses TypeScript, React and Node, with separate Kafka and NATS providers.

## Start with your task

Use Node **24.21 or newer in the 24.x line**, then run `npm ci` from the repository
root. Additional prerequisites depend on the task: see
[development setup](maintainers/development.md) for fixtures, native builds and
source execution, or [documentation work](maintainers/documentation.md) for docs.

There are five npm entry points:

| Command                 | Purpose                                                                |
| ----------------------- | ---------------------------------------------------------------------- |
| `npm run dev`           | Start the browser development host with owned Kafka and NATS fixtures. |
| `npm run build`         | Check types and build the selected host.                               |
| `npm run check`         | Run core shared checks, the 60-second soak and docs.                   |
| `npm run package`       | Build the selected desktop, browser, plugin or EDA artifact.           |
| `npm run docs -- serve` | Preview the documentation.                                             |

Read the relevant guide before choosing additional arguments. The Linux fixture
launcher, production browser host and installed desktop are different execution
paths with different requirements.

## Choose the checks

| Change or purpose              | Qualification                                                                                                                |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| Focused implementation         | Run its owning tests first: `npx --no-install vitest run --config config/vitest.config.ts PATH`.                             |
| PR acceptance                  | Run focused local checks for the changed behavior and require all GitHub lanes; a duplicate full local run is not mandatory. |
| Core local qualification       | Run `npm run check`; no external EDA or NSP access is needed.                                                                |
| Affected vendor integration    | Run `npm run check -- --live eda`, `--live nsp` or `--live all`; every selected vendor requires configuration.               |
| Integrated local qualification | Run `npm run check -- --full` for core plus configured EDA/NSP; unconfigured vendors are explicitly skipped.                 |
| Complete GitHub scope locally  | Run `npm run check -- --ci` with Docker and Chromium available.                                                              |
| Release acceptance             | Add the native, live and artifact evidence required by the changed capability; source checks alone do not qualify packages.  |

GitHub runs shared, docs and runtime lanes in parallel. All three and the final
**CI** gate must pass; the branch must be up to date before normal protected merge.
Main pushes do not repeat that CI. Additional real-provider, security, native and
soak checks depend on the [impact matrix](maintainers/qualification.md#choose-evidence-by-impact).
Live EDA/NSP checks cover affected integration, authentication, trust,
generated-profile and connection/lock/cleanup behavior, plus milestone or release
integration claims. They are not required for unrelated work. Preserve complete
source-bound receipts and report unexecuted scope without claiming a pass.

## Maintainer guides

- [Development](maintainers/development.md): setup, commands, fixtures and cleanup.
- [Architecture](maintainers/architecture.md): a reading path, feature trace and ownership boundaries.
- [Qualification](maintainers/qualification.md): check selection, live effects, evidence and release acceptance.
- [Releases](maintainers/releases.md): versions, artifacts, signing, publication and recovery.
- [Documentation](maintainers/documentation.md): previews, content checks, media and published-site identity.
- [Dependencies](maintainers/dependencies.md): updates, mitigations and their retirement criteria.

Before changing lifecycle, cancellation, credentials or writes, read the owning
module and its failure-path tests. Preserve admitted receipts and cleanup
ownership. A timeout or disconnected UI does not prove remote work stopped.

## Submit a change

Use a Conventional Commit PR title and the repository PR template. Describe the
problem, resulting behavior, executed checks and material untested scope.
Add a regression for changed behavior; use existing validators for a prose-only
or behavior-neutral change. Keep generated artifacts, local credentials and
private planning files out of commits.

Versions are assigned by manual release CI. Development source retains
`0.0.0-dev`; PR qualification does not publish packages or documentation. User-facing
instructions belong in [the website docs](website/docs/), with maintainer procedures
kept in the guides above.

## Build a desktop package

The procedure is maintained in [native packaging](maintainers/development.md#build-a-desktop-package).

## Release notes

The authoring and archive procedure is maintained in [release notes](maintainers/releases.md#release-notes).
