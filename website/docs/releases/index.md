# Match the guide to your release

| Desktop release                     | App/installer version | Contents                                                                                             |
| ----------------------------------- | --------------------- | ---------------------------------------------------------------------------------------------------- |
| [v0.1.0+build.1](v0.1.0+build.1.md) | 0.1.0                 | First release: Kafka workbench, hot plugin lifecycle, EDA Capture and NSP Capture using plugin API 3 |

The first release was published on **2026-10-01** as an **unsigned prerelease**.
The [v0.1.0+build.1 release](https://github.com/asadarafat/streamskope/releases/tag/v0.1.0%2Bbuild.1)
contains installers for macOS ARM64, Windows x64 and Linux x64, plus `SHA256SUMS`.
The [qualification record](../guide/qualification.md) links the exact automated
checks and identifies the live and installed-app workflows still lacking evidence.

Record the full download release tag as well as the app version. Build metadata
can distinguish releases that share app versions and installer filenames. Match
the installer against `SHA256SUMS` from its own GitHub release. If an existing
installation's build is unknown, preserve a full backup before replacing it.

The notice on each guide identifies its documented desktop build and links to
plugin requirements. **Development documentation** comes from a checkout or PR
and may contain changes not yet published. **Published documentation** is the
qualified site built from current `main`, with release availability verified before
deployment.

Merging documentation changes to `main` qualifies and publishes Pages without
another desktop release. Maintainers can also run **Pages** manually from `main`.
Desktop release publication triggers
the same current-main build; an old release event cannot replace the site with an
old checkout. EDA plugin and signed cluster-app publication remain separate.

Before each later desktop release, retain existing release pages, add the exact
new tag's notes, update the documented release in the website configuration, and
review the [compatibility matrix](../start/compatibility.md) and
[qualification record](../guide/qualification.md). Record qualification only after
inspecting evidence for the exact source and environment.
