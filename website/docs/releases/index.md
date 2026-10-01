# Match the guide to your release

| Desktop release                     | App/installer version | Status and contents                                                                              |
| ----------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------ |
| [v0.2.0](v0.2.0.md)                 | 0.2.0                 | Unreleased: independent plugin SemVer and API 4, with migration from original plugins            |
| [v0.1.0+build.1](v0.1.0+build.1.md) | 0.1.0                 | Published unsigned prerelease: Kafka workbench, hot plugin lifecycle, API 3 EDA and NSP packages |

Use the [installation page](../start/installation.md) for the currently published
installers and checksums. Upcoming source versions do not replace those links
until their GitHub release has been published. A tag alone does not establish
that an installer or plugin is available.

From **0.2.0**, application and installer versions equal the desktop tag without
its `v` prefix. Each plugin has an independent Semantic Version. Record both when
reporting a problem. The original `v0.1.0+build.1` retains its historical build
identity; match its installer against `SHA256SUMS` from that exact release.

The notice on each guide identifies its published desktop baseline. Pages with
upcoming version changes are marked **Unreleased** and identify their source
version. **Development documentation** comes from a checkout or PR.
**Published documentation** is the qualified site built from current `main`, with
published installer availability verified before deployment; it can also contain
clearly labeled upcoming changes.

Merging documentation changes to `main` qualifies and publishes Pages without
another desktop release. Maintainers can also run **Pages** manually from `main`.
Desktop release publication triggers the same current-main build; an old release
event cannot replace the site with an old checkout. Plugin and signed EDA cluster-app
publication are independent.

Before each desktop release, retain historical notes and add notes for the exact
new tag. Review the [compatibility matrix](../start/compatibility.md) and
[qualification record](../guide/qualification.md). After publishing, update the
documented download release and remove the applicable unreleased notices through
a reviewed PR. Record qualification only after inspecting evidence for the exact
source and environment.
