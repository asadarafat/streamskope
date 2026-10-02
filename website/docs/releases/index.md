# Match the guide to your release

| Desktop release                     | App/installer version  | Status and contents                                                                              |
| ----------------------------------- | ---------------------- | ------------------------------------------------------------------------------------------------ |
| [Unreleased changes](unreleased.md) | Assigned by release CI | Development: independent plugin SemVer and API 4, with migration from original plugins           |
| [v0.1.0+build.1](v0.1.0+build.1.md) | 0.1.0                  | Published unsigned prerelease: Kafka workbench, hot plugin lifecycle, API 3 EDA and NSP packages |

The first release was published on **2026-10-01** as an **unsigned prerelease**.
The [v0.1.0+build.1 release](https://github.com/asadarafat/streamskope/releases/tag/v0.1.0%2Bbuild.1)
contains installers for macOS ARM64, Windows x64 and Linux x64, plus `SHA256SUMS`.
The [qualification record](../guide/qualification.md) links the exact automated
checks and identifies the live and installed-app workflows still lacking evidence.

Use the [installation page](../start/installation.md) for the currently published
installers and checksums. Unreleased source changes do not replace those links
until their GitHub release has been published. A tag alone does not establish
that an installer or plugin is available.

For new releases, application and installer versions equal the desktop tag
without its `v` prefix. Each plugin has an independent Semantic Version. Record both when
reporting a problem. The original `v0.1.0+build.1` retains its historical build
identity; match its installer against `SHA256SUMS` from that exact release.

The notice on each guide identifies its published desktop baseline. Pages with
upcoming changes are marked **Unreleased**. Their development identity does not
assign a release version. **Development documentation** comes from a checkout or PR.
**Published documentation** is the qualified site built from current `main`, with
published installer availability verified before deployment; it can also contain
clearly labeled upcoming changes.

Merging documentation changes to `main` qualifies and publishes Pages without
another desktop release. Maintainers can also run **Pages** manually from `main`.
Desktop release publication triggers the same current-main build; an old release
event cannot replace the site with an old checkout. Plugin and signed EDA cluster-app
publication are independent.

Maintainers review unversioned notes, then start **Actions → Release → Run workflow**
from `main`, select **desktop** and enter its version. CI qualifies the source,
stamps a build checkout and creates the tagged draft; review it before publishing.
PR and `main` checks only qualify source. Tags are release output, not triggers.
After publication, archive the notes under the exact new tag and retain historical
notes. Review the [compatibility matrix](../start/compatibility.md) and
[qualification record](../guide/qualification.md). After publishing, update the
documented download release and remove the applicable unreleased notices through
a reviewed PR. Record qualification only after inspecting evidence for the exact
source and environment.
