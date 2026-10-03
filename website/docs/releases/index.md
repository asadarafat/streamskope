# Match the guide to your release

| Desktop release                     | App/installer version  | Status and contents                                                                                            |
| ----------------------------------- | ---------------------- | -------------------------------------------------------------------------------------------------------------- |
| [Unreleased changes](unreleased.md) | Assigned by release CI | Development changes after v0.6.0                                                                               |
| [v0.6.0](v0.6.0.md)                 | 0.6.0                  | Published unsigned release: Connect/DLQ operations, environment comparison, CLI, sandbox and generated clients |
| [v0.5.0](v0.5.0.md)                 | 0.5.0                  | Published unsigned release: offset recovery, record replay and exact ACL access review                         |
| [v0.4.0](v0.4.0.md)                 | 0.4.0                  | Published unsigned release: structured decoding, comparisons, schema samples and correlation tracing           |
| [v0.3.0](v0.3.0.md)                 | 0.3.0                  | Published unsigned release: original records, reviewed writes, read-only/masking and reliable administration   |
| [v0.2.0](v0.2.0.md)                 | 0.2.0                  | Published unsigned release: historical queries, saved/shared investigations and plugin API 4                   |
| [v0.1.0+build.1](v0.1.0+build.1.md) | 0.1.0                  | Published unsigned prerelease: Kafka workbench, hot plugin lifecycle, API 3 EDA and NSP packages               |

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

The notice on each guide identifies the source it describes. **Development
documentation** in a checkout or PR labels unreleased guides explicitly; it does
not assign a release version or claim that new features exist in older installers.
**Published documentation** is a qualified snapshot of one published desktop
release: its tagged source, version notice and download links match.

Pages deploys only after a desktop release is published, including prereleases.
Merging to `main`, creating a tag or draft, and publishing plugins do not replace
the site. Documentation changes become public with the next desktop release.
The publication build includes the release event's final notes and checks installer
availability. It also updates the qualification page to the exact desktop release
and links `qualification-vX.Y.Z.json` only when that uploaded asset appears in the
publication event. Maintainers must attach the source-specific report and include
it in `SHA256SUMS` before publishing the draft. Without a report, the site explicitly
leaves live/local qualification unrecorded; publication does not mark it passed.
Plugin and signed EDA cluster-app publication remain independent.

Maintainers review unversioned notes, then start **Actions → Release → Run workflow**
from `main`, select **desktop**, **eda** or **nsp** and enter its version. CI collects
the component's merged PRs up to that exact source commit, qualifies the source,
stamps a build checkout and creates a tagged draft; review it before publishing.
PR checks only qualify source; merging to `main` does not repeat CI.
Tags are release output, not triggers.

The changelog starts after the nearest ancestral published release of that same
component. Stable releases compare against a stable release, including changes
from intervening release candidates. With no eligible baseline, the draft explicitly
covers the full history. Component labels select relevant PRs; without labels,
files clearly owned by a plugin select that plugin, and other paths count as shared.
The [contributor guide](https://github.com/asadarafat/streamskope/blob/main/CONTRIBUTING.md#release-notes)
defines the mapping. The draft records its comparison
range and accompanies the generated changes with reviewed highlights, compatibility,
upgrade instructions, limitations and actual qualification links. Its workflow
artifacts retain the PR selection evidence for review.

After publication, the **published GitHub release body** is the authoritative record.
Pages includes the desktop publication event's exact body in its release snapshot.
Archive published bodies, including draft-review edits, under their release tags
in the repository through a documentation PR for future snapshots. Add page metadata and navigation, retain
historical notes and reset only that component's shipped unreleased commentary.
For a desktop release, update the documented downloads and applicable unreleased
notices in the same PR. That PR qualifies source documentation but does not deploy
the public site. Review the [compatibility matrix](../start/compatibility.md)
and [qualification record](../guide/qualification.md). Record qualification only
after inspecting evidence for the exact source and environment; publishing a
release does not qualify an unexecuted live test.
