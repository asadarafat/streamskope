# Release history

Choose a version to read its features, fixes, upgrade instructions and recorded
qualification limits. The sidebar shows the five newest releases, excluding
prereleases. This overview includes the complete StreamSkope history, including prereleases.

<!-- release-history -->

## Find the right downloads and guide

Use [Install StreamSkope](../start/installation.md) for the current published
installers and checksum instructions. Each release note records its installer
filenames and source identity. Obtain historical installers and `SHA256SUMS` from
that exact tagged [GitHub release](https://github.com/asadarafat/streamskope/releases).
A tag or draft does not establish download availability.

The public guide describes the latest StreamSkope release, excluding prereleases.
Opening an older release note shows historical changes; it does not switch the
whole guide to that version. Check the version notice on each page and the
[compatibility matrix](../start/compatibility.md) before following a procedure.

## StreamSkope and plugins release independently

StreamSkope versions use Semantic Versioning; desktop and browser delivery share
the application version. EDA Connector and NSP Connector each have their own
versions and declared StreamSkope, plugin API and target-system compatibility.
A StreamSkope release does not publish new plugin packages or widen existing package
compatibility. See [plugin versioning and compatibility](../plugins/versioning.md).

<!-- plugin-release-history -->

| Plugin release      | Archived notes                                  | Published assets                                                                            |
| ------------------- | ----------------------------------------------- | ------------------------------------------------------------------------------------------- |
| EDA Connector 0.1.2 | [Read the release notes](plugins/eda/v0.1.2.md) | [Plugin release](https://github.com/asadarafat/streamskope/releases/tag/plugins/eda/v0.1.2) |
| NSP Connector 0.1.2 | [Read the release notes](plugins/nsp/v0.1.2.md) | [Plugin release](https://github.com/asadarafat/streamskope/releases/tag/plugins/nsp/v0.1.2) |
| EDA Connector 0.1.1 | [Read the release notes](plugins/eda/v0.1.1.md) | [Plugin release](https://github.com/asadarafat/streamskope/releases/tag/plugins/eda/v0.1.1) |
| NSP Connector 0.1.1 | [Read the release notes](plugins/nsp/v0.1.1.md) | [Plugin release](https://github.com/asadarafat/streamskope/releases/tag/plugins/nsp/v0.1.1) |
| EDA Capture 0.1.0   | [Read the release notes](plugins/eda/v0.1.0.md) | [Plugin release](https://github.com/asadarafat/streamskope/releases/tag/plugins/eda/v0.1.0) |
| NSP Capture 0.1.0   | [Read the release notes](plugins/nsp/v0.1.0.md) | [Plugin release](https://github.com/asadarafat/streamskope/releases/tag/plugins/nsp/v0.1.0) |

<!-- /plugin-release-history -->

## Read qualification evidence

Release notes distinguish executed checks from limitations and checks that were
not run. Publishing a release does not turn an unrecorded live test into a pass.
See [Qualification evidence](../guide/qualification.md) for source identities,
environments and retained results.

Maintainers can find the release process and changelog selection rules in the
[contributor guide](https://github.com/asadarafat/streamskope/blob/main/CONTRIBUTING.md#release-notes).
Documentation is published with StreamSkope releases, excluding prereleases; PR merges qualify source
documentation without replacing the public site.
