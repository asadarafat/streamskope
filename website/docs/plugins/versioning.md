---
plugin_compatibility: true
unreleased: true
---

# Plugin versioning and compatibility

StreamSkope and its connection plugins use **independent Semantic Versions**.
A plugin version identifies its code and bundled resources; its manifest separately
declares the supported desktop versions, plugin API and target-system versions.
You do not need to infer compatibility from a filename.

Desktop v0.2.0 introduced this convention and **plugin API 4**.
Final desktop and plugin versions are assigned only when a maintainer starts
release CI; source versions remain `0.0.0-dev`.
The published [v0.1.0+build.1 release](../releases/v0.1.0+build.1.md) retains its
original API 3 packages. Source declarations below do not mean the new packages
are already published.

## Read the version and requirements

Each plugin has its own release sequence. For example, an EDA-only fix could
publish EDA Capture **0.1.1** while NSP Capture remains **0.1.0** and the desktop
remains **0.2.0**. These numbers illustrate independent versioning; they do not
reserve future versions or establish publication.

| Field            | Example                   | Meaning                                                                 |
| ---------------- | ------------------------- | ----------------------------------------------------------------------- |
| Plugin version   | `0.1.0`                   | Version of that plugin's code, resources and compatibility declaration. |
| Desktop interval | `>=0.2.0, <0.3.0`         | Inclusive minimum and exclusive maximum host version.                   |
| Plugin API       | `4`                       | Host capabilities and manifest contract required to load the plugin.    |
| Target system    | `eda`                     | Platform handled by the plugin; NSP Capture declares `nsp`.             |
| Target interval  | `26.8.2` through `26.8.2` | Inclusive product versions; equal bounds support exactly one release.   |

Open **Preferences → Plugins** to inspect the version and requirements. The catalog
selects a compatible package, and the plugin reads the running target's version
through its API before creating capture resources or retrieving connection material.
A matching target version cannot make an unsupported plugin API loadable.

Target bounds are exact `major.minor.patch` values. Wildcards such as `26.8.x`
are not package declarations. A wider interval asserts support across the entire
interval, not only its endpoints. Qualify the supported releases before widening
it; a successful connection to one lab does not establish broader compatibility.

## Release and update rules

Both the desktop and plugins follow [Semantic Versioning](https://semver.org/):
patch versions fix compatible behavior, minor versions add compatible capabilities,
and major versions introduce incompatible changes. While a component is below
`1.0.0`, breaking changes increment its minor version and compatible fixes increment
its patch version. A plugin change that removes a previously supported desktop or
target version is a compatibility break; widening a qualified interval can be a
minor release. Changes to bundled workflow bytes also require a new plugin version.

Use prereleases such as `0.2.0-rc.1` for release candidates. The desktop/plugin
release workflows reject `+build.N` as a new release identity: build metadata does
not affect SemVer ordering. CI run numbers and Git commits supply build traceability.
A published version is immutable. Never replace its contents or publish different
bytes under the same plugin ID and version.

Prerelease desktops require explicit qualification: a manifest must name a
prerelease minimum with the same core version. For example, minimum
`0.2.0-rc.1` can admit `0.2.0-rc.2` and subsequent stable versions inside its
bounds. Minimum `0.2.0` with maximum-exclusive `0.3.0` does not silently admit
`0.3.0-rc.1`. Stable desktops do not receive prerelease plugins from the catalog;
preview desktops can receive them when their declared host requirements match.

| Artifact            | Release tag / asset example                                                  |
| ------------------- | ---------------------------------------------------------------------------- |
| Desktop             | `v0.2.0` / `StreamSkope-0.2.0-darwin-arm64.dmg`                              |
| EDA Capture plugin  | `plugins/eda/v0.1.0` / `streamskope-eda-v0.1.0.skope-plugin`                 |
| NSP Capture plugin  | `plugins/nsp/v0.1.0` / `streamskope-nsp-v0.1.0.skope-plugin`                 |
| NSP helper download | `streamskope-nsp-v0.1.0-nsp-capture.workflow.yaml` in the NSP plugin release |

The tags above are release outputs, not triggers. A maintainer selects **desktop**,
**eda** or **nsp** and enters its version in **Actions → Release → Run workflow**
from `main`. Release CI qualifies the exact source, assigns the version in a
disposable build checkout and creates a draft to review and publish. PR checks
only qualify source; merging to `main` neither repeats CI nor assigns a final version.

Plugins publish independently. A desktop release does not republish them, and a
plugin release does not rebuild the desktop. The catalog compares versions within
each plugin ID; EDA's version is never compared with NSP's version. It also enforces
the desktop interval independently of version ordering.

Each plugin draft includes a generated PR changelog against its own previous
eligible published release, plus reviewed highlights, upgrade instructions,
limitations and the packaged compatibility metadata. It does not compare against
the most recent desktop or other plugin release. A first plugin release explicitly
covers history without a previous plugin baseline. Shared changes can appear in
both plugin changelogs. Stable releases include changes introduced by intervening
release candidates.

Maintainers keep EDA and NSP commentary in their respective
`plugins/NAME/RELEASE_NOTES.md` files. After publication, archive the final reviewed
GitHub body in the [release notes](../releases/index.md) through a documentation PR
and reset only the shipped commentary. A listed compatibility interval describes
the package's declaration; use the [qualification record](../guide/qualification.md)
to establish which live environments were actually tested.

## Install, update and target upgrades

```text
Preferences -> Plugins
  -> require supported API + desktop inside host interval
  -> verify package and declared resource digests
  -> install or update without restarting the desktop

Create or refresh a connection
  -> authenticate to the target API
  -> read the running target version
  -> version inside the declared target interval?
       yes -> continue the plugin's setup workflow
       no  -> report the mismatch before new target-side work
```

NSP Capture can retry cleanup of an owned execution even when the target version
changes or its version endpoint is unavailable. EDA recovery has separate lease,
tunnel and cleanup requirements; follow the [EDA recovery guide](eda.md#stop-update-and-resume).
Saved profiles remain available for recovery; removing a plugin does not delete them.

Before upgrading EDA, NSP or StreamSkope, compare the intended version with the
installed plugin's corresponding bounds. Install a qualified compatible plugin
before starting new setup against an upgraded target. A desktop update alone does
not extend a plugin's target interval.

## Upgrade from the original packages

The original release used custom API 3 labels such as
`v0.1.0+build.1--eda-26.8.2-26.8.2--r1`. They remain historical identifiers and are
not renamed. API 2 used still earlier version labels. Release builds supporting
API 4 retain loading support for installed API 2 and API 3 packages and their
saved profiles.

1. [Back up the application data](../guide/recovery.md#back-up-before-upgrading).
2. Upgrade the desktop to a release supporting API 4.
3. In **Preferences → Plugins**, update each plugin when its API 4 release appears.
4. Complete any active-work cleanup prompt, then resume a stopped capture explicitly.

The catalog treats a compatible API 4 package as the successor to the old package
generation. Its Semantic Version is not numerically compared with a legacy target
version or custom revision label. Subsequent API 4 updates use SemVer ordering.
Failed activation retains the previous verified plugin for recovery. Older desktop
releases cannot load API 4; preserve a complete pre-upgrade backup for rollback.
Legacy compatibility does not qualify a plugin against target versions beyond its
original support declaration.

## Component versions

| Component                     | Version responsibility                                                                                                                    |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| StreamSkope app and installer | Release CI stamps one chosen SemVer into the app and installer; tag adds `v`.                                                             |
| EDA / NSP connection plugin   | Release CI stamps an independent SemVer into its manifest; compatibility fields declare requirements.                                     |
| Plugin API                    | Separate host contract version, currently `4`; a plugin patch does not increment it.                                                      |
| EDA cluster app               | Separate artifact aligned exactly with its EDA target, currently `26.8.2`. A desktop/plugin update does not rename or republish it.       |
| NSP helper workflow           | Bundled and digest-verified in the NSP plugin. Changes require a new plugin version and review of the immutable deployed helper identity. |

Continue with the [plugin overview](index.md), [EDA Capture](eda.md) or
[NSP Capture](nsp.md) for setup and cleanup call flows.

## Development packages

Source desktop and plugin versions are `0.0.0-dev`, a development marker with no
release availability claim. Local plugin packaging assigns
`0.0.0-dev.<numeric timestamp>` so a rebuild can be installed without reusing an
immutable identity. Only development hosts load current-API development packages;
released desktops reject them. Test published packages with a compatible released
desktop. The declared `>=0.4.0, <0.5.0` host interval remains the requirement for
released API 4 packages; it does not assign the desktop's next release number.

The browser development host offers locally built packages through the same
**Preferences → Plugins** controls. Follow the [source plugin testing steps](../start/development.md#test-a-development-plugin).
Preserve existing development data and cleanup state; use the previous compatible
build to clean up and remove an older release-versioned development installation
before installing a development package.

Source plugins now target desktop 0.4.x. This declaration does not widen an
already-published package's compatibility or certify live EDA/NSP behavior. Plugin
release qualification and publication remain separate from the desktop release.

## Declared packages

These requirements are generated from source manifests. Publication is established
by the official release catalog, and live results by the [qualification record](../guide/qualification.md).
