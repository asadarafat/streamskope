---
plugin_compatibility: true
unreleased: true
---

# Plugin versioning and compatibility

StreamSkope and its connection plugins use **independent Semantic Versions**.
A plugin version identifies its code and bundled resources; its manifest separately
declares the supported desktop versions, plugin API and target-system versions.
You do not need to infer compatibility from a filename.

This convention starts with the upcoming **StreamSkope 0.2.0** and **plugin API 4**.
The published [v0.1.0+build.1 release](../releases/v0.1.0+build.1.md) retains its
original API 3 packages. Source declarations below do not mean the new packages
are already published.

## Read the version and requirements

The first independent EDA Capture and NSP Capture versions are each **0.1.0**.
Their equal numbers do not couple their releases. An EDA-only fix can publish
EDA Capture **0.1.1** while NSP Capture remains **0.1.0** and the desktop remains
**0.2.0**.

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

Plugins publish independently. A desktop release does not republish them, and a
plugin release does not rebuild the desktop. The catalog compares versions within
each plugin ID; EDA's version is never compared with NSP's version. It also enforces
the desktop interval independently of version ordering.

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
not renamed. API 2 used still earlier version labels. StreamSkope 0.2.0 retains
loading support for installed API 2 and API 3 packages and their saved profiles.

1. [Back up the application data](../guide/recovery.md#back-up-before-upgrading).
2. Upgrade the desktop to a release supporting API 4.
3. In **Preferences → Plugins**, update each plugin when its API 4 release appears.
4. Complete any active-work cleanup prompt, then resume a stopped capture explicitly.

The catalog treats a compatible API 4 package as the successor to the old package
generation. Its `0.1.0` version is not numerically compared with a legacy target
version or custom revision label. Subsequent API 4 updates use SemVer ordering.
Failed activation retains the previous verified plugin for recovery. Older desktop
releases cannot load API 4; preserve a complete pre-upgrade backup for rollback.
Legacy compatibility does not qualify a plugin against target versions beyond its
original support declaration.

## Component versions

| Component                     | Version responsibility                                                                                                                    |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| StreamSkope app and installer | One SemVer from `package.json.version`; release tag adds `v`.                                                                             |
| EDA / NSP connection plugin   | Independent SemVer in its manifest; compatibility fields declare requirements.                                                            |
| Plugin API                    | Separate host contract version, currently `4`; a plugin patch does not increment it.                                                      |
| EDA cluster app               | Separate artifact aligned exactly with its EDA target, currently `26.8.2`. A desktop/plugin update does not rename or republish it.       |
| NSP helper workflow           | Bundled and digest-verified in the NSP plugin. Changes require a new plugin version and review of the immutable deployed helper identity. |

Continue with the [plugin overview](index.md), [EDA Capture](eda.md) or
[NSP Capture](nsp.md) for setup and cleanup call flows.

## Declared packages

These requirements are generated from source manifests. Publication is established
by the official release catalog, and live results by the [qualification record](../guide/qualification.md).
