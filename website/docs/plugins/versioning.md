---
plugin_compatibility: true
---

# Plugin versioning and compatibility

The first release uses this convention for both EDA Capture and NSP Capture,
with desktop **v0.1.0+build.1** and plugin API **3**.

Every connection plugin uses the same package identity. It tells you the minimum
StreamSkope release, the target system and its supported version interval. Open
**Preferences → Plugins** to inspect these requirements before installing or
updating. The catalog selects packages compatible with your desktop; the plugin
checks the running target version through its API before creating capture resources
or retrieving connection material.

## Read a package identity

```text
v0.1.0+build.1--eda-26.8.2-26.8.2--r1
\____________/  |  \____/ \____/  \_/
 desktop min   system min   max  revision
```

| Part             | Meaning                                                                                                                      |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `v0.1.0+build.1` | Minimum supported StreamSkope desktop release. App version alone is insufficient when two releases use the same app version. |
| `eda`            | Target system identifier. NSP Capture uses `nsp`.                                                                            |
| First `26.8.2`   | Earliest supported target version, included in the interval.                                                                 |
| Second `26.8.2`  | Latest supported target version, also included. Equal bounds mean exactly one supported version.                             |
| `r1`             | Immutable package revision, starting at 1 and increasing for that plugin.                                                    |

The complete identity is a custom compatibility label, not a Semantic Version.
StreamSkope's app version still follows [Semantic Versioning](https://semver.org/). Desktop release builds
are compared numerically: `v0.1.0+build.10` is newer than `v0.1.0+build.2`, and
`v0.1.1` is newer than either. Plugin revisions are also numeric: `r10` is newer
than `r2`.

The target bounds are exact `major.minor.patch` values. They are inclusive;
wildcards such as `26.8.x` are not package declarations. A future interval ending
at `27.4.1` would assert compatibility across the declared interval, not merely
with its two endpoints. That range must be qualified before publication. A wider
range must never be inferred from the plugin name, protocol similarity or a
successful connection to one lab.

## Install, update and target upgrades

```text
Preferences -> Plugins
  -> require a supported plugin API and minimum desktop release
  -> verify the downloaded package and declared resources
  -> install or update without restarting the desktop

Create or refresh a connection
  -> authenticate to the target API
  -> read the running target version
  -> version inside the declared interval?
       yes -> continue the plugin's setup workflow
       no  -> report the mismatch before new target-side work
```

NSP Capture can retry cleanup of an owned execution even when the target version
changes or its version endpoint is unavailable. EDA recovery has separate lease,
tunnel and cleanup requirements; follow the [EDA recovery guide](eda.md#stop-update-and-resume).
A compatibility failure blocks new capture resources or a new retrieval workflow.
Saved profiles remain available for recovery; removing a plugin does not delete them.

Before upgrading EDA or NSP, compare the new product version with the installed
plugin's target bounds. If it falls outside them, install a qualified compatible
plugin before starting new setup against the upgraded system. Updating the desktop
alone does not extend a plugin's target interval.

Every change to plugin code, its bundled workflow or its compatibility bounds
requires a new revision. Revisions increase across the plugin's entire history;
they do not reset when the desktop minimum or target interval changes. The same
revision cannot identify different compatibility declarations. A published
identity is immutable, so a correction is another revision, not a replacement
asset under the old name.

For example, an EDA package fix within the same supported versions becomes
`v0.1.0+build.1--eda-26.8.2-26.8.2--r2`. A later change to a target bound must
continue with a higher revision. The desktop app and target product versions do
not change merely because a plugin fix was published.

## Component versions

| Component           | Version responsibility                                                                                                         |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| StreamSkope app     | Semantic Version, such as `0.1.0`.                                                                                             |
| Desktop release     | Exact build identity, such as `v0.1.0+build.1`. It establishes the plugin's minimum host requirement.                          |
| Connection plugin   | Compatibility identity plus monotonically increasing revision. EDA and NSP follow the same convention.                         |
| EDA cluster app     | Separate component aligned with its EDA target, currently `26.8.2`. A desktop plugin revision does not rename or republish it. |
| NSP helper workflow | Included in the NSP plugin download, with its digest declared in the package. Changes require a new plugin revision.           |

Plugin API **3** makes compatibility and resource metadata mandatory where
applicable. The desktop checks this API separately from the package identity.
A matching target interval cannot make an unsupported plugin API loadable.

Continue with the [plugin overview](index.md), [EDA Capture](eda.md) or
[NSP Capture](nsp.md) for the full setup and cleanup call flows.

## Declared packages

The following declarations are generated from the source plugin manifests. They
are source requirements, not a statement that a release asset is published.
Equal target bounds support one exact product release. Use the
[compatibility matrix](../start/compatibility.md) for application capabilities
and [qualification evidence](../guide/qualification.md) for exercised environments.
