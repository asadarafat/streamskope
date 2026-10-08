---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

<!-- development-release-status -->

Published releases provide the component baselines. Run `npm run docs -- pending`
to inspect the PR-derived inventory after those baselines; this page records
additional release highlights, not a complete list of pending changes.
<!-- /development-release-status -->

## Release highlights

Installer-managed browser hosts gain explicit `check`, `upgrade`, `rollback` and
`recover` operations. Normal installer reruns still retain the installed release.
Maintenance preserves the saved owner and URL, backs up consistent stopped data,
and records the replacement only after locked readiness checks pass.

The reviewed legacy starting point is 0.10.3. The initial automated scope requires
an initialized vault and refuses unresolved recovery or saved plugin-managed
profile sources. Rollback uses current compatible data; it does not restore an
older snapshot or rewind remote resources. See the
[upgrade procedure](../guide/browser-host.md#upgrade-an-installer-managed-host)
before changing an existing installation.
