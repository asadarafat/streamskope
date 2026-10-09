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

Kafka connection profiles add SASL PLAIN, SCRAM-SHA-256/512 and PEM mutual TLS
identities. Schema Registry and Connect gain independent Basic, bearer or OAuth
client credentials, certificate trust and optional client identities. Profile
connection tests now check configured Registry/Connect endpoints as well as Kafka.

Existing OAuth and plugin-generated profiles keep their prior behavior. Saving
expanded security adopts a new protected profile format and preserves the original
file for recovery. Older hosts cannot read that format; review the
[rollback snapshot rules](../guide/recovery.md#choose-the-correct-rollback-snapshot).
The runtime qualification matrix covers an isolated Kafka broker and controlled
HTTPS endpoints; this does not claim qualification of managed services or native
OS credential migration.

Kafka records now share one interpretation across the message grid, filters, live
rules, comparison, tracing, masking and export. Each record retains its writer
schema identity and an explicit decoding outcome; original bytes remain unchanged
and are withheld from disclosed records whenever masking is active. Mixed schemas,
tombstones and ordered duplicate headers retain their meaning across workflows.

**Preferences → Records** saves host-wide key/value encoding choices with controlled
detection and manual overrides. The message export advances to schema version 3
to include the captured structured projection. See
[record interpretation](../guide/structured-events.md) and
[export compatibility](../guide/data-handling.md#understand-an-export).
