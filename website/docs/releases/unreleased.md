---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

- Manage Kafka Connect through a profile endpoint: discover connectors, validate string-map configuration and review lifecycle or failed-task restart actions. Omitted update values, including secrets, remain in the host.
- Open configured Connect DLQ topics and inspect supported error context before using bounded record replay.

The paired host/renderer protocol advances to 44. The plugin API remains unchanged. A maintainer assigns the next desktop version when starting release CI.

- Compare bounded, versioned topic configuration snapshots across the active cluster and saved destinations. Export supported settings to Git and selectively promote reviewed differences with identity, drift and read-back checks. Other resource types and continuous reconciliation are not supported. See [Compare environments](../guide/environment-comparison.md).

- Use the source distribution’s bounded read-only inspect/query/export CLI with the shared query and masking behavior.
- Start an owned Kafka/Connect sandbox with reproducible seeds and bounded Node 24 consumer/transform exercises.
- Generate JavaScript CommonJS clients for self-contained Registry JSON Schema draft-07, with Ajv 8.20.0 validation, exact Registry framing and provenance. Other client languages and schema formats are not supported in this slice.

Upgrade: keep a complete pre-upgrade backup. Saving a Kafka Connect endpoint adds profile fields that older desktops do not understand; use that backup for downgrade rather than opening the updated store in an older app.
