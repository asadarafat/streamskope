---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

- Manage Kafka Connect through a profile endpoint: discover connectors, validate string-map configuration and review lifecycle or failed-task restart actions. Omitted update values, including secrets, remain in the host.
- Open configured Connect DLQ topics and inspect supported error context before using bounded record replay.

The paired host/renderer protocol advances to 43. The plugin API remains unchanged. A maintainer assigns the next desktop version when starting release CI.

- Compare bounded, versioned topic configuration snapshots across the active cluster and saved destinations. Export supported settings to Git and selectively promote reviewed differences with identity, drift and read-back checks. Other resource types and continuous reconciliation are not supported. See [Compare environments](../guide/environment-comparison.md).
