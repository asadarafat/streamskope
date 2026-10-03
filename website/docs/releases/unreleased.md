---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

- Manage Kafka Connect through a profile endpoint: discover connectors, validate string-map configuration and review lifecycle or failed-task restart actions. Omitted update values, including secrets, remain in the host.
- Open configured Connect DLQ topics and inspect supported error context before using bounded record replay.

The paired host/renderer protocol advances to 42. The plugin API remains unchanged. A maintainer assigns the next desktop version when starting release CI.
