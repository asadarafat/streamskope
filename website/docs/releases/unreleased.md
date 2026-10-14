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

- Kafka reader setup has a separate 30-second budget for metadata and sequential
  offset lookups. Individual broker requests retain their five-second timeout.
  When preparation exceeds its budget, the error identifies the active step.
  A failed read no longer prompts you to select a topic that is already selected.
