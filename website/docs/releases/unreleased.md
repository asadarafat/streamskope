---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

- Observe selected-topic metadata and consumer-group lag over time, with bounded private desktop history, explicit freshness/gaps and opt-in local thresholds. Collection stops when leaving Observed health or closing the app; broker CPU/disk are not inferred.

The paired host/renderer protocol advances to 46. The plugin API remains unchanged. A maintainer assigns the next desktop version during Release CI.

- Observed health now explains lag trends with a 60-second scenario and held-out error,
  bounded anomaly baselines, partition/key distribution and evidence-linked hypotheses.
  Optional protected record reads retain aggregates and example locators, never payloads
  or raw keys. Missing and stale evidence stays unknown.
