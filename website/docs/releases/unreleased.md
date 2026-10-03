---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

- Observe selected-topic health and consumer lag over time, with bounded local desktop history, visible gaps/freshness and optional local thresholds. Collection stops when leaving Observed health or closing the app.
- Review lag trends with a 60-second scenario and held-out error, anomaly baselines, partition/key distribution and evidence-linked hypotheses. Optional protected record reads retain aggregates and example locators without raw keys or payloads.
- Trace selected-topic relationships and potential schema impact through bounded Kafka group, Connect and Registry evidence. Graphs distinguish observed, declared and inferred links; unknown coverage stays visible.
- Keep release documentation and its qualification report aligned with the exact publication event. Correct API 4 availability wording, history handling, permissions and discovery limits in the operator guides.

## Upgrade and limits

The paired host/renderer protocol advances to 47. Plugin API 4 is unchanged;
source plugin compatibility includes desktop `>=0.4.0, <0.8.0`. Existing published
packages keep their original manifests and plugin releases remain independent.
Back up application data before upgrading; observation history is private local
JSON, not encrypted storage or a Kafka archive.

Observations measure client API timings and offset positions, not broker CPU/disk
or consumer processing latency. Forecasts and anomaly/hot-key/skew hints are bounded
heuristics. Relationship discovery can miss historical, hidden or external
consumers; it never approves a schema change. No monitoring or notifications run
while the desktop is closed. Installers remain unsigned; installed native plugin
migration and credential-service recovery still need their own rehearsals.

A maintainer assigns the next desktop version during Release CI.
