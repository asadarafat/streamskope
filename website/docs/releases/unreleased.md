---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

## Desktop

Topic Monitor now separates current pressure from historical display loss, samples
quiet streams, and identifies the request that owns each measurement. Stop also
cancels an opening read; connection changes discard old queued callbacks. Terminal
publication respects desktop transport pressure and reports any remaining display
omissions rather than overflowing the channel.

The Monitor leads with publication, queue, loss and freshness, keeps Stop/Cancel
available, and places renderer timing and application FPS under Diagnostics.
See [Stream monitor](../guide/operations.md#stream-monitor) for metric boundaries
and stopping behavior.

The internal host protocol changes from **47 to 48**. Host and renderer must be
updated together; plugin API **4** is unchanged. This does not assign a new plugin
release or change declared target-system compatibility.

## Documentation

Documentation publication now checks the latest immutable stable desktop release
before build and deployment. Plugin guides separate source requirements from a
verified catalog availability snapshot; source rehearsal records keep their exact
revision and qualification limits.

## Qualification limits

Shared CI and the Topic Monitor browser workflow passed on the integrated source.
Local qualification remains incomplete: the unchanged 60-second mixed-payload
replay recorded **28.56% display omissions** (limit 2%), **78.99% of one CPU core**
(limit 60%), and **265.16 ms event-loop p99** (limit 150 ms). Memory bounds and
record accounting passed. This is application-ingestion replay evidence, not a
measurement of Kafka fetch, desktop IPC or UI interaction latency.

Live EDA reached capture readiness and lease renewal, but Kafka connection timed
out before record receipt. Stop, owned-resource cleanup and repeat Stop passed;
end-to-end EDA capture remains unqualified on this candidate. Earlier passing EDA,
performance and native recovery rehearsals do not qualify the changed source.
See the [source-bound qualification record](https://asadarafat.github.io/streamskope/guide/qualification/#topic-monitor-candidate-2026-10-04)
for the report and exact limits. Native installers are qualified separately by
this release's build workflow; no new installed upgrade/rollback rehearsal is claimed.

The next version will be assigned when a maintainer starts the release workflow.
Plugin releases remain independent. These changes are not included in
[v0.7.1](v0.7.1.md).
