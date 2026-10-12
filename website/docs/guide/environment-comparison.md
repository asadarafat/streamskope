---
title: Compare environments
description: Export bounded topic configuration snapshots, review drift and selectively promote settings.
---

# Compare environments

## What this workflow does

Compare supported Kafka topic settings with another environment or a saved
snapshot. You can review differences and explicitly promote selected settings
to an existing destination topic.

## The problem it solves

A topic behaves differently in test and production, and you suspect configuration
drift. For example, `retention.ms` differs between the two. Compare the settings
before changing them. This workflow does not compare message content or
synchronize entire clusters.

## Before you start

Know the source and destination identities and have permission to read their
topic configuration. Promotion additionally needs configuration-write permission.
Prepare any plugin-managed destination first. Reducing retention can delete data;
agree that consequence before applying a change.

## How to solve it

1. Connect the source profile and open **Compare environments**.
2. Enter one to twenty names in **Source topics**, then choose **Capture active source**.
   Use **Export source snapshot** and keep the file privately if
   you need to repeat the comparison. Alternatively, paste an existing snapshot
   into **Import source snapshot JSON** and choose **Use imported source**.
3. Choose the intended destination profile or the active connection, then select
   **Capture destination and compare**. A saved destination opens separately
   without replacing the source connection.
4. Review resource identities, observation times and supported setting differences.
   Stop here if you only need to diagnose drift; comparison changes nothing.
5. If promotion is agreed, select individual supported differences and choose
   **Review selected promotion**. Review the target, settings and consequences.
6. Confirm the exact target and setting count, then choose **Apply reviewed promotion**
   once. Read each topic result before considering another attempt.

## Check the result

For comparison, retain the differing setting values, resource identities and
observation times. After promotion, inspect each topic's acknowledgement and
read-back. A verified selected setting does not prove all environments match.
Reconcile unknown results; there is no automatic retry or rollback.

## Watch the walkthrough

A 42-second recording of the actual workbench with on-screen instructions and no
audio. Compare an illustrative desired-state snapshot with one AIO broker and apply one reviewed retention setting. This demonstrates the workflow; it is not a recording of two independent clusters.

<video class="skope-walkthrough" controls muted playsinline preload="metadata"
       poster="../../assets/skope/environment-comparison.png"
       aria-label="Walkthrough: Compare environments"
       aria-describedby="environment-comparison-video-summary">
<source src="https://github.com/asadarafat/streamskope/releases/download/documentation-media/v1/environment-comparison.mp4" type="video/mp4">
  <track kind="captions" src="../assets/skope/environment-comparison.vtt" srclang="en" label="English instructions">
  <a href="https://github.com/asadarafat/streamskope/releases/download/documentation-media/v1/environment-comparison.mp4">Download the walkthrough</a>.
</video>

<p id="environment-comparison-video-summary">Follow the actions and outcomes below. Pause or use
fullscreen to inspect the controls; the written guide covers prerequisites and
the decisions behind each step.</p>

<details markdown="1">
<summary>Read the walkthrough transcript</summary>

- **00:00 — Import the reviewed desired state.** Illustrative snapshot and one AIO broker; this recording does not test two clusters.
- **00:07 — Compare against the destination.** Active connection is the target here. Confirm identity before selecting changes.
- **00:14 — Select only the intended setting.** This owned topic changes retention.ms. Other configuration remains outside the selection.
- **00:21 — Review the promotion.** Review is read-only. It does not change the target setting.
- **00:28 — Confirm and apply once.** Recheck the target identity and consequences of reducing retention before applying.
- **00:35 — Check acknowledgement and read-back.** Success means this selected setting was verified, not that all environments match.

</details>

## Snapshot contract

The `streamskope.topic-config/v1` document contains a cluster ID, observation time, topic UUIDs and the settings below. Topics and settings have deterministic ordering. Exporting the same snapshot yields identical bytes. The value diff ignores observation time, while both times and identities remain visible for review.

| Supported setting                 | Meaning                             |
| --------------------------------- | ----------------------------------- |
| `cleanup.policy`                  | Delete, compact, or both            |
| `retention.ms`, `retention.bytes` | Retention limits                    |
| `segment.ms`                      | Segment roll interval               |
| `min.insync.replicas`             | Minimum in-sync replica requirement |
| `max.message.bytes`               | Topic record batch size limit       |
| `compression.type`                | Compression policy                  |

Only validated numeric or enumerated values are exported. Sensitive, missing, read-only or unsupported values cannot be promoted. Broker credentials, arbitrary configuration strings, ACLs, schemas, Connect configuration and record payloads are excluded. Cluster IDs and topic names are operational metadata; choose an appropriate Git repository for them.

The initial capture requires existing topics. Topic creation, deletion, partition/replica changes and other resource types are unsupported. Missing target topics must be provisioned separately through a reviewed workflow.

## Selective promotion

Select individual supported differences and **Review selected promotion**. The host rereads the destination and rejects drift since the displayed snapshot. It pins the selected values, destination profile revision, cluster and topic identities for a two-minute review. Confirm the exact target and setting count, then apply.

Before each topic write, the host rereads identity/settings, asks Kafka to validate the configuration and checks again before dispatch. Read-only mode blocks application. Changes are acknowledged per topic and read back. A failed or unverified topic stops later topics; their results remain **unsent**. An interrupted write can be **unknown**, and no automatic retry or rollback occurs. Applying the same review identifier returns the recorded result without sending it again.

This is an explicit promotion tool, not continuous reconciliation or a cross-topic transaction. Concurrent changes after the final check remain possible because the Kafka configuration API has no compare-and-swap operation. Source values are immutable historical intent: their age is visible, and the source is not reread at application time. Refresh or replace the source snapshot if that intent has changed.

Review changes to retention and durability carefully. Use [topic configuration](topic-configuration.md) for a single topic and inspect the destination after partial or uncertain results before preparing another promotion.
