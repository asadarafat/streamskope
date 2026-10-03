---
title: Compare environments
description: Export bounded topic configuration snapshots, review drift and selectively promote settings.
---

# Compare environments

Open **Compare environments** after connecting to Kafka. Capture one to twenty named topics from the active source. Save the source JSON in your private Git repository, or paste a previously exported snapshot as the source. Importing does not change Kafka.

Choose a saved destination profile or the active connection, then **Capture destination and compare**. A saved destination opens separately with host-owned credentials and closes after the read; it does not disconnect the source or activate a plugin. Prepare a managed capture before selecting its profile.

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
