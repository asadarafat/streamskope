---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

A maintainer assigns the next desktop version when starting release CI.

## Structured record inspection

Message details adds **Decoded** for explicit JSON, Confluent Avro and Protobuf
inspection, including writer schema IDs, declared references and per-record errors.
Original bytes are unchanged. Integer precision, schema lookup, worker execution
and disclosure limits are explicit; see [structured records](../guide/structured-events.md).

The paired host/renderer protocol is **37**; upgrade them together. Plugin API
compatibility is unchanged. This adds inspection, not automatic table decoding,
custom codecs or a general serializer configuration for producers.

- Compare pinned records by original bytes or decoded structure, including ordered headers and explicit incomplete-input limits.
- Compare exact schema versions and explore a bounded declared reference tree with missing, cyclic and limited edges.

- Generate seeded, validated Avro/Protobuf/JSON samples from exact schema versions, then separately review and publish bounded batches with cancellation accounting.
