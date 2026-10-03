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

The paired host/renderer protocol is **35**; upgrade them together. Plugin API
compatibility is unchanged. This adds inspection, not automatic table decoding,
custom codecs or a general serializer configuration for producers.
