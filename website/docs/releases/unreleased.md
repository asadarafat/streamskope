---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

- Preview exact consumer offset resets, inspect bounded examples and replay
  exposure, then apply to a stopped group with stale-baseline checks and
  per-partition outcome reconciliation. See [offset recovery](../guide/offset-recovery.md).

- Copy a frozen selection to the same topic, another topic or an isolated saved
  profile with explicit transformations, exact destination review, cancellation
  and per-record acknowledgement/uncertainty accounting. See [record replay](../guide/record-replay.md).

- Explain topic READ with prefix/wildcard and DENY semantics, explicit unknown
  broker policy, and reviewed ACL changes that reject stale inputs and retain
  uncertain outcomes. See [access review](../guide/access-review.md).

A maintainer assigns the next desktop version when starting release CI.
