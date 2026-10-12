# Search coverage and evidence

An empty result only describes the work that actually completed. It can mean no
record matched, but it can also mean the wrong interval was selected, access was
denied, a field was unreadable or a bound stopped the read.

## A sample is not the whole topic

Grid filters operate on loaded records. Broker search and correlation tracing
read bounded ranges, with their own limits and completion reports. A match limit,
scan limit, deadline or cancellation can leave some positions unexamined.

Coverage makes that distinction visible: which topics and partitions were
requested, which ranges were reached, and what could not be evaluated. A complete
search of a retained range cannot recover data already removed by Kafka or prove
that an event never existed elsewhere.

NATS live subscriptions have a different boundary. They observe matching
publications while the subscription is active. This workbench does not use them
to establish whether a message was published before subscribing.

## Correlation does not establish causality

The same correlation value at several offsets helps locate related records. It
does not establish their business ordering or prove that one caused another.
Kafka record timestamps, payload timestamps and the time StreamSkope collected
an observation can have different meanings. Their difference is not automatically
an end-to-end latency measurement.

Likewise, consumer lag compares positions. A committed offset does not prove
successful business processing, and a stalled commit can have several causes,
including batching or a stopped application. Consumer and producer logs may be
needed to test a hypothesis.

## Freshness and source matter

Observed health describes a selected topic and optional group from one client.
It reports unknown or stale evidence instead of substituting zero. Optional
Prometheus values come from a separately selected exporter; configuring an
endpoint does not establish that exporter's identity as the connected cluster.

A useful handover therefore includes the source, UTC interval, positions, coverage
gaps and interpretation. A saved view keeps settings and positions so another
engineer can repeat a check; it does not freeze health history, trace results or
message bodies.

## Apply this distinction

- [Find a missing or delayed event](../skope/missing-event.md) works through a
  search and its possible next checks.
- [Diagnose Kafka health](../guide/observed-health.md) explains findings and their
  supporting observations.
- [Share your findings](../skope/share-findings.md) separates repeatable settings
  from the evidence and conclusions being handed over.
- [Read limits and export coverage](../guide/data-handling.md#message-limits)
  specify the bounds of each operation.
