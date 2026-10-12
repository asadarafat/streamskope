# Find a missing or delayed event

You have an event identifier and an approximate time, but cannot find the expected
record or consumer progress. Establish where you looked before deciding where
the problem is.

[Search coverage and evidence](../concepts/evidence.md) explains why an empty or
partial result cannot establish that an event was never published.

## Watch the walkthrough

A 42-second recording of the actual workbench with on-screen instructions and no
audio. Trace an illustrative order across a populated source and an empty destination on the local AIO Kafka broker.

<video class="skope-walkthrough" controls muted playsinline preload="metadata"
       poster="../../assets/skope/missing-event.png"
       aria-label="Walkthrough: Find a missing or delayed event"
       aria-describedby="missing-event-video-summary">
  <source src="../assets/skope/missing-event.mp4" type="video/mp4">
  <track kind="captions" src="../assets/skope/missing-event.vtt" srclang="en" label="English instructions">
  <a href="../assets/skope/missing-event.mp4">Download the walkthrough</a>.
</video>

<p id="missing-event-video-summary">Follow the actions and outcomes below. Pause or use
fullscreen to inspect the controls; the written guide covers prerequisites and
the decisions behind each step.</p>

<details markdown="1">
<summary>Read the walkthrough transcript</summary>

- **00:00 — Confirm the identifier.** Illustrative AIO order: inspect its exact correlation-id header.
- **00:07 — Choose explicit topics.** Search the source and destination; this is a bounded trace.
- **00:14 — Set the value and time range.** Match the case-sensitive header and review the UTC interval.
- **00:21 — Run the trace.** The read searches retained records without committing consumer offsets.
- **00:28 — Check coverage, not just matches.** The empty destination does not explain why delivery stopped.
- **00:35 — Choose the next check.** Check emission and consumer progress. Absence remains bounded by coverage.

</details>

## Before you start

Connect the intended Kafka profile. Know the likely topics, timezone and identifier
representation: a header name, the whole key, or a scalar payload field. If the
payload is structured, first check its [decoding](../guide/structured-events.md).
Unreadable and masked fields cannot establish a non-match.

## Establish the search window

1. Open a likely topic and stop any active read. Clear unrelated filters.
2. Select **Time window → Custom interval** around the event with explicit
   timezone bounds. Choose a suitable **Limit**, then **Load messages**.
3. Check the requested interval and completion coverage. Inspect a candidate's
   key, headers, value and Kafka timestamp to confirm the identifier's actual form.
4. If a grid filter finds nothing, remember that it covers the loaded sample.
   Use [broker search](../guide/messages.md#search-beyond-the-loaded-sample)
   or the explicit multi-topic trace below to examine a bounded broker range.

## Follow the identifier

1. In **Messages**, select **Trace correlation**. Enter the relevant **Trace topics**
   and **Exact correlation value**. Choose the matching source: header, key or
   payload field. For a payload field, enter its JSON Pointer, such as
   `/metadata/correlationId` only when that path exists in your records.
2. Choose the interval and **Start trace**. Inspect each match's topic, partition,
   offset and timestamp. Equal IDs at different offsets remain separate records.
3. Read the coverage for every topic, including topics with no matches. Follow
   the [trace limits and matching rules](../guide/structured-events.md#trace-a-correlation-id-across-topics)
   when the result is partial, denied or not searched.

## Choose the next check

| Evidence                                              | Next action                                                                                                             |
| ----------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Matching record found in a source topic               | Inspect the expected destination topic or consumer; matching IDs do not establish causality or global event order       |
| No matches, but coverage is partial or denied         | Resolve that coverage gap before treating absence as evidence                                                           |
| No matches in a fully searched range                  | Check the identifier and producer's emission history; retention, compaction or a different interval can explain absence |
| Records exist, but consumer progress is unclear       | Open [Observed health](../guide/observed-health.md), select this topic and the intended group, and capture observations |
| Commits remain stalled across comparable observations | Check the consumer's logs and commit policy; batching, stopped consumers or poison records are hypotheses to verify     |

Observed health describes the selected topic/group from this client. Low lag is
not proof of successful business processing. A payload timestamp and a Kafka
timestamp are not automatically a measured end-to-end latency.

## Finish and share

Stop any continuing reads or observation collection. [Save the query and useful
record positions](../guide/saved-views.md), and [share your findings](share-findings.md)
with the UTC range, selected topics and coverage gaps. Trace results themselves
are not a saved end-to-end trace or a historical snapshot.

## Check the delivery path

If the event passes through a connector, [inspect Kafka Connect and its task
results](../guide/kafka-connect.md). For access failures, [review the principal's
topic access](../guide/access-review.md). Where Redpanda transforms are part of
the path, [inspect their status](../guide/governance.md#redpanda-transforms).
These checks require the corresponding endpoint and permissions; the record
search alone cannot establish their behavior.
