# Review and replay a failed record

A failed record has an understood cause and an agreed destination. Inspect its
original evidence, review exactly what will be sent, and keep the outcome before
considering another attempt.

## Watch the walkthrough

A 42-second recording of the actual workbench with on-screen instructions and no
audio. Correct and replay one illustrative JSON record to a separate, owned AIO topic, then inspect the acknowledgement.

<video class="skope-walkthrough" controls muted playsinline preload="metadata"
       poster="../../assets/skope/replay-record.png"
       aria-label="Walkthrough: Review and replay a failed record"
       aria-describedby="replay-record-video-summary">
<source src="../assets/skope/replay-record.mp4" type="video/mp4">
  <track kind="captions" src="../assets/skope/replay-record.vtt" srclang="en" label="English instructions">
  <a href="../assets/skope/replay-record.mp4">Download the walkthrough</a>.
</video>

<p id="replay-record-video-summary">Follow the actions and outcomes below. Pause or use
fullscreen to inspect the controls; the written guide covers prerequisites and
the decisions behind each step.</p>

<details markdown="1">
<summary>Read the walkthrough transcript</summary>

- **00:00 — Inspect the source record.** Illustrative AIO record: review the correction before publishing.
- **00:07 — Choose a separate destination.** Replay leaves the original record unchanged.
- **00:14 — Describe the JSON correction.** This sample changes /status to ready; review production writer mappings separately.
- **00:21 — Preview before sending.** Inspect original and destination bytes. Preview publishes nothing.
- **00:28 — Confirm and apply once.** Type the exact destination. Never blindly retry an uncertain write.
- **00:35 — Read the acknowledgement.** The receipt proves Kafka accepted the write, not consumer processing.

</details>

## Before you start

Agree the recovery action with the consumer owner. You need source-read and
destination-write permissions, complete original record bytes, and an existing
destination topic. Read-only mode prevents publication; masking can prevent
replay review. Check the [replay prerequisites and limits](../guide/record-replay.md).

For a Connect dead-letter queue, start with
[Browse DLQ and reported context](../guide/kafka-connect.md#investigate-a-dead-letter-queue).
Reported headers can be incomplete or ambiguous; they are not proof of broker
identity. Do not infer a safe destination from a topic name alone.

## Review before sending

1. Establish why processing failed. Inspect the original record and check the
   consumer or connector logs when needed. Retrying unchanged data into the same
   failing consumer can repeat the failure or create a loop.
2. Agree whether the correction belongs in the consumer, the record or its
   destination. Confirm the destination with its owner; a familiar topic name
   alone is insufficient. Prepare any plugin-managed destination beforehand.
3. Follow [Copy or replay records](../guide/record-replay.md) to select the intended
   records, configure any correction and obtain a preview. If schemas are involved,
   use its [writer mapping procedure](../guide/record-replay.md#structured-edits-and-destination-writers).
4. Assess the preview against the agreed correction: source and destination
   identities, exact bytes and writer mappings. If it does not match, stop and
   prepare a new review. Preview sends nothing.
5. Apply the reviewed plan once, then use the outcomes below to decide whether
   any further attempt is justified. Do not use another click as a connection test.

## Decide what to do with the outcome

| Outcome                                | Next action                                                                                                                                         |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| Acknowledged by Kafka                  | Retain the receipt and verify downstream processing separately; publication is not consumer success                                                 |
| Explicitly rejected                    | Correct the failure before preparing a new deliberate review                                                                                        |
| Definitely unsent                      | Use [Review definitely unsent records](../guide/record-replay.md#continue-an-interrupted-attempt) if continuing the original attempt is appropriate |
| Uncertain                              | Preserve history and reconcile available destination evidence; do not blindly resend                                                                |
| Cleanup or receipt storage unavailable | Preserve the recovery state and resolve it before new work                                                                                          |

An equivalent record found at the destination does not turn an uncertain dispatch
into an acknowledgement or prove it was sent by this attempt. See
[uncertainty and reconciliation](../guide/record-replay.md#inspect-uncertainty-and-archive-known-chains).

## Finish and share

Open **Repair history** to retain the attempt's outcomes. Reopening history does
not send anything. [Share a reviewed summary](share-findings.md) of the destination,
acknowledged, rejected, unsent and uncertain outcomes.

Replay leaves source records and consumer offsets unchanged. It does not skip
the failed record, resume a connector or prevent a loop when sent back to the
same failing consumer. Any follow-up action needs its own review.

To author a new test record instead of replaying an existing one, use
[Produce and verify one message](../guide/produce-message.md). Review the
destination and downstream effects before sending it.
