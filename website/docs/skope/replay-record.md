# Replay a record after a processing failure

## What this workflow does

Replay sends a new Kafka record based on one you have already read. You can copy
it unchanged or review a supported correction before sending it to an existing
destination topic. The original record stays where it is; the new record gets
its own offset.

## The problem it solves

A consumer could not process an event, and its owner has agreed how to recover it.
For example, an order event contains `"status": "raedy"`, but the consumer accepts
`"ready"`. After confirming that correction, you can send a corrected copy to an
agreed `orders.retry` topic that the recovery consumer reads.

This is an illustrative example, not a rule that every failed event should be
edited. If the consumer bug was fixed and the original event is valid, replay an
unchanged copy instead. If the stopped group needs to reread the original records
in place, consider [resetting its offsets](../guide/offset-recovery.md).

Replay does not discover the failure cause, remove a dead-letter queue entry,
restart a consumer or guarantee successful processing. Sending to a topic that
no consumer reads will not repair the business operation.

## Before you start

- Ask the consumer owner which event failed, why it failed, and whether the
  proposed copy or correction is safe. A duplicate can repeat business effects.
- Confirm the source cluster/topic and an **existing** destination topic and
  partition. In the example below, `orders.retry` must already exist and the
  recovery consumer must read it. Its name alone does not make it a retry queue.
- Have source-read and destination-write permissions and complete original
  record bytes. Stop the current read before selecting records. Read-only mode
  blocks sending; masking blocks replay review.
- Prepare any plugin-managed destination before starting. For a Connect
  dead-letter queue (DLQ), first [inspect its reported context](../guide/kafka-connect.md#investigate-a-dead-letter-queue).
  Do not infer source identity or a safe destination from its headers alone.

## How to solve it

Start with one record. These steps use an ordinary JSON value and an agreed
same-cluster retry topic; Avro, Protobuf and cross-profile records can require
[explicit destination writer mappings](../guide/record-replay.md#structured-edits-and-destination-writers).

1. Open the source topic in **Topics**, load the relevant records and select the
   failed event. Inspect **Message details** and compare it with the failure in
   the consumer logs. Confirm its identifier, partition and offset.
2. Choose **Replay…** in the message toolbar. The dialog lists up to 50 frozen
   displayed records. It initially checks the selected record when complete
   original bytes are available. Confirm that **only** the intended event is
   checked before proceeding.
3. Choose **Active connection**, then the agreed existing destination topic
   `orders.retry` and its partition. Use a low rate such as one record per second.
4. For an unchanged retry, leave transformations off. For the agreed JSON
   correction, enable **Transform structured value**, refresh its source writers,
   and enter this edit:

    ```json
    [{ "op": "set", "path": "/status", "json": "\"ready\"" }]
    ```

    Keep the output as plain JSON for this example. The `json` field contains a
    serialized JSON value, so its inner quotes are required. Do not change the
    key or headers unless that change is also agreed. Invalid or unsupported
    structured input refuses review rather than guessing a repair.

5. Choose **Preview replay**. Confirm the one-record selection, source identity,
   destination cluster/topic/partition and exact before/after bytes. For the
   example, check that only `/status` changes to `"ready"`. Preview sends nothing.
6. Type the displayed destination confirmation and select **Apply reviewed replay**
   once. Read the per-record outcome before taking another action.

## Check the result

An **acknowledged** record means Kafka accepted that write. Retain its receipt
and destination position, then check the recovery consumer's logs or business
result separately. Broker acceptance does not prove the order was processed.

| Outcome                                | What to do                                                                                                                      |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Acknowledged                           | Verify downstream processing; do not replay again just because a screen has not updated                                         |
| Explicitly rejected                    | Correct the reported failure before preparing another review                                                                    |
| Definitely unsent                      | If continuing is appropriate, use [Review definitely unsent records](../guide/record-replay.md#continue-an-interrupted-attempt) |
| Uncertain                              | The write may already exist; inspect history and destination evidence before any new attempt                                    |
| Receipt storage or cleanup unavailable | Preserve recovery state and resolve the failure before new work                                                                 |

Open **Repair history** to inspect the attempt. Reopening history sends nothing.
An equivalent destination record does not prove this uncertain attempt sent it.
Use the [uncertainty procedure](../guide/record-replay.md#inspect-uncertainty-and-archive-known-chains)
when the receipt cannot establish the outcome.

Replay leaves source records and consumer offsets unchanged. It does not skip
the failed record or prevent a loop if the same failing consumer reads the copy.
Agree those follow-up actions separately, then [share a reviewed handover](share-findings.md).

## Watch the walkthrough

A 42-second recording of the actual workbench with on-screen instructions and no
audio. Correct and replay one illustrative JSON record to a separate, owned AIO topic, then inspect the acknowledgement.

<video class="skope-walkthrough" controls muted playsinline preload="metadata"
       poster="../../assets/skope/replay-record.png"
       aria-label="Walkthrough: Review and replay a failed record"
       aria-describedby="replay-record-video-summary">
<source src="https://github.com/asadarafat/streamskope/releases/download/documentation-media/v1/replay-record.mp4" type="video/mp4">
  <track kind="captions" src="../assets/skope/replay-record.vtt" srclang="en" label="English instructions">
  <a href="https://github.com/asadarafat/streamskope/releases/download/documentation-media/v1/replay-record.mp4">Download the walkthrough</a>.
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

For size, timing, fidelity and storage bounds, use the [replay reference](../reference/replay.md).
To create a new test event instead, use [Produce and verify one message](../guide/produce-message.md).
