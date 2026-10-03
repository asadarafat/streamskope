# Preview and reset consumer offsets

Resetting changes where a consumer group resumes. It can reprocess records or skip
unprocessed records. Stop **every consumer in the group** before applying a reset.
Inspect [consumer lag](operations.md) first and retain the original positions if
you may need to restore them.

1. Open **Consumer Groups**, choose the group, then **Reset offsets…**.
2. Select the topic partitions to change, up to 32. Enter each proposed next offset
   as an integer. Unselected partitions are outside the operation.
3. Choose **Preview reset**. This reads group state, permissions, committed offsets
   and retained bounds; it does not commit offsets.
4. Review the before/after table. The earliest offset is inclusive; the end offset
   is the next position after existing records. Targets must stay within those bounds.
5. Examine the replay upper bounds and up to three sample records. Samples show
   only Base64 key/value prefixes, at most 192 bytes each. An unavailable sample is
   not proof of an empty partition or permission to commit.
6. Type the exact group ID and choose **Apply reviewed reset**. Read-only mode
   permits preview and blocks apply. Masking blocks this original-record preview.
7. Inspect every partition result and refresh the group before restarting consumers.

## What the preview establishes

Replay exposure is an offset distance from the proposed position back to the prior
commit, limited by the sampled end offset. With no previous commit it uses the
sampled end. It is an **upper bound**, not a record count: compaction, retention,
aborted transactions and gaps change how many records can actually be reprocessed.
Moving forward can skip data. Later records are outside this snapshot.

Group READ authorization is requested from Kafka; older brokers may report it as
unknown. Topic permissions remain authoritative at dispatch. Preview cannot reserve
permission or prevent another operator from changing offsets.

## Stale plans and partial results

A review expires after two minutes and is tied to its connection. Before each
partition is sent, the host checks that the group is still inactive and every
selected committed offset matches the expected baseline, including positions
already acknowledged by this operation. A changed baseline, denied permission or
invalid retained bound stops the remaining work. Kafka does not offer an atomic
compare-and-reset API: a consumer or another operator can still race the final check.

| Result                   | Meaning                                                              | Next action                                                     |
| ------------------------ | -------------------------------------------------------------------- | --------------------------------------------------------------- |
| Acknowledged, verified   | Kafka accepted the position and read-back matched.                   | Refresh before restarting consumers.                            |
| Acknowledged, unverified | Kafka accepted it; read-back failed or differed.                     | Inspect committed offsets; do not resend to retry verification. |
| Rejected                 | Kafka explicitly refused the partition, or dispatch could not begin. | Correct the cause and create another preview.                   |
| Unknown                  | The request may have reached Kafka.                                  | Reconcile the actual committed position before another attempt. |
| Unsent                   | This partition was never dispatched by the plan.                     | Review why execution stopped.                                   |

The host stops after the first failed or unverified partition and starts no new
dispatch after 60 seconds. In-flight requests settle within their transport timeout.
Repeating the same plan returns its recorded result; it does not commit again.
Raw logs retain per-partition outcomes. This is not an atomic, all-partition rollback.
