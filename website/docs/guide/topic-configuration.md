# Change topic configuration

This workflow writes named settings to the selected Kafka topic. Start with a
[connected profile](connections.md), the intended topic, and an approved change.
Reading needs `DescribeConfigs`; applying needs `AlterConfigs` on that topic.
Review [permissions](security.md#kafka-access-and-effects) with the administrator.

## Read and prepare

1. Open **Topics**, select the exact topic, then open **Configuration**.
2. Choose **Refresh configuration**. Check the connection, topic, current value,
   source, and whether the setting is inherited/default or read-only.
3. Search for the setting and select its row. Read the broker-provided description.
4. Enter **Proposed value**, then **Queue change**. Review each pending name and
   current/proposed value. **Remove** drops one queued change; **Clear** drops all.

Queuing is local preparation; it does not change Kafka. Read-only entries cannot
be queued. Advanced presets can queue several changes: inspect every named value
and any skipped settings before continuing. A preset is not a capacity recommendation.

## Validate and apply

1. Choose **Dry-run changes**. Wait for the broker validation result and resolve
   reported failures. Validation does not apply the change or reserve the state;
   another administrator may change the topic afterwards.
2. Choose **Apply changes**. In the confirmation dialog, verify the target and
   exact named changes, then choose **Apply named changes**.
3. Wait for the result. Choose **Refresh configuration** and compare the broker's
   current values with the intended values. Read warnings even after a successful
   write; a subsequent refresh or history write can fail independently.
4. Open **Configuration history**. Distinguish **Validate** from **Apply**, inspect
   success/failure and timestamp, and retain the correlation ID from **Raw logs**
   when following your site's change-record process.

**Expected result:** the refreshed broker values match the approved change, and
the history identifies the attempted operation. History may be durable,
session-only or unavailable; its status is shown in the dialog. It is not a full
broker audit log and cannot reconstruct other administrators' changes.

## Recover from an uncertain or unwanted change

| Situation                              | Action                                                                                                                         |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Denied or read-only setting            | Ask the administrator to check the exact operation and scope; do not broaden permissions blindly                               |
| Stale view or validation failure       | Refresh, inspect the new baseline, then review the pending values again                                                        |
| Timeout or lost connection after Apply | Reconnect and read the actual broker values before retrying; lack of a response does not prove the write failed                |
| Applied value is wrong                 | Review the prior value and source with the administrator; queue a new corrective change, dry-run and confirm it                |
| Previously inherited setting           | Setting an old displayed value creates an explicit value; restoring inheritance may require the administrator's broker tooling |
| History unavailable                    | Use the broker's current state and your external change record; restore history storage separately                             |

There is no automatic rollback button. Configuration history is bounded and can
hide sensitive values. Do not treat it as a complete configuration backup.

[Inspect records after the change →](messages.md)

## Create a topic

From **Topics**, choose **Create topic**. Enter its name, partition count,
replication factor and optional settings as a JSON array of `name`/`value` pairs.
Unspecified settings inherit broker defaults. StreamSkope checks the name,
available brokers and that the topic does not already exist; Kafka checks
permissions and configuration validity when you confirm.

Review the connection and all requested properties, then choose **Confirm create**.
The two-minute review applies only to that connection. Existing topics are never
adopted or changed by creation. Creation requires read-only mode to be disabled.
After Kafka acknowledges, the app reads back partition and replica counts and
refreshes the inventory. If refresh fails, the acknowledgement remains valid.
For an unknown result, refresh and inspect the topic before another attempt;
there is no automatic retry and no rollback of an acknowledged topic.

## Acknowledgement and recovery

A broker acknowledgement is retained even if the next configuration read or local
history write fails, or the connection changes. Refresh or repair local history
separately; do not apply the configuration again just to refresh its display.
If the write was not acknowledged, inspect the selected keys on the broker before
another attempt. A lost response or cancellation does not prove rollback.

## Increase partitions or delete a topic

Open the topic and choose **Manage topic…**. These actions require host write mode
and the corresponding broker permission. Internal topics and names beginning with
`__` are refused.

For **Increase partitions**, enter the **new total partitions**, up to 4096. Review
the current count, topic UUID and requested total, then type the displayed exact
confirmation and choose **Apply reviewed change**. Expansion cannot shrink or undo
partitions. It changes key routing and can break ordering across old and new
partitions; coordinate with producers and consumers first.

For **Delete topic**, stop this topic's current StreamSkope read and coordinate
with downstream users, then review the topic UUID before confirming
`DELETE <topic>`. This removes the topic and its retained messages with no undo.
The host requires UUID metadata and the UUID-based deletion API; an older broker
without these capabilities is explicitly refused. A recreated topic with the same
name invalidates a review and cannot be deleted by the old UUID request.

Reviews expire after two minutes. Apply freshly checks the connection, UUID,
partition count, replica assignment and visible permissions. Editing an input
requires another review. Kafka's expansion API addresses the topic by name: avoid
concurrent deletion/recreation even after fresh revalidation. StreamSkope does not
claim an atomic lock against another administrator.

Read the receipt before closing. After deletion, closing the receipt refreshes the
inventory and returns to the topic list. **Acknowledged** is a broker acknowledgement;
readback can independently be **verified**, **different** or **unavailable**.
Refresh without repeating an acknowledged mutation. **Unknown** means the request
may have reached Kafka; inspect the actual resource and Activity before another
attempt. **Unsent** means admission/revalidation prevented dispatch. A cleanup
warning preserves the receipt and fences new topic actions until the original
clients are closed through reconnect. Reviews and receipts are bounded and
session-local, so inspect Kafka after a host restart. No automatic retry occurs.

For a cross-environment change, follow [Compare environments](environment-comparison.md).
