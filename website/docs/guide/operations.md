# Check consumer lag

Find a consumer group and compare where it has committed with the end of each
partition. Start with a [connected profile](connections.md) and a group your
account can describe.

## Consumer groups

1. Open **Consumer Groups** in the left navigation.
2. Search for the group you want to inspect and select it.
3. Check its state and refresh time.
4. Read the offsets table: compare the committed offset, end offset and confirmed
   lag for each topic partition.
5. Refresh the group and compare the new values when you need another sample.

**You should see:** the group's reported offsets and any available members and
assignments. A group can have committed offsets without active members.
Lag counts records relative to the reported end offset; it does not measure
processing time.

![Consumer group offsets reported by AIO Kafka](../assets/consumers.png#only-light)
![Consumer group offsets reported by AIO Kafka](../assets/consumers-dark.png#only-dark)

If no groups appear, check that a consumer has created a group in your cluster
and that your account has permission to describe it.

## Stream monitor

If messages stop updating or the display falls behind:

1. Return to the topic and open **Monitor**.
2. Check the stream state and freshness before interpreting counters.
3. Inspect delivery, host queues and renderer retention to locate the backlog.
4. Compare history evictions with display-drop counters.

**You should know:** whether the view is receiving data, waiting, stopped or
reporting degraded delivery. Normal history eviction and overload drops are
separate conditions.

## Raw logs

1. Expand **Raw logs** at the bottom of the workbench.
2. Filter by the operation's text or severity.
3. Read the outcome and correlation ID. Scroll upward to pause following.
4. Choose **Resume live** to follow new entries again.
5. Copy or download the relevant entries if you need to share evidence.

Review logs before sharing, even though secrets are redacted. Use
[troubleshooting](troubleshooting.md) to work through a failed operation.

## Latency

For an explicit producer/consumer timing probe:

1. Open the topic's **Latency** workspace.
2. Review the target topic, record count and timeout. The probe sends actual
   Kafka records, so use a topic where you are allowed to produce.
3. Start the probe and wait for its result or reported failure.
4. Export the report with its context when comparing runs.

The result describes this probe and environment, not an application-wide SLA.

## Next: inspect a schema

[Read a Schema Registry version →](schema-registry.md)
