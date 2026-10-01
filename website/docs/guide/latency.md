# Run a latency probe

A latency probe **produces real Kafka records** and observes its own results.
Use a disposable or explicitly approved diagnostic topic. It is separate from
[passive message inspection](messages.md) and [consumer lag](operations.md).

## Before you start

Connect the intended profile and confirm permission to produce, read and describe
the selected topic, and to read StreamSkope's generated latency groups. Check
[the permission table](security.md#kafka-access-and-effects) for the boundary.
Existing records and downstream consumers can be affected by diagnostic traffic;
agree on the target topic before starting.

## Run and inspect

1. Select the topic and open **Latency**. Review the active connection and target.
2. Set **Probe records**, **Kafka acknowledgements** and **Probe timeout**. These
   controls apply to the next run, not evidence from a completed run.
3. Choose **Run latency probe**, review the confirmation, and start the probe.
4. Wait for completion or choose **Stop latency probe**. Stopping does not remove
   records already produced.
5. Read the result state and observed count before the averages or P95. A partial,
   failed or stale result is not a complete successful measurement. Inspect the
   network, produce, fetch and publish-to-observe measurements for their own scope.
6. Use **Export latency JSON** to retain the run context and results. Compare runs
   only with their topic, acknowledgement mode, record count and environment recorded.

**Expected result:** evidence for the records observed during this probe, with its
settings and outcome. This is not your application consumer's processing latency,
an end-to-end business transaction measurement, or an application-wide SLA.
P95 from a small sample should not be treated as a stable capacity estimate.

## Recover

If producing is denied, check the topic and account permissions. If produces
succeed but observations time out, check read permissions, broker reachability and
the reported stage in **Raw logs**. A timeout can leave probe records in Kafka.
Confirm the previous run's state before starting another; a retry adds another
set of records. Use your site's retention policy for the diagnostic topic.

Changing settings or stopping a probe does not delete historical evidence. Review
exported JSON before sharing it; see [data and exports](data-handling.md).
