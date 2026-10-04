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
Lag is the **offset distance** `max(end offset - committed offset, 0)` for each
partition when both offsets are available. It is not an exact remaining-record
count: offset gaps, compaction and transactional records can make those differ.
The committed offset normally identifies where the group resumes, and the end
offset is a boundary, not the offset of a last displayed record.

An unavailable or negative offset produces an unknown lag, not zero. Zero means
no positive offset distance was reported; it does not prove the application has
finished processing. Membership, commits and end offsets are sampled through
separate requests and can change between reads. Lag does not measure processing time.

<figure class="product-shot">
  <img data-sk-light="consumers.png" data-sk-dark="consumers-dark.png" alt="Consumer group offsets reported by AIO Kafka" width="2880" height="1800" loading="lazy" decoding="async">
  <noscript><img src="../assets/consumers.png" alt="Consumer group offsets reported by AIO Kafka" width="2880" height="1800" loading="lazy"></noscript>
</figure>

An idle **Empty** group may retain offsets. A deleted group disappears from the
inventory; loading a stale selection can return **Dead** with no offsets or a
not-found response. Neither result establishes zero lag. Permission failures
remain errors; StreamSkope does not invent member or offset data.

If no groups appear, check that a consumer has created a group in your cluster
and that your account has permission to describe it.

To change where a stopped group resumes, follow
[Preview and reset consumer offsets](offset-recovery.md). Preview is read-only;
application requires exact confirmation and a fresh inactive-group baseline.

For timestamped selected-topic history and local thresholds, use
[Observed health](observed-health.md). Its bounded samples distinguish gaps,
missing offsets and stale evidence.

## Stream monitor

Use the topic's **Monitor** tab to understand the current StreamSkope read:
is it publishing records, waiting for data, buffering work or omitting records
from the display? This is the application's delivery path. For partition,
replication and consumer-group evidence, open [Observed health](observed-health.md).

1. Check the connection, topic, read mode, operation state and sample age.
2. Compare the host publication rate with queue occupancy and oldest queued age.
3. Inspect historical display loss and its reasons, even after pressure recovers.
4. Use **Stop** or **Cancel** in Monitor to end the same read started in Messages.
   Switching tabs does not start another Kafka consumer.

| Evidence             | Meaning and limit                                                                                                                                                                                            |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Host publication     | Records emitted by the host toward the UI. Publication is not confirmation that every record reached or painted in the renderer.                                                                             |
| Queue and oldest age | Records waiting at the host, bounded by both count and bytes. Age follows the oldest record still waiting, including across partial drains.                                                                  |
| Current pressure     | Transport pause, a queue at its count limit, or bytes nearing their capacity. It can recover while historical display loss remains nonzero.                                                                  |
| Display loss         | Records omitted from the application's delivery path. Host reasons distinguish count capacity, byte capacity, an oversized transfer record and a terminal discard. This does not mean Kafka deleted records. |
| Publication rate     | A measured interval, including zero after an active quiet interval. An unavailable or stale sample is not zero.                                                                                              |
| Freshness            | The time of the aggregate host observation. Last publication, queue-wait and renderer measurements retain their own timestamps. A fresh observation does not make old work measurements fresh.               |

The queue and publication charts share a time window. Open **Diagnostics** for
renderer measurements, effective limits and exact sample tables. Application FPS
measures visible document frames, not message-table throughput. Message-workspace
render/filter timings are measured while **Messages** is mounted; in Monitor they
are unavailable or explicitly last measured with their age. Normal eviction as
the selected message window advances is separate from overload omissions.

Stop first cancels the Kafka read, then performs bounded publication while the
transport can accept it. If pressure or the terminal budget leaves queued records,
they are counted as terminal display omissions. A cleanup timeout or close failure
is reported as a failure; it is not confirmation that all resources closed.
If Stop times out while cleanup continues, **Retry stop** checks that same request.
Last terminal evidence remains inspectable. Starting another read, including the
same topic and settings, gives it new evidence; old callbacks cannot become the
new operation's measurements.

Monitor history is bounded, local to this workbench session and contains aggregate
measurements, not message payloads. Sampling does not poll the broker or produce
messages. For retained records and export limits, see
[Data, exports and limits](data-handling.md#message-limits).

## Raw logs

1. Expand **Raw logs** at the bottom of the workbench.
2. Filter by the operation's text or severity.
3. Read the outcome and correlation ID. Scroll upward to pause following.
4. Choose **Resume live** to follow new entries again.
5. Copy or download the relevant entries if you need to share evidence.

Review logs before sharing, even though secrets are redacted. Use
[troubleshooting](troubleshooting.md) to work through a failed operation.

## Latency

Follow [Run a latency probe](latency.md) for an explicit producer/consumer timing
measurement. It writes records to the selected topic. It does not measure this
consumer group's processing latency. The linked runbook covers confirmation,
partial results, stopping and recovery.

## Next: inspect a schema

[Read a Schema Registry version →](schema-registry.md)
