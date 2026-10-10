# Investigate observed Kafka health

Use **Observed health** to investigate one topic and, optionally, one consumer
group on your connected profile. It uses Kafka metadata, offsets and bounded
protected record reads. Its findings describe the selected resource from this
client; they do not establish cluster-wide health or successful consumer processing.

## Start an investigation

1. Connect a profile, then open **Observed health**.
2. Select an existing topic and optionally a consumer group. Refresh the resource
   lists if a recently created resource is missing.
3. Choose **Capture observation** for one reading, or **Start observing** to
   collect repeatedly. The controls show collection progress and the cooldown
   before another attempt is allowed.
4. Read the current findings and measurements first. Open the affected topic,
   consumer group or sampled record to continue the investigation.

Local lag and request-time thresholds and optional record sampling are available
in the collection settings. A threshold breach is an observation-specific finding;
Activity records entry into a breach without repeating it on every sample.

## Decide what to investigate next

| Evidence                         | What it means                                                                | Useful next step                                                                              |
| -------------------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Partition without a known leader | Selected-topic metadata reports no current leader                            | Inspect the affected partition and broker availability                                        |
| Fewer ISR than assigned replicas | Selected-topic metadata reports a replication gap                            | Check the affected partition and broker replication metrics                                   |
| Lag and no visible group members | Committed positions trail the topic's ends, with no visible members          | Open the group and check whether its application is intentionally stopped                     |
| Append positions outpace commits | New offset positions advance faster than committed positions                 | Inspect consumer capacity and commit policy                                                   |
| Commits appear stalled           | Several recent observations show lag without commit progress                 | Check consumer logs; stopped consumers, batching and poison records are possible explanations |
| Uneven partition growth          | A sufficiently complete interval concentrates offset growth in one partition | Inspect partitioning and available sampled key evidence                                       |
| Unknown or partial measurements  | The read could not establish that value                                      | Read its coverage reason and follow the specific recovery action                              |

Use the partition table's filtering and sorting to find leader, replication and
lag problems. Group and topic actions open their existing workspaces. **Find
sampled record** requests the exact partition and offset through the protected
Topics reader; other records with similar offset text do not match. A compacted,
deleted or out-of-window record may no longer be available. The read's coverage
reports whether it completed or hit a bound.

**No supported finding** does not mean **healthy**. A stable group does not prove
processing success, and advertised broker/controller presence does not prove
each broker is reachable. Broker CPU, disk and end-to-end message latency are
not measured here.

## Freshness, coverage and recovery

Each reading includes its source, observation time and measurement coverage.
A sample becomes stale after **45 seconds**. Retained history can belong to a
different connection: the running host must confirm that the selected sample and
resource identity were captured on its still-current original connection. Page
navigation and Stop preserve that completed evidence; a connection replacement or
host restart requires a new explicit capture. Failed reads do not create zero
measurements, and stale evidence produces no current diagnosis or forecast.

When the host is unreachable, watch status is unknown and capture and resource
links stay disabled. A browser transport interruption alone does not stop the
host watch. Restore transport and attach to its current status. Disconnect, vault
lock, connection replacement and host shutdown revoke collection; recovering
from those transitions requires an explicit new capture or Start and never grants
unattended connection or unlock.

Group-access failures leave available topic evidence intact. Selected-topic lag
uses that topic's committed and end positions; omitted unrelated group members
or assignments do not by themselves invalidate those positions. Missing,
omitted or ahead-of-end commits still produce unknown lag.

Collection errors distinguish invalid selections, access failures, timeouts,
disconnection, cancellation, cooldown and unreadable history. Follow the shown
recovery action rather than treating every failure as an invalid topic. An
unreadable or unsupported history file remains preserved until explicitly cleared.

## Collection and history limits

- One collection runs at a time, with a **15-second deadline** and **1–128
  selected-topic partitions**. The deadline cancels reads; Stop still waits for
  their original provider cleanup and cannot force ownership release.
- Repeated collection waits at least **ten seconds after each completed attempt**.
  A one-shot attempt also observes the host cooldown.
- **Start observing** explicitly authorizes one host-owned watch on the original connection and topic identity. It continues across page navigation while that host is running and unlocked. Reopening the page attaches to its status without starting a second collector.
- **Stop observing**, disconnect, connection replacement, vault lock and app shutdown revoke collection and wait for original work to settle. A browser transport interruption makes status unknown; reconnect to check the host rather than assuming collection stopped. A failed read stops the watch and requires an explicit retry.
- The first positive broker and topic identity binds the watch. A replaced topic stops collection before group or record reads; earlier evidence remains separate. Watch authority is session-only and does not survive host restart or grant unattended connection or unlock.
  The host can add local threshold Activity entries while the page is closed.
  No external notification service is configured. Stopping collection preserves
  completed evidence for inspection while its original connection remains current.
- Desktop history is stored privately in `history/kafka-observations.json` inside
  application data. Browser development retains history only for the host session.
- History loaded or updated by the host is bounded by eight resource identities, **240 samples per identity**,
  24 hours of age and 4 MiB total. At a ten-second cadence, the sample cap holds
  approximately **40 minutes plus collection time**, rather than a full day of
  continuous readings. Older evidence is evicted first.

The 24-hour cutoff filters loaded history; it does not schedule deletion. On
desktop, expired entries remain on disk until a successful capture rewrites
history or you explicitly clear it. There is no background erasure.

Only aggregates, offsets and Kafka resource identities are retained. Payloads,
raw keys, member identities and credentials are excluded. Clear history through
its explicit confirmation action; clearing removes all retained series,
including other profiles. See [backup and recovery](recovery.md) before managing
application data.

## Optional record sampling

Enable **Sample records for size and key distribution** before collection. Reads
use a separate protected reader and leave the active Topics request unchanged.
Each sample remains limited to **200 records, 2 MiB and five seconds**, within the
overall collection deadline.

The sampling window adapts to recent observed offset growth, using a bounded
window of 60 seconds, 10 seconds, one second or 100 milliseconds. Shorter windows
can provide complete evidence on busy topics without increasing the record or
byte budget. The view reports the actual window, count, coverage, mean/p95 record
size and key locators. This is selected-window evidence, not a representative
census of topic traffic.

Incomplete or capped windows retain their measured statistics, but do not qualify
full-window key or record-size inference. Size comparisons require complete,
comparable windows with at least 20 records. Changing the sampling work starts a
separate request-time baseline. Overlapping windows can reuse records.

Raw keys, values, headers and previews are discarded after aggregation. Masked or
unavailable original bytes cannot identify a key; null keys are counted separately.
Unavailable keys suppress frequency hints but do not suppress size comparisons
when original byte sizes and complete window coverage are available.
Key ranks apply to one sample only. Size and frequency aggregates may still reveal
traffic patterns; clear observation history to remove them.

## Optional analysis and methodology

Descriptive lag, append-position and commit-position trends lead the investigation.
The analytical details explain their evidence and limitations:

| Analysis       | Evidence required                                                                     | Interpretation and limits                                                                                                                                                                                                             |
| -------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Lag scenario   | Nine complete samples over at least one minute in the same group state; up to 20 used | A linear scenario 60 seconds ahead. The last three samples are held out; reject when mean absolute error exceeds 20% of mean lag, with a five-position floor. The displayed range is heuristic, not a statistical confidence interval |
| Anomalies      | Eight prior comparable continuous samples; up to 12 form the baseline                 | A change must exceed six median absolute deviations, 50% of the median and the metric floor: 1 position/s, 10 ms or 32 bytes. Three successive deviations indicate a changing baseline                                                |
| Partition skew | Complete end-offset changes across at least two partitions and 20 positions           | Suspect skew when one partition has at least 80% of growth and at least 1.5 times its uniform share                                                                                                                                   |
| Frequent keys  | A complete recent sampled window with at least 20 available non-null keys             | A key accounting for at least half of those keys receives a window-specific hint; it does not establish topic-wide key frequency                                                                                                      |

Offset changes are **positions per second**, not exact message throughput.
Compaction, control records and manual resets affect their meaning. Request time
is client elapsed collection time, including sequential reads and host scheduling;
it is not broker processing latency. Gaps over 45 seconds, restart/stop boundaries,
changed partition identities and backwards offsets break continuity. Heuristics
can miss problems and produce false positives.
