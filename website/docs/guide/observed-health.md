# Observe Kafka health over time

Connect a profile, then open **Observed health**. Select one existing topic and optionally a consumer group. **Capture observation** reads once; **Start observing** repeats at least ten seconds after each completed attempt. **Stop observing**, navigation away, disconnect and app shutdown stop collection. The host allows one collection at a time and a 15-second deadline.

## What the readings mean

| Reading                     | Source and meaning                                       | Limits                                                                                                      |
| --------------------------- | -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Broker/controller presence  | Kafka metadata advertises broker IDs and the controller  | Advertisement does not prove each broker is reachable; CPU/disk are unavailable                             |
| Leader and in-sync replicas | Selected-topic metadata                                  | 1–128 partitions; no all-topic health claim                                                                 |
| End and committed positions | Kafka latest offsets and selected consumer-group offsets | Observations are sequential, not an atomic broker snapshot                                                  |
| Consumer lag                | End minus committed position for the selected topic      | Unknown for missing, omitted or ahead-of-end commits; offset positions are not exact readable record counts |
| Group state and members     | Group description                                        | A stable group does not establish processing success                                                        |
| Request time                | Client elapsed collection time                           | Includes transport/host scheduling; not broker processing latency                                           |

Each observation shows source, timestamp, coverage, state and elapsed request time. A sample is stale after 45 seconds. Failed reads do not create zero measurements; later successful samples start a new history segment. A restart or explicit stop also breaks continuity. Resource identities separate cluster/topic recreations.

## Local thresholds

Optionally set a lag threshold or request-time threshold before collecting. Only available values strictly above the configured threshold trigger. The page shows each sample's breach; Activity records entry into a breach without repeating it on every sample. No notification service runs when the page or desktop is closed. Missing data means unknown, not recovered or healthy.

## Retention and removal

Desktop history is stored privately in `history/kafka-observations.json` inside application data. Browser development retains it only for the host session. History retains up to eight cluster/topic/group identities, 240 samples per identity, 24 hours and 4 MiB in total; oldest evidence is evicted first. Timestamps remain unchanged across restart, so old evidence is labelled stale.

Only aggregates, offsets and Kafka resource identities are retained. Payloads, raw keys, member identities and credentials are excluded. **Clear all observation history** requires typing `CLEAR HISTORY`; it deletes every retained series, including history from other profiles. An unreadable or unsupported file is preserved until explicitly cleared. See [backup and recovery](recovery.md) before managing application data.

## Explain the evidence

Below the history, **Explain these observations** shows bounded analysis. Offset growth
and commit progress are **positions per second**, not exact message throughput or proof
that a consumer processed records. Compaction, control records and manual offset resets
can change their meaning. Requests include sequential metadata/group reads; their elapsed
time is client observation cost, not broker or end-to-end message latency.

| Analysis       | Evidence required                                                                 | Interpretation and limits                                                                                                                                                                                                                                                            |
| -------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Lag scenario   | Nine complete samples over at least one minute, same group state; up to 20 used   | A linear projection 60 seconds ahead. The last three samples are held out for a backtest. Reject when mean absolute error exceeds 20% of mean lag, with a five-position floor. The displayed range scales held-out error and residuals; it is not a statistical confidence interval. |
| Anomalies      | Eight prior continuous samples; up to 12 form the baseline                        | Compare offset-position growth, request time and sampled mean record size. A change must exceed six median absolute deviations, 50% of the median and the metric floor (1 position/s, 10 ms or 32 bytes). Three successive deviations are labelled a changing baseline.              |
| Partition skew | Complete end-offset changes across at least two partitions, at least 20 positions | Suspect skew when one partition has at least 80% of growth and at least 1.5 times its uniform share. This is selected-topic evidence.                                                                                                                                                |
| Frequent keys  | At least 20 available non-null keys in a recent sample                            | A key accounting for at least half of these keys receives a hot-key hint. Capped reads may favour some partitions; the sample is not a cluster traffic census.                                                                                                                       |
| Hypotheses     | Recent metadata, group states, offsets and sample timestamps                      | Stalled commits, append/commit imbalance, state changes and replication gaps suggest checks. Consumer logs and broker metrics are needed to establish a cause. A poison record is only one possible explanation for stalled commits.                                                 |

Gaps over 45 seconds, changed process/collection segments, partition identities and
backwards offsets break continuity. Incomplete lag or a group-state change prevents
projection across that boundary. Stale evidence produces no current forecast, anomaly
classification or diagnosis. Heuristics can produce false positives and miss problems;
**no hint** does not mean **healthy**.

## Optional record sampling

Enable **Sample records for size and key distribution** before collection. Each
observation uses a separate protected reader for the preceding minute (end excluded),
limited to **200 records, 2 MiB and five seconds**, within the overall 15-second deadline.
This adds broker reads and may overlap the preceding sample. It leaves the Topics reader
unchanged and stops on cancellation or navigation.

The view reports coverage, mean/p95 original record byte size, partition counts, and
ranked key counts with example partition/offset locators. Complete window coverage with
at least 20 records is required for a record-size anomaly baseline. Switching sampling
on or off starts a separate request-time baseline because the work has changed.

Raw keys, values, headers and previews are discarded after aggregation. Masked or
unavailable original bytes cannot identify a key; null keys are counted separately.
Key ranks apply to one sample only. To inspect a permitted example, open its partition
and offset in **Topics**. Size and frequency aggregates are retained with history and
may still reveal traffic patterns; use **Clear all observation history** to remove them.
