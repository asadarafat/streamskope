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
