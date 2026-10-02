# Find and inspect a message

Open a topic, narrow the records on screen, and read a message with its key,
headers and offset. Start with a [connected profile](connections.md).

## Open a topic

1. Open **Topics** in the left navigation.
2. Select the topic you want to inspect. In the [development sandbox](../start/development.md), use `test`.
3. Wait for the **Messages** table to load.

**You should see:** records from the selected topic, or a stream waiting for data.

<figure class="product-shot">
  <img data-sk-light="topics.png" data-sk-dark="topics-dark.png" alt="Topics on the local AIO Kafka broker" width="2880" height="1800" loading="lazy" decoding="async">
  <noscript><img src="../assets/topics.png" alt="Topics on the local AIO Kafka broker" width="2880" height="1800" loading="lazy"></noscript>
</figure>

## Read and filter

1. If a read is running, click **Stop tail** or **Cancel fetch** to enable its settings.
2. Use **Read** to choose **Tail** for ongoing traffic, or **Newest N**, **First N** or **Time window** for a bounded read. Set **Limit** to the number of records you need.
3. Click **Start tail** or **Load messages**, depending on the chosen mode.
4. Once the records you need are present, click **Stop tail** for a stable view
   or wait for the snapshot to complete.
5. Copy a key from a visible record, then open **Filters**.
6. Paste that key into **Key contains** and select a matching row.
7. Clear the filter to restore the other loaded records.

**You should see:** only matching records in the current view. Filters work on
records available to the workbench; they do not search the entire cluster.
If nothing matches, clear the filter and check the topic and read mode.

## Choose a read mode

| Read mode   | What it requests                                            | Completion                                                 |
| ----------- | ----------------------------------------------------------- | ---------------------------------------------------------- |
| Tail        | A recent starting window, then incoming records             | Runs until stopped; the display remains bounded            |
| Newest N    | Recent offsets from each partition, capped by Limit overall | Ends at the captured partition ends or the record limit    |
| First N     | Earliest retained offsets in each partition                 | Ends at the captured bounds or the record limit            |
| Time window | Last 2 minutes, or your explicit custom start/end interval  | Ends at the time-derived offset bounds or the record limit |

Limit is an overall fetch cap for bounded reads, not a promise of that many records
from each partition. Newest N is not a globally sorted latest-N query: partition
arrival order and the cap affect which records you receive. First N cannot recover
records already removed by retention or compaction. The active time-window bounds
are displayed in UTC.

For a past incident, select **Time window → Custom interval**. Enter an inclusive
**Start time** and an exclusive **End time**, using ISO 8601 with seconds and a
time zone: `2026-07-24T16:03:00+02:00` and `2026-07-24T14:03:00Z` represent the
same instant. Check **Requested interval in UTC** before loading. Missing time
zones, impossible dates, and end times at or before the start disable loading.
The chosen settings stay available after completion or cancellation so you can
adjust and repeat the read. **Last 2 minutes** continues to resolve its bounds
when you click **Load messages**.

Kafka resolves time-window boundaries to offsets per partition. This is not a
payload-time search or a guarantee of completeness when timestamps are out of
order. **Timestamp contains** only filters already-loaded timestamp text.

### Example: inspect a past incident and save the result

Suppose an error happened at 14:03 UTC on `orders.events`:

1. Select that topic, stop any active read, select **Time window**, and set **Limit**
   to `1000`. Choose **Custom interval**, enter that date's start and end times
   around the incident with `Z` for UTC, and click **Load messages**.
2. Note the UTC range shown by **Active fetch request** and wait for completion,
   or use **Cancel fetch** to stop the read. History removed from Kafka cannot be recovered.
3. Open **Filters**, set **Key contains** to a known affected key, and inspect
   matching records' partition, offset and timestamp.
4. Clear the filter if you need the rest of the loaded sample. Reaching the limit,
   an empty match or a timeout does not prove that the interval has been fully examined.
5. Use **Export** as described below and retain the UTC range separately alongside
   the export; the file is a bounded sample, not a complete incident archive.

## Inspect a record

1. Select a row to open **Message details**.
2. Choose **Value**. For JSON, compare **Formatted JSON** and **Raw**.
3. Choose **Key** to inspect the record key.
4. Choose **Metadata** to read its partition, offset, timestamp and headers.
5. Use **Copy** when you need the value elsewhere.

**You should have:** the payload and the topic, partition and offset needed to
find its position again. An offset belongs to one partition; it does not order
records across the whole topic.

<figure class="product-shot">
  <img data-sk-light="messages.png" data-sk-dark="messages-dark.png" alt="Message inspector for an orders.events record on AIO Kafka" width="2880" height="1800" loading="lazy" decoding="async">
  <noscript><img src="../assets/messages.png" alt="Message inspector for an orders.events record on AIO Kafka" width="2880" height="1800" loading="lazy"></noscript>
</figure>

## Export the records you need

1. Stop an active tail or wait for the bounded read to finish so the sample is stable.
2. Set the filters you want. The shown/retained count indicates how much of the
   retained sample is visible; selecting a row does not limit the export to that row.
3. Click **Export** (accessible name **Export filtered JSON**). Desktop opens a save
   dialog; browser development downloads JSON. There is no format or scope picker.
4. Open the saved JSON and check its topic, filters, retained/exported counts,
   stale-state marker and record truncation fields before using it as evidence.

Only filtered records still retained in the workbench are exported. Review
[Data, exports and limits](data-handling.md) for plaintext contents, byte limits
and incomplete-evidence fields. Older records may have left the window; **Monitor**
distinguishes ordinary history eviction from overload drops. An export error means
no successful export was confirmed; narrow the sample and retry.

## Next: check a consumer

[Check consumer lag and offsets →](operations.md)

## Rules and configuration

For a repeatable check against a real record:

1. Copy the record's JSON value, then open **Rules**.
2. Click **Create rule**, or select a saved rule. Review its **JSONPath expression**, **Topic filter**
   and severity; use **Syntax** for the supported expression forms.
3. Open **Test**, paste the copied value into **Sample JSON**, and choose
   **Evaluate selected rule**.
4. Read the result and any stale-result notice before relying on it.

The test runs locally in the host. The topic filter controls where a saved rule
applies; opening a topic does not limit every rule to that topic.

To change broker settings, follow [Change topic configuration](topic-configuration.md)
for preparation, dry-run validation, confirmation and recovery. To measure a
producer/consumer round trip, use [Run a latency probe](latency.md); it writes
records to Kafka.
