# Find and inspect a message

Open a topic, narrow the records on screen, and read a message with its key,
headers and offset. Start with a [connected profile](connections.md).

## Open a topic

1. Open **Topics** in the left navigation.
2. Select the topic you want to inspect. In the local quickstart, use `test`.
3. Wait for the **Messages** table to load.

**You should see:** records from the selected topic, or a stream waiting for data.

![Topics on the local AIO Kafka broker](../assets/topics.png#only-light)
![Topics on the local AIO Kafka broker](../assets/topics-dark.png#only-dark)

## Read and filter

1. If a read is running, click **Stop tail** or **Cancel fetch** to enable its settings.
2. Use **Read** to choose **Tail** for ongoing traffic, or **Newest N** or **First N** for a bounded read. Set **Limit** to the number of records you need.
3. Click **Start tail** or **Load messages**, depending on the chosen mode.
4. Once the records you need are present, click **Stop tail** for a stable view
   or wait for the snapshot to complete.
5. Copy a key from a visible record, then open **Filters**.
6. Paste that key into **Key contains** and select a matching row.
7. Clear the filter to restore the other loaded records.

**You should see:** only matching records in the current view. Filters work on
records available to the workbench; they do not search the entire cluster.
If nothing matches, clear the filter and check the topic and read mode.

## Inspect a record

1. Select a row to open **Message details**.
2. Choose **Value**. For JSON, compare **Formatted JSON** and **Raw**.
3. Choose **Key** to inspect the record key.
4. Choose **Metadata** to read its partition, offset, timestamp and headers.
5. Use **Copy** when you need the value elsewhere.

**You should have:** the payload and the topic, partition and offset needed to
find its position again. An offset belongs to one partition; it does not order
records across the whole topic.

![Message inspector for an orders.events record on AIO Kafka](../assets/messages.png#only-light)
![Message inspector for an orders.events record on AIO Kafka](../assets/messages-dark.png#only-dark)

## Export the records you need

1. Set the filters and selection you want to export.
2. Open **Export** and review its available scope and format.
3. Confirm the export and check the resulting file.

Export covers the scope shown by that workflow. The live view keeps a bounded
history, so older records may have left the window. Check **Monitor** when you
need to distinguish ordinary history eviction from overload drops.

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

To change broker settings, open the topic's **Configuration**, search for the
setting, refresh its current value, then review the proposed change before
applying it. This changes the broker's configuration.
