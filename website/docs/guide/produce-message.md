# Produce and verify one message


In a topic, choose **Produce message**. Select the partition, enter a key and value,
and optionally add an ordered JSON array of headers. Repeated header names are
preserved. **Null key** differs from an empty key; **Tombstone** writes a null value.
Use the Base64 option for binary key/value bytes and header values. Header names in
the form use UTF-8. Each attempt is limited to one record and 64 KiB including
headers; this is an operational composer, not a bulk importer.

Choose **Review message** and check the connection, topic, partition and complete
Base64 representation before **Confirm produce**. The host binds the review to
that connection and expires it after two minutes. Kafka must already have the
selected topic and partition. Read-only mode blocks confirmation.

An **Acknowledged** result includes the Kafka partition and offset. StreamSkope
attempts to read back the exact key, value and ordered headers without committing
consumer offsets. A failed read-back does not mean the write failed. An **Outcome
unknown** result means Kafka may have accepted the record: inspect the destination
before making a new attempt. The app never automatically resends a record. If the
host response is lost, **Check attempt result** uses the same plan without sending
again. Attempt results are session-local and bounded; after restart, inspect Kafka.

For reviewed recovery, see [copy or replay records](record-replay.md) and
[preview consumer offset resets](offset-recovery.md).


Return to [Read Kafka messages](messages.md) to inspect the acknowledged record.
For a recovery decision, follow [Review and replay a failed record](../skope/replay-record.md).
