# Under the Skope

Start with an operational question. These workflows connect reads, comparisons
and reviewed changes to the evidence needed for your next decision.
To connect and read messages, start with [Everyday tasks](../guide/index.md).
Each workflow links to the administration or development procedures it needs.

| Your question                                             | Workflow                                                            | What you finish with                                                    |
| --------------------------------------------------------- | ------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| Is an event missing, delayed or outside my search?        | [Find a missing or delayed event](missing-event.md)                 | Search coverage and a justified next check                              |
| Why does this payload look different?                     | [Understand a changed payload](changed-payload.md)                  | A comparison of fields, bytes or writer schemas                         |
| What explains this topic's lag or replication finding?    | [Diagnose Kafka health](../guide/observed-health.md)                | Source-bound observations and hypotheses to verify                      |
| Why can't I see the expected EDA export?                  | [Inspect an EDA export](eda-export.md)                              | A checked capture path, available record evidence and confirmed cleanup |
| Did NSP publish the notification I expected?              | [Follow an NSP notification](nsp-notification.md)                   | A checked connection and bounded search of the intended topic           |
| Can I send a failed record again safely?                  | [Review and replay a failed record](replay-record.md)               | A reviewed destination and per-record outcomes                          |
| Which records would a consumer-position change reprocess? | [Recover consumer positions](../guide/offset-recovery.md)           | A reviewed offset plan and its actual outcomes                          |
| What differs between these environments?                  | [Compare environments](../guide/environment-comparison.md)          | An explicit comparison and reviewed changes where supported             |
| What could a schema change affect?                        | [Assess relationships and schema impact](../guide/relationships.md) | Observed relationships with their coverage limits                       |
| Why does connection or reading fail?                      | [Fix a connection or reading problem](../guide/troubleshooting.md)  | The failed stage and the next recovery check                            |
| Can another engineer repeat my checks?                    | [Share your findings](share-findings.md)                            | Reviewed settings, positions and a scoped handover                      |

Every workflow includes a 42-second video, optional captions and a transcript.
Each video identifies its live target or illustrative fixture data. Use the
written guide for prerequisites, decisions and recovery steps.

The event and recovery workflows here use Kafka. EDA and NSP require their
optional [connection plugins](../plugins/index.md). For live subjects on an
external NATS server, use [NATS live subscriptions](../guide/core-nats.md).

```text
Choose the source and time range
             |
             v
Read evidence and its coverage
             |
             v
Interpret the result -> Choose the next check
             |
             v
Save useful findings and finish safely
```

[Search coverage and evidence](../concepts/evidence.md) explains how much a result
can establish. [Reference](../reference/index.md) owns exact formats and limits.
The linked task and plugin guides own the operating steps, avoiding a second
copy of the same procedure in each scenario.
