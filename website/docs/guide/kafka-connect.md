---
title: Manage Kafka Connect
description: Validate connector configuration, review lifecycle actions and investigate supported dead-letter queues.
---

# Manage Kafka Connect

Connect to Kafka, then open **Kafka Connect** in the sidebar. Add the **Kafka Connect URL** under your profile's cluster services first. The endpoint must be reachable from the StreamSkope host.

Choose **Connect authentication**: no HTTP authorization, the profile's OAuth bearer,
HTTP Basic username/password, a separate bearer token, or a separate OAuth client.
Choose **Connect certificate trust** independently: system authorities, broker
trust, or a separate PEM/JKS/PKCS12 bundle. Configure Connect's own mutual TLS
identity if its HTTPS endpoint requires a client certificate. Its separate OAuth
token request uses these same service trust and identity settings.

Test the profile to check broker and configured service access, then save and
reconnect. Successful inventory access does not establish permission to change
connectors. See [certificate trust](tls-trust.md) for separate issuing CAs.

## Discover and validate

Refresh connectors to list connector names and classes installed on the responding worker. During rolling upgrades, workers can have different installed plugins. Select a connector to see its connector/task states and observation time. Arbitrary remote configuration values and raw worker stack traces are withheld because they may contain secrets.

For a new connector, enter a name containing letters, digits, dots, underscores or hyphens and a JSON object of string values. For example, with the Apache FileStream sink installed:

```json
{
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": "1",
    "topics": "orders.events",
    "file": "/tmp/orders.txt"
}
```

The file belongs to the Connect worker, not your desktop. **Validate configuration** calls Connect's validation endpoint without creating or changing a connector. Local checks require a connector class, a positive `tasks.max` when supplied, and a DLQ topic when `errors.tolerance` is `all`. Remote validation failures identify the field; inspect worker diagnostics securely for details that cannot safely be shown.

For updates, **Configuration changes** is a JSON object containing only fields to set. **Fields to remove** is a separate JSON string array, such as `["errors.tolerance"]`. The host starts with the complete current configuration, applies these explicit changes and validates it before review. A key cannot be both set and removed. Omitted fields keep their actual values, including credentials; protected display placeholders cannot be submitted as replacement values. Removing a field can restore worker or connector defaults. The connector name cannot be removed or replaced through configuration.

An empty update, duplicate removal, absent removal or oversized merged configuration is refused. Selecting a connector starts with `{}` changes and `[]` removals; its displayed protected configuration is for inspection.

## Review and apply

Choose create, update, pause, resume, restart failed tasks or delete. **Review action** validates the configuration where applicable, captures the complete canonical configuration/task state and expires after two minutes. The review names the connected profile and lists set and removal keys without their secret values. Confirm the exact action and name before applying. Read-only mode prevents application. Keep the receipt until you choose **Dismiss receipt and start another review**; refreshing inventory does not resend the action.

A changed connector, connection or expired review is rejected before dispatch. Reconnecting to a profile with the same name also clears its edited form and prior review. One review identifier permits one attempt. A successful HTTP response means **acknowledged**; asynchronous task transitions can still be pending. Refresh until the expected state appears. An interrupted request can be **unknown**: inspect the connector before making another review. StreamSkope does not automatically resend mutations. Concurrent changes after the final read cannot be made atomic by the Connect REST API.

The receipt separates four facts:

| Evidence                 | Meaning                                                                                                                                                                                                                                      |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dispatch                 | `not-sent` or one `attempted` request; an attempted request may have an unknown remote outcome.                                                                                                                                              |
| Acknowledgement          | `acknowledged`, explicitly `rejected`, or `unknown`, based on the actual response. A missing or invalid response body cannot erase an already received success status.                                                                       |
| Readback                 | `verified` means the full requested configuration or expected lifecycle state was observed; `different` means it was read but did not match, and `unavailable` means it could not be checked. Non-acknowledged actions use `not-applicable`. |
| Original request cleanup | `confirmed` requires the original HTTP request and socket to close. `unresolved` blocks new work and keeps the original lease for cleanup; a timeout does not establish closure.                                                             |

An acknowledged action can therefore have unavailable readback or unresolved cleanup. Keep that receipt; do not resend it as a new action. Disconnect or lock joins the original admitted Connect requests and shared OAuth refresh, including work whose caller already stopped waiting. If cleanup cannot be confirmed, inspect host diagnostics and resolve the original operation before reconnecting. These checks do not establish cleanup of the connector's external systems.

Delete removes the connector configuration, not its Kafka topics or external data. Restart failed tasks requests `includeTasks=true&onlyFailed=true`; it does not rewind offsets, repair a converter or guarantee delivery.

## Investigate a dead-letter queue

When a connector declares `errors.deadletterqueue.topic.name`, **Browse DLQ** opens that topic. For sink connectors that support Connect error handling, enable `errors.deadletterqueue.context.headers.enable` to include original topic, partition, offset, connector, task and processing stage.

The message inspector interprets complete, unmasked Connect context headers as **reported metadata**, not independently verified provenance. Missing, masked or truncated context stays unavailable. Ordinary message inspection remains available.

Use [Copy or replay records](record-replay.md) to choose an explicit destination and inspect exact original/transformed bytes. The same bounded replay preserves key, value, nulls and ordered headers and reports acknowledged, rejected, unknown and unsent records. Copying back to a source can fail again or loop. This workflow does not skip a task's bad record, delete the DLQ record or commit a source offset. Connector-specific DLQ support must be verified with that connector.

REST behavior follows the [Apache Kafka Connect guide](https://kafka.apache.org/43/kafka-connect/user-guide/). Real-worker qualification uses Apache Kafka/Connect 4.3.1 with its FileStream sink; it does not qualify every connector plugin.
