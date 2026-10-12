---
plugin_scope: nsp
---

# Follow an NSP notification

You expect a notification after an NSP operation. Establish access to the broker,
search the intended stream, and distinguish a missing match from an incomplete
read or a source that has not emitted anything.

## Watch the walkthrough

A 42-second recording of the actual workbench with on-screen instructions and no
audio. Recorded against the local NSP 26.4 lab with the NSP Connector already installed. Credential entry and provisioning waits are omitted. This lab disables API certificate verification; use trusted certificates in production.

<video class="skope-walkthrough" controls muted playsinline preload="metadata"
       poster="../../assets/skope/nsp-notification.png"
       aria-label="Walkthrough: Follow an NSP notification"
       aria-describedby="nsp-notification-video-summary">
  <source src="../assets/skope/nsp-notification.mp4" type="video/mp4">
  <track kind="captions" src="../assets/skope/nsp-notification.vtt" srclang="en" label="English instructions">
  <a href="../assets/skope/nsp-notification.mp4">Download the walkthrough</a>.
</video>

<p id="nsp-notification-video-summary">Follow the actions and outcomes below. Pause or use
fullscreen to inspect the controls; the written guide covers prerequisites and
the decisions behind each step.</p>

<details markdown="1">
<summary>Read the walkthrough transcript</summary>

- **00:00 — Connect via NSP.** Live NSP lab; connector installed. Credential entry and provisioning are omitted.
- **00:06 — Review the generated profile.** This lab disables API certificate verification; use trusted certificates in production.
- **00:12 — Connect and select the topic.** Use the generated Kafka profile; no cluster credentials are entered here.
- **00:18 — Read recent notifications.** A bounded read shows ten retained notifications from this lab.
- **00:24 — Keep metadata with the event.** Record topic, partition, offset and time alongside your interpretation.
- **00:30 — Disconnect when finished.** Disconnect ends this client session; it does not delete notifications.
- **00:36 — Reuse the saved profile.** The profile and reusable NSP workflow remain for the next connection.

</details>

## Before you start

Use a supported [NSP Connector](../plugins/nsp.md#before-you-start). Obtain the
notification topic, the operation's time interval and a known resource or
correlation identifier from the operator. Topic names and payload fields depend
on your NSP configuration; there is no universal notification topic or JSON path.

The StreamSkope host needs network access to NSP's API and Kafka's advertised
broker addresses. The plugin retrieves connection material; it creates no broker
or tunnel.

## Connect and follow the notification

1. Follow [Create the connection profile](../plugins/nsp.md#create-the-connection-profile).
   Wait for retrieval, execution cleanup, connection testing and profile saving.
   Connect the saved profile and open **Topics**.
2. Open the intended notification topic. For an upcoming operation, use **Tail**.
   For a past operation, stop the current read, select **Time window**, then
   **Custom interval** with explicit timezone bounds around the operation.
3. Choose **Load messages** for the time window. Inspect the displayed read
   coverage, then select a record and check its **Value** and **Metadata**.
4. Use the known identifier to narrow the records. A grid filter covers loaded
   records only. Use [broker search](../guide/messages.md#search-beyond-the-loaded-sample)
   when the question extends beyond that sample.
5. If several known Kafka topics carry the same identifier, follow the
   [correlation workflow](missing-event.md#follow-the-identifier). Select the
   actual header, key or payload path used by those records.

Kafka time-window reads use record timestamps, which can differ from an operation
timestamp inside the payload. Record both when timing matters.

## Interpret what you see

| Result                                      | What to do next                                                                                                      |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| API access succeeds, Kafka connection fails | Check broker reachability, certificate names and Kafka authentication separately                                     |
| Expected topic is absent                    | Confirm the configured notification stream and your topic permissions with the NSP administrator                     |
| No match in a partial read                  | Continue the supported bounded read or narrow the interval; do not conclude the notification was never published     |
| No match in a completed range               | Check the identifier, timestamp interpretation and source emission; the finding applies only to that searched range  |
| Matching notification found                 | Record its source position and relevant fields; a notification alone does not prove another application processed it |

## Finish and share

Stop the read and [share the findings](share-findings.md) you need. Disconnect
when finished. Keep your saved profile for reuse; use the plugin's credential
refresh action when its connection material changes.

The plugin normally removes its completed execution records and retains its
reusable workflow definition. It does not remove NSP topics or notifications.
If **Cleanup required** remains, follow
[workflow ownership and cleanup](../plugins/nsp.md#what-the-workflow-changes)
with the same account instead of deleting recovery state.
