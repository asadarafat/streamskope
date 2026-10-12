---
plugin_scope: eda
---

# Inspect an EDA export

## What this workflow does

Use EDA Connector to inspect records exported by Nokia EDA. You can connect to
an existing Kafka destination or create a separate temporary capture, inspect
its records, and remove the temporary resources afterward.

## The problem it solves

An EDA node changes state or an alarm appears, but your downstream application
sees no event. Check whether EDA exports that event, whether the Kafka connection
works, and whether your read covers it. Installing the connector or starting a
capture does not generate source events.

## Before you start

Have a supported [EDA Connector](../plugins/eda.md#before-you-start), access to
the intended exporter, and an expected event or emission interval agreed with
its owner. An exporter name alone does not identify its topics. For temporary
capture, check the cluster app prerequisites and coordinate with other operators:
the cluster accepts only one unexpired capture session.

## How to solve it

### Check the path and read a record

1. Open **Add connection → Connect via EDA**, enter the platform URL and
   credentials, keep certificate verification enabled, then **Discover sources**.
   Use the [setup guide](../plugins/eda.md#install-the-desktop-plugin-and-discover-sources)
   if the connector is not installed. Match the **Producer** namespace/name or **ClusterProducer** name and its
   exported topics. Do not select an unrelated exporter just because it is visible.
2. Choose **Connect to existing Kafka** when you have its network access and
   credentials. For a separate destination, choose **Set up temporary capture**,
   review the source and port, then **Start capture**. Follow the
   [temporary capture setup](../plugins/eda.md#start-temporary-capture) when the
   cluster helper needs installation. Note which path you chose.
3. Connect the resulting profile. Open **Topics** and the intended exported topic.
   Clear filters. Use **Tail** for an upcoming emission or **Newest N** for records
   already retained in this broker. Stop the read after the relevant records arrive.
4. Select a record. Inspect **Value**, **Metadata** and **Decoded**. Compare its
   resource identifier with the event you expected and retain its topic,
   partition, offset and timestamp. Field names depend on the export definition.
5. If nothing arrives, use the table below before starting another capture.

Temporary capture creates a new broker and copied exporter. It does not copy the
original broker's retained history or generate test events. An event that happened
before capture started may need to be inspected at the existing destination.

### Interpret what you see

| Result                                      | Next check                                                                                                                       |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| No source in discovery                      | Verify exporter scope, named exports and API read permissions with the EDA administrator                                         |
| Capture setup fails                         | Read the failed step; check the local port, cluster app readiness and capture ownership in the [plugin guide](../plugins/eda.md) |
| Connected, but the expected topic is absent | Check the copied exporter's status and intended destination; API login and tunnel readiness do not prove export delivery         |
| Topic exists, but the read is empty         | Check filters, read mode and source emissions since capture began                                                                |
| A record matches the expected resource      | You have evidence of that record in this broker; it does not establish downstream processing success                             |

Use [Find a missing or delayed event](missing-event.md) if the records are present
but their timing or relationship needs further explanation.

### Finish and share

[Export the record evidence you need](share-findings.md) before removing a
temporary capture: its broker messages are discarded during removal. Bookmarking
requires verified source identity and can be unavailable for a capture broker;
saved positions do not preserve its messages.
Disconnect Kafka, then use **Stop and remove capture** and confirm the reported
cleanup. Disconnecting alone leaves capture running. Follow the
[cleanup diagnosis](../plugins/eda.md#administrator-diagnosis-and-cleanup-verification)
if confirmation is unavailable; elapsed lease time is not proof of deletion.

If you used existing Kafka, disconnecting ends your client connection. Do not
remove the original EDA exporter as part of this workflow.

## Check the result

You have an exported record whose source, identifier and time match the event,
or a specific unresolved stage to check: exporter selection, connection or read
coverage. Confirm removal of any temporary capture you created. Kafka receipt
does not prove that a downstream consumer handled the event.

## Watch the walkthrough

A 42-second recording of the actual workbench with on-screen instructions and no
audio. Recorded against the local EDA 26.8.2 lab with the EDA Connector and cluster capture app already installed. Credential entry and provisioning waits are omitted; the original exporter remains unchanged.

<video class="skope-walkthrough" controls muted playsinline preload="metadata"
       poster="../../assets/skope/eda-export.png"
       aria-label="Walkthrough: Inspect an EDA export"
       aria-describedby="eda-export-video-summary">
<source src="https://github.com/asadarafat/streamskope/releases/download/documentation-media/v1/eda-export.mp4" type="video/mp4">
  <track kind="captions" src="../assets/skope/eda-export.vtt" srclang="en" label="English instructions">
  <a href="https://github.com/asadarafat/streamskope/releases/download/documentation-media/v1/eda-export.mp4">Download the walkthrough</a>.
</video>

<p id="eda-export-video-summary">Follow the actions and outcomes below. Pause or use
fullscreen to inspect the controls; the written guide covers prerequisites and
the decisions behind each step.</p>

<details markdown="1">
<summary>Read the walkthrough transcript</summary>

- **00:00 — Connect via EDA.** Live EDA lab; connector installed. Credential entry and provisioning are omitted.
- **00:06 — Review the capture profile.** The temporary broker and exporter leave the original producer unchanged.
- **00:12 — Connect and open the topic.** Select the exported topic from the generated connection.
- **00:18 — Read an exported record.** Inspect its content; a visible event is evidence for this capture only.
- **00:24 — Retain the record position.** Keep the topic, partition, offset and timestamp for follow-up.
- **00:30 — Disconnect the client.** Disconnecting Kafka leaves the temporary capture running.
- **00:36 — Remove the temporary capture.** Removal discards captured messages. Confirm cleanup; the original exporter remains.

</details>
