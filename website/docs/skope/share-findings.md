# Share your findings

Another engineer needs to repeat your checks. Separate the settings they can
reopen from the record evidence you choose to share, and explain where your
search stopped.

## Watch the walkthrough

A 42-second recording of the actual workbench with on-screen instructions and no
audio. Bookmark an illustrative AIO record, review and export the view, then import it with positions unloaded.

<video class="skope-walkthrough" controls muted playsinline preload="metadata"
       poster="../../assets/skope/share-findings.png"
       aria-label="Walkthrough: Share your findings"
       aria-describedby="share-findings-video-summary">
  <source src="../assets/skope/share-findings.mp4" type="video/mp4">
  <track kind="captions" src="../assets/skope/share-findings.vtt" srclang="en" label="English instructions">
  <a href="../assets/skope/share-findings.mp4">Download the walkthrough</a>.
</video>

<p id="share-findings-video-summary">Follow the actions and outcomes below. Pause or use
fullscreen to inspect the controls; the written guide covers prerequisites and
the decisions behind each step.</p>

<details markdown="1">
<summary>Read the walkthrough transcript</summary>

- **00:00 — Bookmark the useful position.** Illustrative AIO record: positions cannot restore expired data.
- **00:07 — Review what the file includes.** Names, filters and positions can be sensitive. Payloads and credentials are excluded.
- **00:14 — Export settings and positions.** Record evidence is a separate export. Review before sharing.
- **00:21 — Review and select a local profile.** Importing the file does not connect or read records.
- **00:28 — Open with positions unloaded.** Read explicitly under your current credentials and protection.
- **00:35 — Add the handover context.** Include the question, UTC range, result, coverage gaps and next check.

</details>

## Before you start

Finish or stop the active read. Know the intended recipient and what operational
data you may share. Even without credentials or payloads, topic names, filter
literals and record positions can be sensitive.

## Prepare the handover

1. [Save the current view](../guide/saved-views.md#keep-investigation-views) with a
   useful name, topic or group, explicit time interval and filters. A saved
   **Last 2 minutes** interval becomes fixed when saved.
2. [Bookmark relevant records](../guide/saved-views.md#bookmark-and-reload-a-record).
   These are verified source positions. Retention, compaction or resource replacement
   may make the records unavailable when someone reloads them later.
3. Choose what the recipient needs: a view for settings and positions, or only
   query settings. Use the [view sharing procedure](../guide/saved-views.md#share-an-investigation)
   to export the chosen scope. Review its literals, names and positions for disclosure.
4. If actual record content is needed, separately [export the selected evidence](../guide/messages.md#export-the-records-you-need).
   Review the file, its masking and its coverage before sharing. View JSON contains
   no message bodies; record exports are not encrypted merely because masking applies.
5. Write a short account of what you checked. Use the template below, omitting
   any identifiers the recipient should not receive.

```text
Question:
Source cluster and topic/group:
UTC interval and identifier/selector:
Result and relevant record positions:
Coverage gaps, decoding errors or masked fields:
Next check and responsible operator:
Capture cleanup or replay outcome, if applicable:
```

For an application failure, add the reviewed
[Support report](../guide/troubleshooting.md#share-useful-evidence). It contains
bounded activity metadata and correlation IDs, not record payloads or a diagnosis.

## Reopen at the receiving end

1. The recipient follows the [import review procedure](../guide/saved-views.md#share-an-investigation)
   and deliberately selects an appropriate local profile. Opening restores
   settings and unloaded positions; it does not connect or read.
2. Decide whether the same source and interval are still available, then connect
   and reload under current credentials, decoding and protection. Imported
   positions cannot silently move to another cluster or replacement topic.
3. Compare the new coverage with the sender's notes. Retention or changed access
   can explain a different result; repeatability does not guarantee identical data.
4. Record any changed conclusion separately and save the imported view locally
   if the settings should persist.

## Know what was shared

View files exclude credentials, broker endpoints, record bodies and local topic
notes. They do not capture a running trace, health history or replay job. Local
notes remain local; include only approved findings in your separate handover.
See [the sharing contract](../guide/saved-views.md#share-an-investigation) for file
limits and query-link behavior.

Keep sensitive evidence in an approved location and remove temporary copies when
they are no longer needed. Shared settings make a check repeatable; they cannot
restore data that Kafka no longer retains.

For repeated evidence collection, use the [read-only CLI](../guide/read-only-cli.md).
Before sharing values, check the [record masking settings](../guide/masking.md)
and review the actual exported file.
