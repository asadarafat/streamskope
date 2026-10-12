# Save and share views

Keep Kafka view settings and record positions for another session, or share a
reviewed view with another engineer. Start with a [connected profile](connections.md)
and [read the records](messages.md) you want to inspect. A view stores settings and
positions, not a copy of the messages.

## Keep investigation views

1. Choose a topic and its **Messages**, **Monitor**, **Latency**, **Rules** or
   **Configuration** task, or select a **Consumer group**. For a topic, choose the
   read mode, limit and filters. Saving **Last 2 minutes** captures absolute times
   at the moment you save; reopening does not move that interval forward.
2. On Messages, use the grid column menu to choose visible columns and drag a
   column edge to resize it. Open or close **Filters** and resize the inspector
   with its separator. A compact screen may temporarily hide columns; it does not
   change your saved choices.
3. Open **Views** in the header, enter **View name**, optionally choose a
   **Local connection profile**, and click **Save current view**.
4. Later, choose the entry in **Saved view** and click **Open view**. Opening
   restores the destination, controls and unloaded record positions, and clears old
   record contents, comparison results and read coverage. It never connects, reads records, refreshes group detail or
   starts a latency probe. Stop an active read/probe and wait for confirmation
   before opening another view.
5. Connect the chosen profile if needed, review the destination and settings,
   then explicitly read messages, start a probe or refresh the consumer group.
   A group selection starts as **Consumer group not loaded**; absence from the
   bounded inventory does not prove the group was deleted. Topic Configuration
   can load its existing read-only metadata when opened.
6. To change an entry, adjust the controls, return to **Views**, select it and use
   **Replace selected**. **Delete selected** asks for confirmation and removes only
   the saved settings. Sidebar navigation preserves the current resource;
   breadcrumbs return to the topic or group inventory.

Views retain a topic task or group identity, optional background topic query,
column visibility and widths, inspector width and filter-panel state, plus an
optional local profile ID. A group-only view needs no topic. A deleted profile
must be replaced or its reference cleared; a different profile with the same name
is not substituted. Missing topics remain selected for review before a read.

Views may also retain the selected record position, comparison baseline position
and named bookmarks. They contain no credentials, record contents, decoded values,
original bytes, continuation tokens, running jobs, latency results or configuration drafts. They
do not replace current encoding or protection preferences. Filters and resource
names can still contain sensitive text; protect saved-view files accordingly.
The dynamic **Rule matches only** switch cannot be saved; use a JSON expression
for a repeatable independent filter.

Desktop and the installed browser host store up to 100 views in
`queries/kafka-queries.json`. Browser development keeps its library only until
that development host restarts. Existing query entries open with default layout;
the first saved-view or local-notes change writes version 4 and preserves the exact legacy
file as a private `.pre-views-v1` (version 1), `.pre-records-v2` (version 2) or
`.pre-catalog-v3` (version 3) predecessor. An unreadable, oversized or
unsupported library is preserved and reported as unavailable. Restore a valid
backup before retrying. Back up the entire [application data](recovery.md),
including predecessor files; hosts supporting only versions 1–3 cannot read version 4.

## Start from observed health

[Observed health](observed-health.md#save-a-repeatable-investigation) can prepare
a saved topic or group investigation after showing its source and coverage.
Review and explicitly save its controls in the same library. These views contain
no measurements or diagnosis; re-evaluate current evidence when you reopen them.

## Bookmark and reload a record

1. Read a record and open its inspector. Choose **Bookmark record**. This action
   requires source identity captured by the delivering reader; a record without
   verified cluster/topic identity and its original batch epoch cannot be bookmarked.
2. Choose an existing **Saved view**, or enter a new **View name** to save the
   current view. Enter **New bookmark name**, then choose **Save bookmark**.
   Adding to an existing view preserves that view's settings and other positions.
   The same position updates its existing bookmark rather than creating a duplicate.
3. To resume, open **Views**, choose the view and **Open view**. Selected and
   baseline positions start **Not loaded**, even after restart. Connect the intended
   profile if needed, then choose **Reload selected** or **Load baseline** in
   **Saved record positions**. Stop an active read or probe first.
4. Choose a **Record bookmark** and **Use as selection** or **Use as baseline** to
   prepare another position. Choosing does not read it; Reload is separate. From
   **Views → Saved bookmark**, **Open bookmarked topic** opens its controls and
   unloaded position. Then choose **Reload selected**.
5. Use the existing inspector and **Compare** tab for the loaded records. Reloaded
   records use current encoding and protection. They do not enter the grid's
   filtered window or change its read coverage, and do not run live rules.

**Save current view** also captures supported selected and pinned baseline
positions. An ordinary transient comparison remains available when durable
bookmarking is unsupported. Changing connection, encoding or protection clears
loaded record contents; the saved positions remain available for explicit reload.
All positions in one view must identify the same Kafka cluster; comparisons may
span topics within that cluster. A profile name alone never establishes identity.
To start an investigation on another cluster, choose **Clear saved positions**.
This clears positions and reloaded contents in the current workspace; saved views
remain unchanged. A pending reload must confirm its stop before positions can be cleared.

The library allows 32 bookmarks per view and 256 overall, with names up to 128
characters, within its existing 1 MiB file limit. In **Views**, choose a **Saved
bookmark** to rename or remove it. Removing a bookmark never deletes a Kafka
record. Concurrent edits are rejected; refresh and review before retrying.

Reload distinguishes verified retention expiry, a replaced topic/cluster, reused
record history, a confirmed missing topic, denied access, and a conclusive offset
gap. Incomplete evidence is reported as unavailable, not proof of deletion. A
missing record is never silently replaced with the next offset. Kafka resource
identity and batch epochs verify log positions; they are not forensic proof
against administrative restores that clone resource identities and log metadata.
If cleanup is unconfirmed, **Retry stop** remains available and new reloads stay
blocked until the original reader confirms its stop.

## Share an investigation

1. Open **Views → Import/share investigations** and select a saved view, or leave
   the selection empty to use the current workspace. Review filter text and
   resource positions before sharing.
2. Choose **Export view JSON**. This includes the topic task or consumer group,
   query settings, layout, selected/comparison positions and named bookmarks.
3. The recipient chooses **Import file**, or pastes JSON into **View or query
   JSON/link**, then selects **Review import**. Review the destination and settings
   and optionally choose a local connection profile.
4. Choose **Open imported view**. An active read must stop through its original
   host first. Opening restores controls and unloaded positions; it does not
   connect, read records, follow links or save the view. Connect and reload
   deliberately, then **Save current view** if it should survive restart.

Portable views are version-1 `streamskope.kafka-view` JSON documents, limited to
128 KiB. They exclude local view, bookmark and profile IDs, credentials, broker
endpoints, record contents, running jobs, global preferences and local topic notes.
Import review is passive. Opening assigns fresh local bookmark IDs; source cluster
and topic identities remain unchanged, so a record reload cannot silently use a
different cluster. Current decoding and protection still apply.

For a smaller share, expand **Query settings only** and use **Export query JSON**
or **Copy query link**. Query documents retain their version-1 format and 32 KiB
limit. They exclude layout, group destination and record positions; a group view
without a background topic query cannot export query settings. The same import
control reviews them before **Open imported query**.

Browser query links open the application at its original address, which must be
reachable, and show the review dialog. The fragment is removed from the visible
address when loaded. Desktop links use `streamskope://app/#query=…` as a portable
string to paste into the dialog; this version does not register an operating-system
link handler. Full views use JSON files, not URL fragments.

Both formats reject unknown fields, unsupported versions, credentials and record
bodies. Invalid imports leave the current investigation unchanged. Filter literals,
names and positions can still be sensitive: review the file before sharing it.

## Keep local topic notes

Open a topic and choose **Local topic notes** to record its description, owner,
labels and named HTTPS links. These are your local annotations, separate from
broker-discovered facts. Saving verifies the current cluster ID and topic UUID;
notes never transfer to a different cluster or a recreated topic with the same name.

Choose **Verify current topic** before editing stored notes. If another operation
changes them, a stale save is
rejected and your draft remains available for review. A connection change also
requires fresh verification before a new save. A disk write already admitted for
the original resource retains its outcome.

Use **Views → Local topic notes** to inspect all stored notes even while
disconnected. Old or inaccessible resources can be removed locally without
modifying Kafka. Opening the dialog never reads record bodies. Links open only
after an explicit click through the application's protected external opener.

Notes share the saved-view library's 1 MiB limit, with at most 256 annotated topics.
Each entry allows a 4,096-byte UTF-8 description, 128-character owner, 16 unique
labels of 64 characters and eight named HTTPS links. No entries are pruned to make
space. Notes are ordinary local settings, not encrypted secrets or shared team
metadata; protect them and their [complete backups](recovery.md#saved-view-library-recovery).
