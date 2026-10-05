---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

## Desktop

Application shutdown rejects new requests immediately and waits for every owned
cleanup to finish, including environment comparisons that open a separate saved
destination. A failed cleanup cannot make shutdown finish while another cleanup
is still running. Cancellation preserves acknowledged or uncertain write outcomes;
it does not automatically retry a promotion.

Cancelled Kafka reads and latency probes keep already-started driver promises
observed through settlement, preventing unhandled rejections when cancellation
wins.

Desktop and development hosts isolate registered provider routes, event streams
and shutdown ownership while preserving the existing Kafka host API. Desktop
event failures mark the affected stream unavailable; replacement subscriptions
cannot acknowledge events from an earlier subscription. Kafka remains the only
available messaging provider in the UI at this stage. The hosts now include a
separately typed Core NATS backend with protected native profiles, verified
token/TLS connections and confirmed live subscription cleanup. Its operator
workspace is introduced and qualified separately. Core NATS offers live receipt
only; it does not provide Kafka offsets, subject inventory or historical replay.

The product shell now owns shared navigation presentation and layout. Provider
switching waits for confirmed stream stop and disconnect, retains the current
workspace after a cleanup failure, and blocks new commands from retired views
without discarding already-admitted write responses.

When the final development-browser client leaves, the host stops its active reader
and waits for confirmed cleanup before accepting more commands. Desktop event
failure uses the same confirmed-stop rule. The broker connection stays available;
failed cleanup requires restarting the host. Transport queues account for records
held by an outstanding write, and HTTP omission counts stay with the read generation
that lost those records.

Observed health now prioritizes selected-topic findings, measurement coverage and
investigation actions. Existing resources can be selected from the connected
profile; collection progress and cooldown are visible. Partition filtering and
sorting and group/topic/exact-record drilldowns reduce manual investigation work.
Host loss stops collection and marks previous measurements as retained evidence;
recovery requires a new capture before resource links become actionable.

Collection errors retain specific safe recovery reasons. Selected-topic lag is
independent of unrelated group-member/assignment omissions. Record sampling uses
bounded adaptive windows; incomplete coverage does not qualify key or size
inference. Existing schema-1 history remains readable.

The desktop host protocol advances to 49 for explicit observation coverage and
recovery errors. Development renderer and host builds must be updated together;
the desktop installer includes both. This does not change the plugin API.

Focused operator recovery and isolated multi-broker outage/recovery checks cover
the changed behavior. Executed results and limitations belong to the exact PR
revision; earlier release qualification is not carried forward automatically.

The next desktop version will be assigned when a maintainer starts the release
workflow. Plugin releases remain independent; their pending notes are retained
in the corresponding plugin release commentary.
