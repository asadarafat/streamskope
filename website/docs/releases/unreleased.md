---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

## Desktop

Observed health now prioritizes selected-topic findings, measurement coverage and
investigation actions. Existing resources can be selected from the connected
profile; collection progress and cooldown are visible. Partition filtering and
sorting and group/topic/exact-record drilldowns reduce manual investigation work.

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
