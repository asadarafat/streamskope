---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

## Desktop

Topic Monitor now separates current pressure from historical display loss, samples
quiet streams, and identifies the request that owns each measurement. Stop also
cancels an opening read; connection changes discard old queued callbacks. Terminal
publication respects desktop transport pressure and reports any remaining display
omissions rather than overflowing the channel.

The Monitor leads with publication, queue, loss and freshness, keeps Stop/Cancel
available, and places renderer timing and application FPS under Diagnostics.
See [Stream monitor](../guide/operations.md#stream-monitor) for metric boundaries
and stopping behavior.

The internal host protocol changes from **47 to 48**. Host and renderer must be
updated together; plugin API **4** is unchanged. This does not assign a new plugin
release or change declared target-system compatibility.

## Documentation

Documentation publication now checks the latest immutable stable desktop release
before build and deployment. Plugin guides separate source requirements from a
verified catalog availability snapshot; source rehearsal records keep their exact
revision and qualification limits.

The next version will be assigned when a maintainer starts the release workflow.
Plugin releases remain independent. These changes are not included in
[v0.7.1](v0.7.1.md).
