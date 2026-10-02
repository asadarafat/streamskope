---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

## Release versions assigned in CI

Maintainers start **Actions → Release → Run workflow** from `main`, select the
desktop or an individual plugin, and enter its Semantic Version. Release CI
qualifies the exact source, assigns the version in a disposable build checkout,
and creates a tagged draft for review. PR and `main` CI only qualify source;
merging does not assign a release version. Development source uses `0.0.0-dev`.
New releases use ordinary versions or prereleases; `+build.N` remains historical.

## Independent plugin versions

Plugin API **4** separates each plugin's own version from its requirements. EDA
Capture and NSP Capture declare StreamSkope **>=0.2.0, <0.3.0**, with exact targets
EDA **26.8.2** and NSP **26.4.0**. These desktop bounds express compatibility;
they do not assign the desktop's next release version. The catalog filters host
compatibility before offering updates, and the plugins verify the target's
version through its API before setup.

Desktop releases contain installers and checksums. Independent plugin releases
supply the selected plugin's bundle and manifest, plus the workflow download for
NSP. The NSP workflow remains inside its verified plugin bundle. The separate EDA
cluster application retains version **26.8.2** and its separate publication process.

## Upgrade and recovery

Release desktops preserve installed API 2 and API 3 plugins and their saved profiles.
Compatible API 4 packages supersede that legacy generation without comparing a
SemVer with the old target-based or revision-based labels. Hot installation,
updates, removal and failed-activation recovery retain their existing lifecycle.
Development hosts and their development packages are isolated from release hosts.

Back up the complete application data before upgrading. API 4 plugins need a
desktop supporting that API; an older desktop requires its matching pre-upgrade
plugin/data backup. See [plugin versioning and migration](../plugins/versioning.md)
and [backup and recovery](../guide/recovery.md).

## Qualification

Shared CI and native release packaging must pass for the exact release source.
[Qualification evidence](../guide/qualification.md) tracks the checks and remaining
live/native scenarios. Publication does not establish live EDA/NSP qualification
or an installed upgrade rehearsal. Previous results do not qualify changed source.
