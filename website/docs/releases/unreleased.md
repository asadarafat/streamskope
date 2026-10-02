---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

## Original Kafka records

The **Original** inspector tab and version 2 message exports preserve bounded
key/value bytes and ordered duplicate/null headers as Base64. Null and empty
values remain distinct. Originals above 256 KiB or the header bounds are explicitly
unavailable; display previews must not be used for byte-exact writes. Retention
accounting includes original envelopes, headers and previews. The paired host
protocol is **32**; upgrade the host and renderer together. Plugin API is unchanged.

Add reviewed highlights, upgrade instructions and limitations here as changes merge.
A maintainer assigns the next desktop version when starting release CI.
