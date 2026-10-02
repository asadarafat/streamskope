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
protocol is **33**; upgrade the host and renderer together. Plugin API is unchanged.

## Read-only and record masking

Preferences → Protection controls host-enforced read-only operations and deterministic
key, header and JSON-path masking. Direct host commands and supported plugin
lifecycle entry points use the same guard. Masking precedes display, rule evaluation,
copy and export; original bytes cannot bypass it. Save protection while disconnected
after finishing plugin work. Broker-side search is unavailable while masking is
active; local filters work on the masked records. These are local operator controls,
not multi-user authorization or a plugin sandbox.

Add reviewed highlights, upgrade instructions and limitations here as changes merge.
A maintainer assigns the next desktop version when starting release CI.

### Reviewed Kafka writes

- Produce one bounded record with UTF-8 or Base64 bytes, null keys, tombstones and
  ordered duplicate headers. Confirm the destination, then retain the acknowledged
  offset independently of read-back success.
- Create a topic with explicit partitions, replication and selected settings.
  Existing topics are left untouched. Duplicate confirmations reuse one attempt;
  unknown outcomes require inspection before a new attempt.
- Host protocol 34 adds typed write reviews and outcomes. Read-only mode blocks
  all write confirmations.
