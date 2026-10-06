<h1 align="left">
  <img src="src/platform/ui/assets/streamskope.svg" width="72" height="72" align="absmiddle" alt="StreamSkope logo">
  StreamSkope
</h1>

[![PR CI](https://github.com/asadarafat/streamskope/actions/workflows/ci.yml/badge.svg?event=pull_request)](https://github.com/asadarafat/streamskope/actions/workflows/ci.yml?query=event%3Apull_request)
[![Ask DeepWiki](https://img.shields.io/badge/Ask-DeepWiki-blue)](https://deepwiki.com/asadarafat/streamskope)

A desktop workbench for exploring and troubleshooting Kafka.

Inspect messages, investigate consumer lag, and manage cluster resources from
one application.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="website/docs/assets/messages-dark.png">
  <img src="website/docs/assets/messages.png" alt="StreamSkope message explorer with a selected Kafka record, its metadata and headers" width="1440">
</picture>

## Download

Download StreamSkope for macOS **ARM64**, Windows **x64** or Linux **x64** from
[GitHub releases](https://github.com/asadarafat/streamskope/releases). Each release
identifies its version, source revision and automated qualification evidence.
Desktop packages are **unsigned**. Verify the installer against its release's
`SHA256SUMS` and follow the [platform installation instructions](website/docs/start/installation.md).
Kafka is a separate service; the desktop installer does not require Node or Docker.
The [qualification record](website/docs/guide/qualification.md) links the release's
automated checks and identifies workflows that still need recorded evidence.

This checkout is development source, not an assigned release. See the
[unreleased changes](website/docs/releases/unreleased.md). A maintainer chooses the
version when starting release CI; PR checks only qualify changes for `main`.

This checkout also includes **NATS** as a built-in messaging provider.
Open **Connection Profiles → Add connection → NATS server** to save a
token/verified-TLS profile, connect it, subscribe to a known subject or wildcard,
and inspect live records. Read the
[NATS guide](website/docs/guide/core-nats.md) for storage, receipt and omission
limits. The desktop connects to the NATS server you supply; it does not embed
one or require the development fixture. Check your desktop release notes for provider availability; merging source
does not replace a published installer or its documentation snapshot.

## What you can do

- Browse topics, read and filter messages, inspect original record bytes and export filtered JSON.
- Decode JSON, Confluent Avro and Protobuf, compare records and trace correlation values across topics.
- Create topics and publish reviewed records or bounded schema-generated samples.
- Preview and apply consumer offset resets, copy records across topics or clusters, and review exact ACL changes.
- Use host read-only controls and deterministic record masking.
- Investigate consumer lag, stream activity and producer/consumer probe latency.
- Retain bounded health observations, inspect lag trends and review evidence-linked diagnostic hints.
- Explore observed topic, consumer and connector relationships and partial schema-change impact.
- Manage topic configuration, Schema Registry and ACLs.
- Manage Kafka Connect, validate connector configuration and inspect supported DLQ context.
- Compare topic settings across clusters and promote selected, reviewed differences.
- Test message rules and inspect deployed Redpanda transforms.
- Save TLS and OAuth connection profiles, with optional SSH or HTTPS secret retrieval.

Kafka authentication supports **OAUTHBEARER or no SASL**. Avro/Protobuf inspection
requires a separate Schema Registry and an explicit encoding selection in
[Message details → Decoded](website/docs/guide/structured-events.md).
Transform operations require Redpanda. Check the [compatibility matrix](website/docs/start/compatibility.md)
for implemented capabilities, tested environments and limitations.

## Read your first message

1. [Install and open StreamSkope](website/docs/start/installation.md) for your platform.
2. [Connect your Kafka](website/docs/guide/connections.md) using reachable brokers,
   trust material and any required OAuth credentials.
3. [Read your first message](website/docs/start/quickstart.md#3-open-a-record) from a known topic.

Use an inspection account with the [required permissions](website/docs/guide/security.md).
Producing or replaying records, resetting offsets, creating topics, configuration changes, ACL/schema operations
and latency probes can write to your services.

## Documentation and help

These repository guides describe this checkout. The
[published documentation](https://asadarafat.github.io/streamskope/) is built from the exact published desktop release source. Its version notice and
downloads identify that release; unreleased changes stay in repository/local previews.
Installer availability is verified before the site is published.

- [Messages and filtering](website/docs/guide/messages.md), [consumer lag](website/docs/guide/operations.md)
  and [Schema Registry](website/docs/guide/schema-registry.md)
- [Observed health and diagnostic limits](website/docs/guide/observed-health.md)
  and [relationships and schema impact](website/docs/guide/relationships.md)
- [Security and permissions](website/docs/guide/security.md),
  [backup and recovery](website/docs/guide/recovery.md), and [data and export limits](website/docs/guide/data-handling.md)
- [Troubleshoot a problem](website/docs/guide/troubleshooting.md)
- [Report a defect](https://github.com/asadarafat/streamskope/issues): include the
  exact release tag, OS/architecture, action and redacted error. Exclude credentials,
  tokens, private payloads and complete application-data directories.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for the five project commands, checks and
release process. The [source workbench](website/docs/start/development.md)
provides disposable local Kafka and requires Node, Docker, Containerlab and Java.

## Fund development

If StreamSkope helps your work, you can support its continued development.

<a href="https://www.buymeacoffee.com/asadarafat"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy me a coffee" width="217" height="60"></a>

## License

[Apache-2.0](LICENSE).
