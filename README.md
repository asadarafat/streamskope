<h1 align="left">
  <img src="src/platform/ui/assets/streamskope.svg" width="72" height="72" align="absmiddle" alt="StreamSkope logo">
  StreamSkope
</h1>

[![PR CI](https://github.com/asadarafat/streamskope/actions/workflows/ci.yml/badge.svg?event=pull_request)](https://github.com/asadarafat/streamskope/actions/workflows/ci.yml?query=event%3Apull_request)
[![Ask DeepWiki](https://img.shields.io/badge/Ask-DeepWiki-blue)](https://deepwiki.com/asadarafat/streamskope)

A workbench for exploring and troubleshooting Kafka and NATS.

Inspect messages, investigate consumer lag, and manage cluster resources from
one application.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="website/docs/assets/messages-dark.png">
  <img src="website/docs/assets/messages.png" alt="StreamSkope message explorer with a selected Kafka record, its metadata and headers" width="1440">
</picture>

## Download

Download StreamSkope for macOS **ARM64**, Windows **x64** or Linux **x64** from the
[latest published release](https://github.com/asadarafat/streamskope/releases/latest). Each release
identifies its version, source revision and automated qualification evidence.
Desktop packages are **unsigned**. Verify the installer against its release's
`SHA256SUMS` and follow the [platform installation instructions](website/docs/start/installation.md).
Kafka is a separate service; the desktop installer does not require Node or Docker.
The [qualification record](website/docs/guide/qualification.md) links the release's
automated checks and identifies workflows that still need recorded evidence.

Prefer a browser workbench on a Linux Docker host? Follow
[Run with Containerlab](https://asadarafat.github.io/streamskope/start/containerlab/).
The latest release provides a public AMD64/ARM64 GHCR image and a Containerlab
topology pinned to its version and digest. Saved connection credentials are
protected by a browser-unlocked vault. Offline delivery includes Docker save
archives and a separate local-image topology.

StreamSkope also includes **NATS** as a built-in messaging provider.
Open **Connection Profiles → Add connection → NATS server** to save a
token/verified-TLS profile, connect it, subscribe to a known subject or wildcard,
and inspect live records. Read the
[NATS guide](website/docs/guide/core-nats.md) for storage, receipt and omission
limits. The desktop connects to the NATS server you supply; it does not embed
one or require the development fixture. Check your chosen release notes for its
supported provider capabilities.

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

Development builds use the neutral `0.0.0-dev` version. Release CI assigns the
selected version to its build checkout; PR checks qualify changes for `main`.
This placeholder does not indicate pending product changes. Changes awaiting
future releases are derived from merged PRs after each component's last published
stable release. Run `npm run docs -- pending` for the current committed-source
inventory. [Release highlights](website/docs/releases/unreleased.md) provide
additional context when an upgrade needs explanation.

## Fund development

If StreamSkope helps your work, you can support its continued development.

<a href="https://www.buymeacoffee.com/asadarafat"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy me a coffee" width="217" height="60"></a>

## License

[Apache-2.0](LICENSE).
