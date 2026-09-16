<p align="center">
  <img src="src/platform/ui/assets/streamskope.svg" width="72" height="72" alt="StreamSkope logo">
</p>

# StreamSkope

[![CI](https://github.com/asadarafat/streamskope/actions/workflows/ci.yml/badge.svg)](https://github.com/asadarafat/streamskope/actions/workflows/ci.yml)
[![Documentation](https://github.com/asadarafat/streamskope/actions/workflows/docs.yml/badge.svg)](https://github.com/asadarafat/streamskope/actions/workflows/docs.yml)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/asadarafat/streamskope)

A desktop workbench for exploring and troubleshooting Kafka.

Inspect messages, investigate consumer lag, and manage cluster resources from
one application.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="website/docs/assets/messages-dark.png">
  <img src="website/docs/assets/messages.png" alt="StreamSkope message explorer with a selected Kafka record, its metadata and headers" width="1440">
</picture>

## What you can do

- Browse topics, filter messages, inspect payloads and export records.
- Monitor consumer groups, stream activity and latency.
- Manage topic configuration, Schema Registry and ACLs.
- Test message rules and inspect deployed Redpanda transforms.
- Save connection profiles with TLS and OAuth, with optional SSH or HTTPS
  secret retrieval.

Schema Registry requires a configured service. Deployed transforms require
Redpanda; they are not a standard Kafka capability.

## Get started

[Download for macOS, Windows or Linux](https://github.com/asadarafat/streamskope/releases/latest) ·
[Documentation](https://asadarafat.github.io/streamskope/) ·
[Install StreamSkope](https://asadarafat.github.io/streamskope/start/installation/) ·
[Try it with local Kafka](https://asadarafat.github.io/streamskope/start/quickstart/) ·
[Connect your cluster](https://asadarafat.github.io/streamskope/guide/connections/)

## Documentation

- [Messages and filtering](https://asadarafat.github.io/streamskope/guide/messages/)
- [Consumer lag](https://asadarafat.github.io/streamskope/guide/operations/)
- [Schema Registry](https://asadarafat.github.io/streamskope/guide/schema-registry/)
- [Optional secret retrieval](https://asadarafat.github.io/streamskope/guide/secret-retrieval/)
- [Troubleshooting and profile recovery](https://asadarafat.github.io/streamskope/guide/troubleshooting/)

## Support StreamSkope

If StreamSkope helps your work, you can support its continued development.

<a href="https://www.buymeacoffee.com/asadarafat"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy me a coffee" width="217" height="60"></a>

## License

[Apache-2.0](LICENSE).
