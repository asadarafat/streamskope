# About StreamSkope

StreamSkope is an open-source workbench for Kafka and NATS operators and developers,
available as a desktop app or a browser workbench on a Linux Docker host.
It brings connection setup, message inspection and operational evidence into one
window. The source is licensed under Apache-2.0.

## Current scope

Kafka and NATS are built-in providers connecting to the remote servers you supply;
StreamSkope does not embed either server. Kafka workflows can use a separate
Schema Registry, Connect REST endpoint or Redpanda Admin API.
[NATS inspection](guide/core-nats.md) subscribes to live subjects; JetStream history,
stream administration and consumer replay are outside its current scope.
EDA and NSP are optional [Kafka connection plugins](plugins/index.md).

See [installation](start/installation.md) for desktop packages, supported build
targets and platform security requirements. For browser delivery, follow
[Install the browser workbench](start/containerlab.md) and
[Use the browser workbench](guide/browser-host.md).

## Website and media

This site uses [Zensical](https://zensical.org/). The launch composition is rendered
with [HyperFrames](https://github.com/heygen-com/hyperframes). Product screenshots
show data read from the local AIO Kafka broker and are licensed with StreamSkope. The launch film's
original synthesized soundtrack is also licensed under Apache-2.0.
