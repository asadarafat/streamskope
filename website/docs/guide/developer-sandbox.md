---
title: Run a consumer sandbox
description: Start an owned, reproducible Kafka and Connect environment for bounded Node.js consumer and transform exercises.
---

# Run a consumer sandbox

Use this sandbox to try a Node.js consumer or a simple consume/transform/produce batch against disposable data. It starts Apache Kafka and Kafka Connect 4.3.1 through Docker Compose. It is separate from the AIO OAuth/Registry fixture used by the [source workbench](../start/development.md).

## Start and connect

Use the source archive for your desktop release, or a matching Git checkout. Install **Node 24.x**, npm and Docker with the **Compose v2** command available. Docker must be running. The pinned images need an initial download; no production credentials are required.

```sh
npm ci
npm run dev -- sandbox up
```

The command waits for Kafka and Connect, then verifies ten deterministic JSON records with keys `event-1` through `event-10` in `sandbox.events`. It also creates the empty `sandbox.processed` destination. Repeating `up` preserves those records and does not reseed them. An incomplete or altered seed fails verification; inspect the data before using the reset procedure below.

Create a **Plaintext** desktop connection profile with:

| Setting                | Default                  |
| ---------------------- | ------------------------ |
| Kafka bootstrap broker | `127.0.0.1:19096`        |
| Kafka Connect URL      | `http://127.0.0.1:18086` |
| Connect authentication | None                     |

Open `sandbox.events`, select **Earliest**, and read the ten seed records. [Kafka Connect](kafka-connect.md) exposes the installed example connector classes. Kafka Connect workers reach the broker at `kafka:29092` inside the Compose network.

Both published ports bind only to the Docker host's loopback address. Run the desktop on that host, or use an explicit trusted tunnel from your workstation. This sandbox does not configure OrbStack forwarding, remote access, TLS, OAuth or Schema Registry. Set `STREAMSKOPE_SANDBOX_KAFKA_PORT` and `STREAMSKOPE_SANDBOX_CONNECT_PORT` before `up` to avoid conflicts; use the same values for later commands.

## Exercise a consumer and transform

```sh
npm run dev -- sandbox status
npm run dev -- sandbox consume
npm run dev -- sandbox transform
```

`consume` prints the first ten records. `transform` reads the same bounded batch and writes a JSON envelope containing its source offset and value to `sandbox.processed`. Repeating `transform` deliberately creates another ten copies. It is not an exactly-once pipeline or a long-running consumer group. The supported runtime is Node.js 24 with the pinned `@platformatic/kafka` client; Kafka Streams, Flink and arbitrary topology orchestration are outside this initial sandbox.

For repeatable shell reads, the launcher writes private configuration and query files for the [read-only CLI](read-only-cli.md):

```sh
npm run dev -- cli query --config .artifacts/sandbox/connection.json --query .artifacts/sandbox/query.json
```

The seed definition and worker configuration are in `tools/sandbox.ts` and `sandbox/worker.properties`. Keep experimental changes in your own source branch. Schema-generated records can be prepared through [Schema Registry](schema-clients.md) when you supply a Registry separately.

## Limits and cleanup

Kafka is limited to 768 MiB and one CPU; Connect to 512 MiB and one CPU. Each JVM has a 256 MiB heap. The single broker has no replication safety, and its default retention limit is 16 MiB per partition. This is a small development environment, not a throughput or durability benchmark. Avoid producing unlimited data.

```sh
npm run dev -- sandbox down
npm run dev -- sandbox up
```

`down` removes the owned Compose containers, network and disposable records. The next `up` creates a fresh seed. Containers, networks and volumes under the `streamskope-sandbox` project must carry this checkout's ownership marker; the command refuses foreign resources. Other Docker projects and the AIO fixture are unaffected. Only one checkout can own this project at a time.

Keep `.artifacts/sandbox/instance` until cleanup completes. An operation lock prevents concurrent commands from the same checkout. If a process is forcibly killed, verify that no sandbox command is running before removing its stale `.artifacts/sandbox/operation.lock`; do not delete ownership files to bypass an ownership failure. An interrupted `up` leaves owned resources available for inspection and an explicit `down`.
