# Source workbench with local Kafka

Start the local workbench, connect to the included AIO Kafka broker, and inspect
one record. You do not need an existing Kafka cluster for this walkthrough.

This walkthrough is for a source checkout. For the released application, use the
[desktop quickstart](quickstart.md).

For a smaller consumer/transform exercise with a Kafka Connect worker, use the
[consumer sandbox](../guide/developer-sandbox.md).

Already running StreamSkope with your own cluster? Go to
[Connect your Kafka](../guide/connections.md).

## Before you start

Use a local checkout of StreamSkope with **Node 24.x**, npm, Docker,
Containerlab, Java `keytool` and OpenSSL available where you run the
commands. Docker must be running. The first launch builds or downloads the
fixture images and can take several minutes.

Prefer the desktop app? Follow [Install StreamSkope](installation.md),
then [connect your cluster](../guide/connections.md).

## 1. Start the workbench

1. Open a terminal in the StreamSkope repository root.
2. Install the dependencies:

    ```sh
    npm ci
    ```

3. Start the workbench and local Kafka together:

    ```sh
    npm run dev
    ```

4. Wait for the launcher to print its browser address, then open that address.

**You should see:** StreamSkope with a **Local AIO Kafka** connection profile in
a fresh development session. The launcher prepares the local broker, OAuth
service and Schema Registry for you.

WebDev derives the current `<vm>.orb.local` hostname inside OrbStack and uses
loopback elsewhere. To force direct loopback access, run:

```sh
STREAMSKOPE_DEV_PUBLIC_HOST=127.0.0.1 npm run dev
```

## 2. Connect to local Kafka

1. Open **Connection Profiles**.
2. Find **Local AIO Kafka**, check that its **System** is Kafka, and select **Connect**.
3. Wait for **Connected** in the bottom status bar.

**You should see:** the topic list. If connection fails, open **Raw logs** at the
bottom and follow [Fix a connection problem](../guide/troubleshooting.md).
If the local profile is missing, use [manual connection setup](../guide/connections.md)
with the endpoints and CA path printed by fixture startup. Existing profiles
are retained when the launcher starts.

## 3. Open a record

1. Open **Topics** in the left navigation.
2. Select `test`, the topic created by the included AIO fixture.
3. Wait for a record in **Messages**, then select its row.
4. In **Message details**, open **Value** to read the payload.
5. Open **Metadata** to find its topic, partition, offset and headers.
6. Click **Stop tail** when you have finished watching the topic.

**You have your first result:** a message read from Kafka, its value, and the
partition and offset that identify its position. No new traffic is required;
the included fixture already contains a record. If the table is empty, clear
filters and check the selected read mode before retrying.

## Next: find a specific message

[Filter a message and inspect its details →](../guide/messages.md)

To connect a different broker, follow [Connect your Kafka](../guide/connections.md).

<span id="try-local-core-nats"></span>

## Try local NATS

The launcher also prepares the private `aio-nats` lab and a **Local AIO NATS**
session profile. In **Connection Profiles**, connect **Local AIO NATS** from its
NATS row, then open **Live Subscription**. Enter `streamskope.fixture.>` and select
**Start subscription** before publishing a sample batch:

```sh
npm run dev -- nats publish
```

**You should see:** generated records in **NATS records**, with their original
payloads and headers available in **Record inspector**. Live NATS subscriptions receive future
traffic only; publishing before subscribing leaves no historical records to read.
Repeat the command to send another bounded batch. These are local generated
fixtures, separate from live EDA or NSP traffic.

The default batch publishes two records per second for 60 seconds. For a shorter
run, use `npm run dev -- nats publish --seconds 5 --rate 2`.

Like `aio-kafka`, the persistent `aio-nats` lab uses Containerlab. Its source
topology is `aio-nats/topology.clab.yml`; startup gives each instance a unique
lab name and a daemon-assigned management subnet. The lab uses verified TLS and
a private generated token. Startup loads both into
the development host without printing the token. Browser profiles stay in memory;
the lab's credentials and ownership record stay under `aio-nats/ownership/`, which
Git ignores. See [NATS live subscriptions](../guide/core-nats.md) for subscription and recovery
semantics.

To operate the lab independently:

```sh
npm run dev -- nats start
npm run dev -- nats status
npm run dev -- nats stop
```

Repeated startup reuses the owned server and credentials. Stop checks the
server, private volume and management network before erasing private files; it
preserves ownership evidence if cleanup is unconfirmed. Existing Docker-only
NATS fixtures keep working until an explicit stop/start migrates them. This
fixture uses a local Unix Docker endpoint. See the checkout
[`aio-nats/README.md`](https://github.com/asadarafat/streamskope/blob/main/aio-nats/README.md)
for prerequisites and interrupted-deployment recovery.

## Test a development plugin

Source builds use the development identity `0.0.0-dev`. They load current-API
development packages; test published plugins with a compatible released desktop.

1. Build the local EDA and NSP packages:

    ```sh
    npm run package -- plugin
    ```

2. Start or return to the workbench launched by `npm run dev`.
3. Open **Preferences → Plugins** and select **Check for updates**.
4. Choose **Install**, or **Update to** the new development version.

**You should see:** the plugin's connection action becomes available without a
restart. Development reads verified packages from `dist/plugin-package/`; startup
does not build them automatically or fetch published packages from GitHub.
After changing plugin source, rebuild, refresh and update. Each local build gets
a distinct development identity, so updates retain the normal cleanup lifecycle.

Keep `.cache/development-plugins` and its capture/recovery state. If an older
checkout installed a release-versioned plugin there, use that compatible build
to complete owned-work cleanup and remove it before installing a development
package. Do not delete the cache to bypass cleanup. Packaging alone does not
qualify a live EDA or NSP connection; follow the plugin's target prerequisites.

## Stop local Kafka when finished

The local broker stays available after you close the browser. To remove the
owned disposable fixture **and its data**, run:

```sh
node --import tsx tools/dev/kafka-fixture/cli.ts stop --name streamskope-kafka
```

Use your fixture name if you chose a different one. This command does not stop
external Docker or Containerlab workloads.
