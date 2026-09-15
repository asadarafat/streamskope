# Read your first Kafka message

Start the local workbench, connect to the included AIO Kafka broker, and inspect
one record. You do not need an existing Kafka cluster for this walkthrough.

Already running StreamSkope with your own cluster? Go to
[Connect your Kafka](../guide/connections.md).

## Before you start

Use a local checkout of StreamSkope with **Node 24.x**, npm, Docker,
Containerlab and Java `keytool` available in the environment where you run the
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
    npm run dev:web
    ```

4. Wait for the launcher to print its browser address, then open that address.

**You should see:** StreamSkope with a **Local AIO Kafka** connection profile in
a fresh development session. The launcher prepares the local broker, OAuth
service and Schema Registry for you.

If `clab.orb.local` does not resolve, use this launch command on a direct local
host, then open the new address it prints:

```sh
STREAMSKOPE_DEV_PUBLIC_HOST=127.0.0.1 npm run dev:web
```

## 2. Connect to local Kafka

1. Open **Connection Profiles**.
2. Find **Local AIO Kafka** and click its connect/play button.
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

## Stop local Kafka when finished

The local broker stays available after you close the browser. To remove the
owned disposable fixture **and its data**, run:

```sh
npm run fixture:stop -- --name streamskope-kafka
```

Use your fixture name if you chose a different one. This command does not stop
external Docker or Containerlab workloads.
