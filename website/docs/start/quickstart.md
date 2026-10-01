# Read your first Kafka message

Install StreamSkope, connect a Kafka cluster you can access and inspect one record.
The released desktop app does not require a source checkout or development tools.
If you need a disposable local broker, use the separate
[development sandbox](development.md).

Before installing, [check compatibility](compatibility.md) for your cluster's
authentication method and message formats, and check the
[desktop prerequisites](installation.md#desktop-prerequisites).

## 1. Install and open StreamSkope

1. [Download the installer](installation.md#download) for your operating system and CPU.
2. Verify its checksum and follow the platform's installation instructions.
3. Launch StreamSkope under your normal desktop account with its credential service unlocked.
4. Open **Connection Profiles**.

**You should see:** the workbench and your saved profiles, or an empty profile list
on first use. The installer does not bundle Kafka or create a local broker.

## 2. Connect your Kafka

Have reachable bootstrap brokers, TLS trust material and any OAuth settings ready.
Ask your administrator for [inspection permissions](../guide/security.md#kafka-access-and-effects).

1. Choose **Add connection → Existing Kafka cluster**, enter a name and bootstrap brokers, and keep TLS enabled
   for a secured cluster. Select the matching PEM, JKS or PKCS12 trust material.
2. Configure OAuth if required. Follow [Connect your Kafka](../guide/connections.md#configure-manually)
   for field details and optional service connections.
3. Choose **Test connection** and resolve any reported error before saving.
4. Choose **Save profile**, then its connect/play button. Wait for **Connected**.

**You should see:** the topics visible to your account. Testing a draft does not
save or connect it. For Nokia EDA, use [Capture from EDA](../plugins/eda.md) to install
the optional plugin and prepare a capture profile.

## 3. Open a record

1. Open **Topics** and select a known topic containing data you are permitted to read.
2. Stop an active tail if needed. Choose **Newest N**, set a small limit such as
   `10`, then choose **Load messages**.
3. Select a row and open **Message details → Value**.
4. Check **Metadata** for the topic, partition, offset and headers.

**You have your first result:** a record and the broker position that identifies
it. If the view is empty, clear filters and confirm the topic contains retained
data. For a failed request, inspect **Raw logs** and use
[Troubleshooting](../guide/troubleshooting.md).

Message reads use manual assignment with automatic offset commits disabled;
they do not advance your application's consumer-group offsets. Configuration
changes, latency probes and other administrative actions have separate effects
described in [Security and permissions](../guide/security.md).

## Next: inspect and export carefully

[Filter a message and inspect its details →](../guide/messages.md)

Read [Data, exports and limits](../guide/data-handling.md) before using an export
as incident evidence. Before upgrading or downgrading the app, follow
[Backup and recovery](../guide/recovery.md).
