# Connect to Kafka or NATS

StreamSkope connects to the broker or server you supply. It does not run an
embedded Kafka or NATS server. Install the [desktop app](installation.md) or
[browser workbench](containerlab.md), then choose the connection you need.

| Your source   | Start with                                                              | What you need                                                           |
| ------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| Kafka brokers | [Connect your Kafka](../guide/connections.md)                           | Bootstrap addresses, authentication and certificate trust               |
| NATS server   | [Save a NATS profile](../guide/core-nats.md#save-and-connect-a-profile) | Server URL, authentication, trust and a subject you may subscribe to    |
| EDA export    | [EDA Connector](../plugins/eda.md)                                      | A compatible plugin, EDA API access and its administrator prerequisites |
| NSP Kafka     | [NSP Connector](../plugins/nsp.md)                                      | A compatible plugin and NSP API access; the host must also reach Kafka  |

Check [compatibility](compatibility.md) before requesting access. Kafka and NATS
are built in; only the platform connection workflows need plugins.

The **Connection Profiles** list shows both systems. Check its **System** column
and choose **Connect** on the intended profile. Saving or selecting a profile does
not connect it. Switching systems stops the current stream and disconnects the
original host connection first; an unresolved cleanup needs recovery before the
switch can complete.

Broker traffic comes from the StreamSkope host. For the desktop this is your
computer; for the browser workbench it is the StreamSkope container, not the browser tab.
That host must reach the server and any additional addresses it advertises.

After connecting Kafka, [read your first message](quickstart.md#3-open-a-record).
For NATS, [start a live subscription](../guide/core-nats.md#start-inspect-and-stop);
it receives matching publications while subscribed and does not browse historical
JetStream data. If connection fails, use
[connection troubleshooting](../guide/troubleshooting.md).
