# Everyday tasks

Connect to your broker or server, then read a message. These guides cover the
normal path from a saved connection to inspecting values, keys and headers.

| Step          | Guide                                               | Result                                                       |
| ------------- | --------------------------------------------------- | ------------------------------------------------------------ |
| 1. Connect    | [Connect to Kafka or NATS](../start/connections.md) | A connected profile for the source you want to read          |
| 2. Read Kafka | [Read Kafka messages](messages.md)                  | A bounded read or live tail, with filters and record details |
| 2. Read NATS  | [Read NATS subjects](core-nats.md)                  | A live subscription to a subject you are allowed to read     |

Choose the reading guide for your connection. Kafka can read retained records;
NATS subscriptions here receive new publications while subscribed.

If a connection fails, a message is missing, or you need to compare, diagnose or
change something, continue with [Under the Skope](../skope/index.md).
