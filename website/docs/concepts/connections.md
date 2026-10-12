# Connections and capture lifecycles

A saved profile, a running connection and a remote capture are separate things.
Treating them as one lifecycle can leave remote work running or make a saved
profile appear to promise resources that no longer exist.

## A profile stores how to connect

Saving or editing a profile does not activate it. The host tests or connects only
when requested. Protected credentials belong to that host: the desktop uses its
operating-system credential service; the production browser host uses its encrypted
vault. A browser tab is a client of that host, not the place where Kafka, NATS or
platform API requests originate.

The workbench has one active connection. Switching profiles first stops the
current stream and disconnects its original host connection. A failed cleanup
cannot be turned into success by selecting another profile. Saved settings and
completed evidence are also different from permission to resume a stopped read.

## Connection plugins prepare Kafka access

Kafka and NATS are built-in providers. EDA and NSP plugins extend Kafka profile
setup and management through their platform APIs; the core Kafka reader still
handles messages. Installing a plugin neither grants target permissions nor
creates source events.

EDA can connect to an existing destination or create a temporary broker and
exporter. Disconnecting Kafka stops the client connection; it does not remove
those remote capture resources. Removing a capture discards its temporary
messages. A retained profile cannot recover those messages after the resources
are gone.

NSP prepares access to its existing broker. Its reusable helper workflow and its
individual executions have different lifetimes. Disconnecting the Kafka client
does not delete the shared helper or the source notifications. Neither the
workflow nor a saved profile creates a tunnel to the NSP broker.

## Completion needs evidence from the owner

A disconnected browser cannot prove that host work stopped. Similarly, an expired
EDA lease makes resources eligible for cleanup, but the agent still needs to
reconcile them. An unreachable API or failed deletion leaves cleanup unresolved.
Recovery must use the original owner and its retained state, rather than deleting
local metadata to hide the uncertainty.

The exact actions remain in the [EDA lifecycle guide](../plugins/eda.md#stop-update-and-resume),
[NSP workflow guide](../plugins/nsp.md) and
[browser host guide](../guide/browser-host.md). Use
[Connect to Kafka or NATS](../start/connections.md) to choose a connection and
[Under the Skope](../skope/index.md) to diagnose its event path.
