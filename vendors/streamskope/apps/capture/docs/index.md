# StreamSkope Capture

StreamSkope Capture provides an EDA-managed bridge from an explicitly selected
EDA Kafka exporter source to a temporary Kafka endpoint consumed by StreamSkope.

The application keeps deployment authorization inside EDA. Desktop users do not
import Kubernetes credentials. Creating a session is explicit, bounded by a
lease, and separate from installing or uninstalling the application.

## Lifecycle

1. Install the signed application from the `streamskope` catalog.
2. Create a capture session for an existing EDA Kafka exporter source.
3. Inspect session readiness before connecting StreamSkope.
4. Stop the session or allow its lease to expire.
5. Confirm that exporter, broker, service and temporary storage owned by the
   session have been removed.

The original EDA Kafka exporter resource is never adopted or deleted by a
capture session.

## Operations

The agent runs as a non-root container with a read-only root filesystem behind an EDA `HttpProxy` using
`inApiServer` authentication. It does not create a `NodePort` or `LoadBalancer`.
Its ClusterRoleBinding grants cluster-wide access to capture sessions, Services,
StatefulSets and EDA Producer/ClusterProducer resources. Existing-resource ownership
checks are enforced by application code; RBAC is not restricted by those labels.
The temporary broker uses plaintext Kafka inside the cluster, and the copied
exporter's TLS/SASL settings are removed. The agent's NetworkPolicy permits TCP
8080 without restricting source pods; caller authentication relies on the EDA proxy.
Review [Security and permissions](https://asadarafat.github.io/streamskope/guide/security/)
when approving deployment.

Only one unexpired capture session is accepted across the cluster. The desktop
requests a 900-second lease and renews every 300 seconds; exiting stops renewal.
Expiry requires a functioning reconciler to complete deletion. Follow the
[EDA operator runbook](https://asadarafat.github.io/streamskope/guide/eda/)
for installation prerequisites, capacity and exact cleanup verification.

Session requests are limited to 32 KiB. Leases range from 30 to 900 seconds. The
Kafka tunnel accepts at most eight connections and 16 MiB per WebSocket message;
idle connections close after 60 seconds. These limits intentionally favor bounded
recovery over unattended, permanent infrastructure.
