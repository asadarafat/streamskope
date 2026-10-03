# Security and permissions

Review this page with the Kafka and EDA administrators before connecting production
systems. Use an inspection-only account for investigation; grant write access only
for the operations that account must perform.

## Kafka access and effects

The table maps StreamSkope's requests to Apache Kafka ACL operations. Broker vendors
can apply additional authorization rules; verify the intended actions with your
principal before widening access. The [qualification record](qualification.md) distinguishes a real Kafka 4.3.1
restricted-account rehearsal from request-derived requirements that have not been
verified across vendors.

| Workflow                                    | Kafka permission/resource                                                                                                              | Effect                                                                                                                                            |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| List topics and inspect partition offsets   | `Describe` on the selected topics                                                                                                      | Reads metadata and offsets                                                                                                                        |
| Read messages                               | `Read` and `Describe` on selected topics; `Read` on generated `streamskope-` groups                                                    | Fetches records with manual assignment and automatic offset commits disabled                                                                      |
| List and inspect consumer lag               | `Describe` on the relevant groups and topics                                                                                           | Reads membership, committed offsets and end offsets; does not reset offsets                                                                       |
| Observe health and discover relationships   | `Describe` on selected topics and visible consumer groups; optional record sampling also needs topic `Read` and generated-group `Read` | Reads bounded metadata/positions; optional samples do not commit application offsets. Connect and Registry reads need independent HTTP permission |
| Preview and reset consumer offsets          | Group `Describe` and `Read`; topic `Describe` and `Read` for selected partitions; generated preview groups also need `Read`            | Preview reads positions/examples; confirmed application changes only selected committed offsets                                                   |
| Copy or replay records                      | Target topic `Write` and `Describe`, plus `Read` and generated-group `Read` for read-back                                              | Preview does not produce; confirmed replay writes a bounded selection with new offsets                                                            |
| Explain topic access and review ACL changes | Cluster `Describe`; cluster `DescribeConfigs` adds policy evidence                                                                     | Reads bindings and visible policy; unknown policy is explicit                                                                                     |
| Read topic configuration                    | `DescribeConfigs` on the topic                                                                                                         | Reads configuration                                                                                                                               |
| Read broker configuration                   | `DescribeConfigs` on the cluster                                                                                                       | Reads broker configuration                                                                                                                        |
| Change topic configuration                  | `AlterConfigs` on the topic                                                                                                            | Writes the reviewed settings                                                                                                                      |
| Inspect ACLs                                | `Describe` on the cluster                                                                                                              | Reads access rules                                                                                                                                |
| Create or delete ACLs                       | `Alter` on the cluster                                                                                                                 | Changes authorization                                                                                                                             |
| Run a latency probe                         | Topic `Write`, `Read` and `Describe`; Group `Read` for `streamskope-latency-` groups, plus any broker-specific producer permissions    | Produces real records and consumes probe results                                                                                                  |

Group listings may contain only authorized groups. An empty inventory is not proof
that no groups exist. Message inspection uses generated StreamSkope consumer
identifiers; it does not join or commit offsets for your application consumer group.
The current client still performs coordinator lookup and group join for its own
generated group even with manual partition assignment. Grant Group `Read` on the
`streamskope-` prefix (which includes the required Describe access); Group Describe
alone fails at JoinGroup. This does not require Group access to unrelated application
groups unless you also inspect their lag. Automatic offset commits remain disabled.
The [Apache Kafka authorization reference](https://kafka.apache.org/40/security/authorization-and-acls/)
defines the protocol permissions behind this table.

Schema Registry and Redpanda Admin authorization are separate. Registry inspection
needs subject/schema reads; registration, compatibility checks and deletion need
their corresponding API access. Transform inspection needs Admin API reads, logs
also require access to the transform log topic, and deletion needs Admin API write
access. Kafka sign-in alone does not grant either service's access.

[Relationships](relationships.md) checks Connect's reported Kafka cluster ID
before associating its connectors with the current broker. It reads connector
information and tracked topics without changing configuration. Registry reads
discover only visible, bounded subject versions; missing permissions leave coverage
unknown. Observed health records client-observed offsets and request timings,
not broker CPU, disk utilization or application processing latency.

For an inspection account, test topic listing, a bounded read and any required group
or schema view. Have the administrator confirm that produce, configuration changes,
ACL changes and schema/transform deletion are denied. Do not test destructive
denials against a production object; use a disposable environment or policy review.

## Desktop and plugin trust

Release installers are currently unsigned; follow the [download verification
instructions](../start/installation.md). A matching checksum verifies downloaded
bytes, not a publisher signature.

The plugin catalog accepts published packages from `asadarafat/streamskope`, checks
GitHub's asset SHA256 digests and validates the package manifest/API version. This
is repository provenance and integrity verification, not independent cryptographic
publisher signing. The plugin backend runs as trusted code inside the desktop host
with that process's permissions. It is not a sandbox for arbitrary extensions.
Plugin UI runs through the host bridge without direct Node access.

Saved profile secrets depend on the OS credential service. Other local metadata
and exported messages have different protection; review [Data and exports](data-handling.md)
and [Backup and recovery](recovery.md) before sharing files or changing machines.

## EDA authorization

Desktop users authenticate to EDA; they do not need a Kubernetes credential file.
Ask the EDA administrator to authorize the operations needed for each role:

| Role/task                  | Required EDA access and effect                                                                                                                        |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Discover sources           | Read `/core/about/version` and the Kafka `Producer`/`ClusterProducer` inventories                                                                     |
| Capture user               | Read app readiness and use the `streamskope-capture` HTTP proxy to create, read, renew, stop and tunnel sessions                                      |
| Installation administrator | Read/create the `streamskope` Catalog and `streamskope-capture` SigningKey, submit/read an `AppInstaller`, and satisfy EDA's application requirements |

These are operations, not invented EDA role names. Map them to your site's EDA
authorization policy. EDA sign-in does not grant access to an existing external
Kafka destination. See the [EDA installation prerequisites](../plugins/eda.md#administrator-prerequisites)
before authorizing the one-time cluster installation.

The installed agent uses a `ClusterRoleBinding`. Its ClusterRole grants:

- `get`, `list`, `create`, `update`, `patch`, `delete` on `CaptureSession` resources;
- `get`, `patch`, `update` on their status;
- `get`, `create`, `patch`, `delete` on Services, StatefulSets, Producers and
  ClusterProducers across the cluster.

These permissions are not restricted to resource names or ownership labels.
The application checks ownership labels before managing existing capture resources;
that is an application safeguard, not a Kubernetes authorization boundary. Review
the deployment manifest for the exact cluster-app artifact being installed when
approving the app. The [original release manifest](https://github.com/asadarafat/streamskope/blob/v0.1.0%2Bbuild.1/vendors/streamskope/apps/capture/agent/config/deployment.yaml)
is historical evidence; compare it with the intended artifact rather than assuming
the original file describes every later build.

## EDA transport boundaries

| Connection                          | Current protection                                                                          |
| ----------------------------------- | ------------------------------------------------------------------------------------------- |
| Desktop → EDA API/proxy             | HTTPS and an authenticated WebSocket tunnel; certificate verification is enabled by default |
| EDA proxy → capture agent           | HTTP inside the cluster; the proxy uses `inApiServer` authentication                        |
| Copied exporter → temporary broker  | Plaintext Kafka inside the cluster; the copy's original TLS/SASL settings are removed       |
| Local Kafka client → desktop tunnel | Plaintext on loopback; the tunnel carries traffic onward to EDA                             |

The temporary broker uses ClusterIP listeners on ports 9092/9093, not NodePort or
LoadBalancer. ClusterIP does not imply encryption or isolation from other pods.
The shipped agent NetworkPolicy allows TCP 8080 without restricting source pods;
the agent relies on the EDA proxy for caller authentication. The capture deployment
does not provide per-session broker network isolation. Production approval must
account for the cluster's network controls and trusted workloads. Keep EDA TLS
verification enabled. The plugin has no custom-CA upload; review the
[supported API trust and private-CA limitation](tls-trust.md#eda-and-nsp-api-certificates)
before approving an endpoint. Broker-profile trust does not configure the EDA API.

## Host read-only mode

Disconnect Kafka and finish any active plugin capture or cleanup, then enable
**Preferences → Protection → Read-only mode**. The status bar shows **Read-only**.
The host rejects remote mutations before dispatch, including direct host commands:
Kafka configuration and environment promotion, Connect lifecycle actions, offset resets, record replay and ACL writes, Schema Registry registration and deletion,
transform deletion, latency probes, remote credential acquisition, and plugin
actions or installation/removal. New commands must declare their access category.

Ordinary Kafka connections, bounded message reads, metadata and validation requests,
health observations, relationship discovery and local history controls remain available.
Managed plugin profiles require lifecycle hooks, so connecting
or testing them is blocked in this mode. Use an ordinary saved Kafka profile with
credentials that the broker restricts to reading, or deliberately disable the mode
before managing a capture. Plugin state changes and remote cleanup also require
leaving read-only mode; finish them before enabling it.

This is an operator safeguard, not role-based authorization. A local user can
change it, and trusted installed plugin code is not sandboxed. Broker and Registry
permissions remain the final enforcement boundary. The [source CLI](read-only-cli.md) exposes only inspect/query/export. It uses explicit private connection and masking configuration, always enables read-only behavior, and rejects broker-side search while masking is active. It does not inherit desktop preferences or activate plugins. New host commands must use the same access policy.

Workbench preference reset preserves protection settings. If preferences cannot
be read, remote operations fail closed. Restore the file or explicitly reset while
disconnected; recovery enables read-only and masks keys and values. Review header
masking before reconnecting. Browser-development preferences last only for the host
process; desktop preferences persist in application storage.
