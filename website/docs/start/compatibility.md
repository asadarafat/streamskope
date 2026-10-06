# Check compatibility before connecting

The capabilities below describe this source snapshot. The site banner identifies
its desktop release or marks it as development documentation. Plugin requirements differ by generation,
as described below. Implemented means
the application exposes the capability; it does not qualify every vendor/version.
See [qualification evidence](../guide/qualification.md) for what was actually exercised.

## Connection and data capabilities

| Capability               | Implemented scope                                                                                        | Boundary                                                                                                                                                               |
| ------------------------ | -------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Kafka transport          | TLS with server certificate validation; explicitly selected plaintext                                    | No mutual-TLS client certificate/key configuration                                                                                                                     |
| Kafka authentication     | OAuth 2.0 client-credentials token retrieval with SASL OAUTHBEARER, or no SASL                           | SASL PLAIN, SCRAM and Kerberos are not configurable                                                                                                                    |
| Broker trust             | PEM CA, JKS and PKCS12 trust material                                                                    | Truststores provide CA trust, not client identity                                                                                                                      |
| Message inspection       | UTF-8 views plus explicit JSON, Confluent Avro/Protobuf decoding                                         | Declared schema-ID framing only; bounded complete originals required; decoded projections do not replace original bytes or change table filters                        |
| Reviewed writes          | Create topics, produce records and replay a bounded selection after exact review                         | Replay is limited to 50 complete records; unknown outcomes require reconciliation before another attempt                                                               |
| Offset recovery          | Preview per-partition positions and apply to an inactive group                                           | Up to 32 partitions; no atomic reset or automatic rollback; stale and partial results are explicit                                                                     |
| Topic access review      | Explain one topic/principal/client address and review exact ACL changes                                  | Kafka StandardAuthorizer evidence only; unknown broker policy and external authorization remain explicit                                                               |
| Record protection        | Host read-only guard and deterministic key/header/JSON-path masking                                      | Local operator controls; broker permissions remain authoritative                                                                                                       |
| Message reads            | Tail, Newest N, First N and recent or custom Time window                                                 | Reads retained Kafka records within explicit limits; see [read semantics](../guide/messages.md#choose-a-read-mode)                                                     |
| Schema Registry          | Confluent-compatible HTTP API; Avro, JSON Schema and Protobuf schema definitions                         | Writer schema IDs/references support explicit decoding; auth is none or the profile's OAuth token, not HTTP Basic                                                      |
| Redpanda transforms      | Redpanda Admin API and transform-log topic                                                               | Not an Apache Kafka feature; independent service permissions and connectivity required                                                                                 |
| Kafka Connect            | Discover, validate and review connector lifecycle and failed-task restart through Connect REST           | Apache Connect 4.3.1 FileStream fixture; HTTP auth is none or profile OAuth, not Basic; DLQ support depends on the connector                                           |
| Environment comparison   | Versioned snapshots and selected promotion of seven existing topic settings                              | 20-topic capture; no other resource types, automatic reconciliation or atomic multi-topic transaction                                                                  |
| Generated clients        | Node 24 JavaScript CommonJS JSON Schema draft-07 codec and producer helper, Ajv 8.20.0                   | Self-contained supported keywords only; no Avro/Protobuf client generation or schema-ID translation                                                                    |
| Source CLI               | Bounded inspect/query/export with explicit private connection and protection settings                    | No mutation commands, desktop profile access or plugin activation; masked broker search is rejected                                                                    |
| Consumer sandbox         | Owned Docker Compose Kafka/Connect with deterministic seeds and bounded Node 24 consumer/transform batch | Loopback plaintext, no Registry/OAuth, no Kafka Streams/Flink runtime, disposable data                                                                                 |
| Observed health          | Bounded local offset/health history, thresholds, backtested lag scenarios and diagnostic hints           | Client observations only; no broker CPU/disk, processing-latency measurement or closed-app monitoring; optional protected record sampling is bounded and can be biased |
| Relationships and impact | Timestamped topic/group/Connect evidence and partial exact-version schema impact                         | Three topics; bounded API-visible coverage; inferred framing/naming links remain distinct; no exhaustive producer inventory or safe-change guarantee                   |
| EDA capture              | EDA Capture, API 3 published packages and API 4 development packages                                     | Requires exactly EDA 26.8.2 and the separate matching cluster app                                                                                                      |
| NSP capture              | NSP Capture, API 3 published packages and API 4 development packages                                     | Requires exactly NSP 26.4.0 and the workflow/mounted-trust layout                                                                                                      |

TLS failures do not fall back to plaintext. HTTP service authentication and Kafka
broker authentication must each be accepted by their destination. A valid Kafka
OAuth token is not necessarily valid for the Registry, Connect or Redpanda Admin API.

## Test baselines and support boundaries

| Component                 | Repository baseline                 | Qualification boundary                                                                                                                    |
| ------------------------- | ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Apache Kafka              | AIO fixture pins 4.3.1              | Local fixture provides OAuth/TLS; first-release qualification is recorded separately                                                      |
| Apache Kafka Connect      | Docker fixture pins 4.3.1           | FileStream sink lifecycle, task failure/restart and supported error-context DLQ; not every connector plugin                               |
| Schema Registry           | Karapace 5.0.3                      | Confluent-compatible fixture; not certification of Confluent Cloud or every Registry vendor                                               |
| EDA                       | 26.8.2                              | Live tests require the configured local cluster; ordinary GitHub checks cannot establish cluster compatibility                            |
| NSP                       | 26.4.0                              | Product read through the NSP API; declared target does not qualify every listener, authentication mode or workflow environment            |
| Temporary EDA broker      | Redpanda v24.3.5                    | Pinned capture broker, not qualification of all Redpanda Admin/transform operations                                                       |
| Desktop operating systems | macOS ARM64, Windows x64, Linux x64 | See [native release-check environments](installation.md#desktop-prerequisites); other architectures/minimum OS versions are not qualified |

Do not infer managed-service support from protocol compatibility alone. Obtain the
service's required authentication method, endpoints and permissions first. If they
fall outside the implemented scope, changing broker security to fit this client is
not part of the installation procedure.

## Plugin compatibility declarations

| Package generation       | Desktop requirement                                                         | Plugin versioning                                                                           |
| ------------------------ | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Published API 3 packages | Minimum `v0.1.0+build.1`                                                    | Original combined compatibility/revision labels, preserved unchanged                        |
| Source API 4 EDA / NSP   | [Manifest-derived requirements](../plugins/versioning.md#declared-packages) | Independent Semantic Versions; supported host/target intervals are separate manifest fields |

Release builds supporting API 4 preserve installed API 2 and API 3 packages.
The declared desktop interval is a compatibility bound, not a scheduled release
version. Development builds use isolated development plugin packages. The
[manifest-derived declarations](../plugins/versioning.md#declared-packages)
show the current source's API 4 requirements; they do not claim those packages are
published. The official catalog establishes download availability and selects a
compatible package. Follow the [generation migration steps](../plugins/versioning.md#upgrade-from-the-original-packages)
when upgrading from the original release.

Equal bounds permit one exact product release. They are not a vendor-wide
support claim or an automatic promise for later patch releases. The host checks
desktop compatibility when selecting/installing/loading a package; each plugin
checks the target through its platform API before setup work.

[EDA Capture](../plugins/eda.md#compatibility-package) needs the matching EDA
cluster app for temporary capture. [NSP Capture](../plugins/nsp.md#compatibility-package)
needs the workflow service, permissions and mounted truststore layout described
in its guide. Version matching does not replace these prerequisites.
