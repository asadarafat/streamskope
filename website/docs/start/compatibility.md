# Check compatibility before connecting

The core capabilities below apply to the published desktop **v0.1.0+build.1**
(app **0.1.0**) and upcoming **0.2.0**. Plugin requirements differ by generation,
as described below. Implemented means
the application exposes the capability; it does not qualify every vendor/version.
See [qualification evidence](../guide/qualification.md) for what was actually exercised.

## Connection and data capabilities

| Capability           | Implemented scope                                                                | Boundary                                                                                                              |
| -------------------- | -------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Kafka transport      | TLS with server certificate validation; explicitly selected plaintext            | No mutual-TLS client certificate/key configuration                                                                    |
| Kafka authentication | OAuth 2.0 client-credentials token retrieval with SASL OAUTHBEARER, or no SASL   | SASL PLAIN, SCRAM and Kerberos are not configurable                                                                   |
| Broker trust         | PEM CA, JKS and PKCS12 trust material                                            | Truststores provide CA trust, not client identity                                                                     |
| Message inspection   | Keys/values interpreted as UTF-8; JSON formatting when valid                     | No automatic Avro, Protobuf or Schema Registry wire-format decoding; no byte-exact binary export                      |
| Message reads        | Tail, Newest N, First N and a recent two-minute Time window                      | Bounded retention; no arbitrary historical date picker; see [read semantics](../guide/messages.md#choose-a-read-mode) |
| Schema Registry      | Confluent-compatible HTTP API; Avro, JSON Schema and Protobuf schema definitions | Browsing/registration is separate from message decoding; auth is none or the profile's OAuth token, not HTTP Basic    |
| Redpanda transforms  | Redpanda Admin API and transform-log topic                                       | Not an Apache Kafka feature; independent service permissions and connectivity required                                |
| EDA capture          | EDA Capture, API 3 (published), API 4 (upcoming)                                 | Requires exactly EDA 26.8.2 and the separate matching cluster app                                                     |
| NSP capture          | NSP Capture, API 3 (published), API 4 (upcoming)                                 | Requires exactly NSP 26.4.0 and the workflow/mounted-trust layout                                                     |

TLS failures do not fall back to plaintext. HTTP service authentication and Kafka
broker authentication must each be accepted by their destination. A valid Kafka
OAuth token is not necessarily valid for the Registry or Redpanda Admin API.

## Test baselines and support boundaries

| Component                 | Repository baseline                 | Qualification boundary                                                                                                                    |
| ------------------------- | ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Apache Kafka              | AIO fixture pins 4.3.1              | Local fixture provides OAuth/TLS; first-release qualification is recorded separately                                                      |
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

| Package generation             | Desktop requirement      | Plugin versioning                                                                           |
| ------------------------------ | ------------------------ | ------------------------------------------------------------------------------------------- |
| Published API 3 packages       | Minimum `v0.1.0+build.1` | Original combined compatibility/revision labels, preserved unchanged                        |
| Upcoming API 4 EDA / NSP 0.1.0 | `>=0.2.0, <0.3.0`        | Independent Semantic Versions; supported host/target intervals are separate manifest fields |

StreamSkope 0.2.0 preserves installed API 2 and API 3 packages. The
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
