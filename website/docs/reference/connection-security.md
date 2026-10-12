# Authentication and certificate trust

The [connection procedure](../guide/connections.md) owns profile creation and
[certificate setup](../guide/tls-trust.md) owns the trust and client-identity steps.

## Broker authentication

| Setting                             | Required material                                                                                 |
| ----------------------------------- | ------------------------------------------------------------------------------------------------- |
| No SASL                             | No SASL credentials; the broker may still require mutual TLS                                      |
| OAuth 2.0 / OAUTHBEARER             | Token endpoint, client ID, client secret and optional scope                                       |
| SASL PLAIN                          | Username and password                                                                             |
| SASL SCRAM-SHA-256 or SCRAM-SHA-512 | Username and password for the selected mechanism                                                  |
| Broker mutual TLS                   | PEM client certificate chain, matching PEM private key and encrypted-key passphrase when required |

Mutual TLS is a transport identity and can be combined with SASL. Explicit
plaintext transport has no server-certificate verification and cannot use a TLS
client identity. It also leaves traffic and credentials unencrypted in transit.
A failed TLS connection never falls back to plaintext.

## Trust ownership

| Connection                            | Certificate trust                                                                          | Client identity                                                               |
| ------------------------------------- | ------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------- |
| TLS Kafka broker                      | Profile PEM CA bundle, or CAs decoded from JKS/PKCS12; hostname verification stays enabled | Optional broker PEM certificate/private key and encrypted-key passphrase      |
| Plaintext Kafka broker                | No TLS or certificate verification                                                         | Not available                                                                 |
| Broker OAuth token endpoint           | Broker trust bundle; host default HTTPS trust for a plaintext broker profile               | Does not borrow the Kafka client identity                                     |
| HTTPS Schema Registry or Connect      | Each service selects system authorities, broker trust or a separate CA/truststore          | Optional independent service identity; never implicitly copies the broker key |
| Separate service OAuth token endpoint | That service's selected trust                                                              | That service's selected client identity                                       |
| HTTPS Prometheus endpoint             | Independent metrics system authorities, broker trust or separate CA/truststore             | Optional metrics PEM client identity, separate from the broker                |
| HTTPS Redpanda Admin API              | Existing profiles inherit broker trust                                                     | Existing behavior is preserved                                                |
| EDA or NSP setup API                  | Host default HTTPS trust, independent of Kafka profile settings                            | Controlled by the plugin, not the broker profile                              |

**System certificate authorities** means the trust available to the StreamSkope
host's HTTPS runtime; a certificate accepted by your browser does not prove that
the host accepts it. An explicit CA bundle replaces that default trust for requests
using it. Keep hostname verification enabled. An `http://` URL has no TLS protection;
use HTTPS for credential-bearing services.

Existing service settings without an explicit trust choice preserve broker-trust
inheritance after migration. Choose each service's trust deliberately when editing
an existing profile; changing broker transport does not make a failed TLS request
fall back to plaintext.

## What a connection test establishes

**Test connection** checks the broker and configured Schema Registry/Connect
services. It does not read the optional Prometheus endpoint or establish permission
for every write operation. Prometheus requires an explicit **Read metrics** after
connection. Success does not establish universal managed-service compatibility;
use the [recorded support boundaries](../start/compatibility.md).
