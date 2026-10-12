# Configure certificate trust

Choose trust for the endpoint the StreamSkope host is contacting. Browser sign-in,
successful API authentication and a successful Kafka connection establish different
things. A truststore password unlocks certificates; it is not a Kafka login.

## Which trust settings apply?

Use the [endpoint trust matrix](../reference/connection-security.md#trust-ownership)
to identify which CA bundle and client identity apply to each request. Configure
broker and service trust separately when their issuers differ. Keep hostname
verification enabled and use HTTPS for credential-bearing services.

## Supply a combined profile CA bundle

1. Ask the administrator for the approved issuing CA certificates for the brokers
   and broker OAuth endpoint. Verify provenance through your organization's normal
   certificate process.
2. Supply a PEM file containing those CA certificate blocks, or a JKS/PKCS12
   truststore containing the required certificate entries. Keep private keys out
   of this trust bundle.
3. Edit the Kafka profile. Select **TLS**, choose **Trust material format**, then
   **Select trust material**. For JKS/PKCS12, supply its truststore password.
4. Under each configured service, choose its **certificate trust**. Use system
   authorities for a trusted public-CA endpoint, **Reuse broker certificate trust**
   if the same bundle is appropriate, or **Separate CA or truststore** for its own CAs.
   A separate OAuth token endpoint uses that service bundle too; include both
   issuing CAs when they differ.
5. Test the profile. Broker, token and configured service checks report their own
   failures. Save, reconnect and perform the intended operation in each workspace;
   a successful inventory read does not establish write permission.

The optional Prometheus endpoint has its own authentication, trust and client
identity, including its separate OAuth token request. **Test connection** checks
Kafka and configured Registry/Connect services; it does not read Prometheus.
Connect Kafka and choose **Read metrics** in [Observed health](observed-health.md#read-optional-prometheus-metrics)
to test that endpoint explicitly.

Adding a CA does not fix a hostname mismatch. Check every broker address returned
by Kafka metadata, not just the bootstrap address.

## Supply a mutual TLS identity

Enable **Broker mutual TLS** when Kafka requires a client certificate. Upload the
PEM certificate chain and corresponding PEM private key separately. Supply the
private-key passphrase if encrypted. The issuer must be accepted by the server,
the identity must be current, and the certificate and key must match.

Mutual TLS verifies the client independently of SASL. Choose no SASL for a
certificate-only listener, or configure the required SASL mechanism as well when
the listener requires both. A broker truststore password is not the private-key
passphrase or SASL password.

Schema Registry and Connect have their own mutual TLS fields. Supplying a broker
identity does not authorize another service to receive that key. Service identities
apply only to that service and its separately configured OAuth token endpoint.

Certificate/key content, passphrases and service credentials stay in protected
profile storage. Profile lists expose presence indicators rather than returning
these values to the renderer. Replacing or clearing a saved identity is explicit.

The qualification matrix uses a real isolated Kafka broker and controlled HTTPS
endpoints. It does not certify every managed service, TLS terminator or native OS
credential service. See [qualification boundaries](qualification.md).

## EDA and NSP API certificates

The plugin dialogs expose a certificate-verification checkbox, **not a custom-CA
upload**. Their production API clients use the host's default HTTPS trust. The
Kafka profile's trust material is not passed to these API clients. Importing a CA
into a browser does not demonstrate that the desktop API client trusts it.

For an API signed by a private CA that the desktop does not trust, there is no
qualified custom-CA installation procedure in the current plugin UI. Ask the
administrator to expose an approved API endpoint whose certificate the desktop
can verify. Do not assume that a system trust-store change will work across all
packaged platforms. A per-plugin CA setting would require a product change.

For a trusted development lab only, the dialog allows explicitly disabling API
certificate verification. That setting applies to the EDA API/tunnel or NSP API
operation. It does not disable verification on an NSP Kafka profile or its saved
OAuth endpoint. Keep verification enabled for production connections.

NSP retrieves Kafka CA material **after** API authentication. It cannot use that
future result to establish trust in the initial NSP API connection. Refreshing
NSP credentials replaces retrieved material; arrange certificate changes with the
NSP administrator rather than relying on manual profile edits surviving refresh.

[Diagnose a TLS failure →](troubleshooting.md#tls-trust-or-hostname-failure)
