# Configure certificate trust

Choose trust for the endpoint the desktop host is contacting. Browser sign-in,
successful API authentication and a successful Kafka connection establish different
things. A truststore password unlocks certificates; it is not a Kafka login.

## Which trust settings apply?

| Connection                 | With a TLS Kafka profile                                                                                   | With a plaintext Kafka profile     |
| -------------------------- | ---------------------------------------------------------------------------------------------------------- | ---------------------------------- |
| Kafka broker               | Profile PEM CA bundle, or CA certificates decoded from its JKS/PKCS12 store; hostname verification enabled | No TLS or certificate verification |
| HTTPS OAuth token endpoint | The same profile CA bundle                                                                                 | Host's default HTTPS trust         |
| HTTPS Schema Registry      | The same profile CA bundle                                                                                 | Host's default HTTPS trust         |
| HTTPS Redpanda Admin API   | The same profile CA bundle                                                                                 | Host's default HTTPS trust         |
| EDA or NSP setup API       | Host's default HTTPS trust, independently of Kafka profile settings                                        | Same independent API trust         |

An explicit profile CA bundle replaces the default trust for the HTTPS requests
using it. A public-CA service can therefore fail alongside a private-CA broker if
the bundle contains only the broker's CA. There is currently no separate CA field
per OAuth, Registry or Admin endpoint. Authentication choices do not change this
trust selection. An `http://` service URL has no TLS protection; use HTTPS for
credential-bearing services.

## Supply a combined profile CA bundle

1. Ask the administrator for the approved issuing CA certificates for **every**
   broker, token endpoint, Registry and Admin endpoint used by this profile.
   Verify their provenance through your organization's normal certificate process.
2. Have the administrator provide one PEM file containing those CA certificate
   blocks, or one JKS/PKCS12 truststore containing the required certificate entries.
   Do not include client private keys; mutual TLS client identity is not configurable.
3. Edit the Kafka profile. Select **TLS**, choose **Trust material format**, then
   **Select trust material**. For JKS/PKCS12, supply its truststore password.
   Selecting a different format clears the draft's previously selected material.
4. Run **Test connection**, save, and connect. Then open **Schema Registry** or
   the relevant Admin workspace and perform a read there. The Kafka test does not
   establish that these optional services work.
5. Confirm that each hostname matches its certificate, including broker addresses
   returned by Kafka metadata. Adding a CA does not fix a name mismatch.

**Expected result:** Kafka connects and each configured HTTPS service completes
its own authorized read. If only one service fails, identify that endpoint's
certificate chain and permissions before changing the profile again.

This procedure follows the profile trust wiring in the host. The mixed-CA behavior
is covered by HTTPS adapter tests; it is not a claim that every native OS trust
configuration has been rehearsed. See [qualification boundaries](qualification.md).

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
