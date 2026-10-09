# Connect your Kafka

Save a connection profile, test it, and open your cluster's topics.
A profile keeps the settings for one Kafka connection together.
**Connection Profiles** lists saved Kafka and NATS connections together. Check
the **System** column before connecting. Selecting, editing or saving a profile
does not replace the active connection; its **Connect** action does.

Trying StreamSkope for the first time? Follow the [desktop quickstart](../start/quickstart.md).
For a disposable broker in a development checkout, use the [development sandbox](../start/development.md).

Check the [compatibility matrix](../start/compatibility.md) before requesting credentials.

## Before you start

Have your bootstrap broker addresses and authentication settings ready. TLS is
the default and requires PEM, JKS or PKCS12 trust material. Explicit plaintext
connections require no broker trust but leave broker metadata, messages and
Kafka credentials unencrypted. Broker addresses must be reachable from
the machine running the StreamSkope host. Review the [workflow permissions](security.md#kafka-access-and-effects)
with your administrator before requesting access.

Schema Registry and Connect can each use separate credentials, certificate trust
and a TLS client identity. Existing profiles continue to inherit broker trust unless
you explicitly select another trust mode. The broker OAuth endpoint uses the broker
trust settings. Follow [Configure certificate trust](tls-trust.md) for different issuing CAs.

## Configure manually

1. Open **Connection Profiles**, then **Add connection → Kafka broker**.
2. Enter a **Profile name** you will recognize and the **Bootstrap brokers**.
3. Keep the default **TLS** broker transport and select the matching certificate
   or truststore, or deliberately select **Plaintext (insecure)** for an isolated
   unsecured environment. Plaintext never results from missing trust or a failed
   TLS attempt.
4. Choose **Broker authentication**: no SASL, **OAuth 2.0 (OAUTHBEARER)**,
   **SASL PLAIN**, **SASL SCRAM-SHA-256** or **SASL SCRAM-SHA-512**.
   OAuth needs its token endpoint, client ID, client secret and optional scope.
   PLAIN/SCRAM need **SASL username** and **SASL password**.
5. If the broker requires a client certificate, enable **Broker mutual TLS** and
   select its PEM client certificate and private key files. Supply **Broker private
   key passphrase** when the key is encrypted. Client identity is separate from
   the CA certificates used to verify the server; it can be combined with SASL.
6. If needed, enter Schema Registry or Connect endpoints. Choose each service's
   authentication and certificate trust independently. See [Schema Registry](schema-registry.md)
   and [Kafka Connect](kafka-connect.md).
7. Click **Test connection**. Wait for the broker and configured service checks.
   If a stage fails, open **Raw logs** and correct that endpoint's settings.
8. Click **Save profile**. Find the saved Kafka profile and select **Connect**.
9. Wait for **Connected**, then open **Topics**.

**You should see:** the topics your account can access. Testing checks the supplied
settings; you still need to save and connect to use them.
Only one connection is active. Connecting a profile for another system stops the
current stream and disconnects its host before connecting the selected profile.
If cleanup fails, follow the recovery message for the original connection.

<figure class="product-shot">
  <img data-sk-light="profiles.png" data-sk-dark="profiles-dark.png" alt="Connection profiles in StreamSkope" width="2880" height="1800" loading="lazy" decoding="async">
  <noscript><img src="../assets/profiles.png" alt="Connection profiles in StreamSkope" width="2880" height="1800" loading="lazy"></noscript>
</figure>

## Read messages from EDA

Install **EDA Connector** through **Preferences → Plugins**, then open
**Add connection → Connect via EDA**. Follow [Connect via EDA](../plugins/eda.md) for
cluster prerequisites, existing Kafka access, temporary captures and recovery.
Installation, updates and removal apply without restarting the desktop.

### Connect to existing Kafka

The [EDA guide](../plugins/eda.md#use-existing-kafka) explains discovery and the separate
Kafka credentials/network access required for an existing destination.

### Start a temporary capture

Review [administrator prerequisites](../plugins/eda.md#administrator-prerequisites), then
follow [Start temporary capture](../plugins/eda.md#start-temporary-capture).

### Stop, remove and recover

Use the [EDA lifecycle table](../plugins/eda.md#stop-update-and-resume) and
[cleanup verification](../plugins/eda.md#administrator-diagnosis-and-cleanup-verification).
A saved profile does not guarantee that its temporary capture still exists.

## Connect through NSP

Install NSP Connector, then use your NSP API URL and credentials to create a tested
Kafka profile. Follow [Connect via NSP](../plugins/nsp.md) for the workflow,
permissions and cleanup behavior. Check each plugin's [desktop and target requirements](../plugins/versioning.md); the original
v0.1.0+build.1 packages and development API 4 packages have different host requirements.

## Next: inspect a message

[Open a topic and read a record →](messages.md)

If you cannot connect, follow [Fix a connection problem](troubleshooting.md).
For Schema Registry, [configure and check its separate service connection](schema-registry.md).

## Automate repeated setup

Use a [Secret Retrieval Profile](secret-retrieval.md) to retrieve trust material
and suggest OAuth settings, then test and save the connection as above.

## Update or recover

1. Open the saved profile's editor and change the settings you need.
2. Test the edited settings before updating the profile.
3. Reconnect and confirm that the expected topics are available.

Editing non-secret settings retains protected values unless you replace or clear
them. Changing authentication methods removes the previous method's credentials
from the updated profile. Desktop secrets use the operating-system credential
service; the production browser host uses its unlocked encrypted vault, while
browser development profiles are session-only.

Existing OAuth profiles and EDA/NSP-generated profiles keep their authentication,
service-token inheritance and resource identity during migration. New credential
fields are stored inside the protected profile content. Before using an older app
against upgraded data, follow the backup and recovery procedure below; do not
manually remove credential fields or plugin ownership metadata.

Before replacing or downgrading the app, follow [Upgrade, back up and recover](recovery.md).
That page owns storage locations, migration-specific snapshots and restoration.
See [Data and exports](data-handling.md) for files outside the protected profile store.
