# StreamSkope secure Kafka fixture

This Containerlab fixture provides a single Kafka broker with TLS and
OAuthBearer authentication, a small OAuth token/JWKS service, and a
Confluent-compatible Karapace Schema Registry. It is development and validation
infrastructure; it is not included in StreamSkope desktop packages.

Credentials in `fixture.config.json` are public, development-only test values.
Never reuse them for production or expose this fixture to untrusted networks.

## One fixture owner, isolated instances

`aio-kafka` owns the interactive lab, real Kafka test fixtures, service variants
and qualification image. Tests keep separate instances so permission changes,
offset resets and broker failures cannot alter the development lab.

| Variant | Purpose | Runtime |
| --- | --- | --- |
| Development lab | TLS/OAuth, seed message and authenticated Registry | Persistent Containerlab lab; explicit CLI cleanup |
| Secure broker / authentication matrix | Record workflows, vault restart, PLAIN, SCRAM, OAuth and mutual TLS | Disposable Docker image built automatically from the lab's pinned Kafka image |
| Authorization broker | ACLs, groups, quotas and topic administration | Disposable Docker instance of the same pinned Kafka image |
| Replication | Three brokers, broker failure and replica health | Three disposable Docker instances |
| Connect / Registry / metrics | Real service behavior and external metrics | Disposable service containers, alongside their owned broker |
| Native installer recovery | Actual installer checks on Linux, macOS and Windows | Every client attaches to the same configured remote AIO TLS/OAuth broker |

Implementations live in `fixtures/`. `pins.ts` reads the Kafka version and digest
from `topology.clab.yml`; Connect uses that same version. The reference-capable
Registry and metrics pins also live there. The authenticated development Registry
keeps its qualified variant in `fixture.config.json`.

The secure test image lives in `images/qualification-broker/`. Its small build
adds the storage-format/start entrypoint to the pinned Kafka base; it downloads no
separate JDK or Kafka archive. It is built and cached locally or on the CI runner,
with the immutable image ID recorded in fixture metadata. `keytool` and OpenSSL
run inside the image. There is no separate GHCR publication or release asset for
this test-only image.

Disposable containers receive an instance ownership label and a fixture-system
label. Cleanup checks the immutable container ID and owner before removal,
confirms removal and supports retries. A creation timeout is resolved through its
owner label; a conflicting foreign name is never removed. Tests use random
loopback ports and bounded CPU/memory settings. The Docker backend currently
requires a Linux Docker host, including a Linux VM. Native installer recovery
uses an explicitly configured remote AIO connection on every operating system;
it does not download Kafka or start a host JVM.

Run the complete real-provider/browser lane from the repository root:

```sh
npm run check -- --ci --lane runtime
```

Focused test files can still be run with Vitest. Tests that start disposable
fixture handles clean up their own instances and need no running development
lab. The development-lab verification below and tests that use
`loadFixtureConnection()` require a running lab or an explicit external fixture.

## Prerequisites

These are prerequisites for the Linux fixture host. A remote native-test client
needs Node.js and its OS credential service, plus access to the AIO endpoints and
a local copy of the public CA certificate; it needs no Docker, Containerlab or JDK.

- Node.js 24.21.0 or newer in the 24.x line
- Docker
- Containerlab
- `keytool` from a JDK or JRE

Run commands from the StreamSkope repository root.

## Start the web workbench and fixture together

```bash
npm run dev
```

In an OrbStack guest this derives `http://<current-vm>.orb.local:5173/`; elsewhere
it defaults to `http://127.0.0.1:5173/`. It starts an absent owned fixture,
resumes its stopped containers, checks Kafka/OAuth/Schema Registry readiness,
and offers the session-only Local AIO Kafka profile. Running containers, data
and certificates are retained; Kafka endpoints stay on loopback. Foreign
containers or conflicting fixture settings require explicit recovery, not
automatic deletion.

Repeat the command to reuse the server. Changing `STREAMSKOPE_DEV_PUBLIC_HOST`
gracefully replaces a verified launcher from this checkout on Linux/macOS;
unrelated port owners are never killed. Explicit loopback web access remains available:

```bash
STREAMSKOPE_DEV_PUBLIC_HOST=127.0.0.1 npm run dev
```

The OrbStack web hostname listens beyond loopback: use it only on a trusted
development network. Gateway authentication and origin checks remain enabled.

## Start an owned fixture

```bash
node --import tsx tools/dev/kafka-fixture/cli.ts start \
  --name streamskope-kafka \
  --kafka-port 19093 \
  --oauth-port 15000 \
  --schema-registry-port 18081
```

Startup:

1. rejects occupied host ports;
2. builds the digest-pinned OAuth image and downloads the digest-pinned
   Karapace image when it is not already cached;
3. creates instance-specific ignored TLS material;
4. deploys an instance-specific Containerlab lab;
5. verifies OAuth token retrieval, Kafka TLS/OAuth metadata, and that the Schema
   Registry rejects missing and malformed bearer tokens before accepting the
   fixture token;
6. creates `test` and produces one deterministic seed record;
7. registers and reads back the deterministic `test-value` Avro schema; and
8. writes a private ownership record only after every readiness check succeeds.

The command prints the broker endpoint, token endpoint, Schema Registry
endpoint, and PEM CA path. Demo OAuth values, seed payload, and seed schema have
one canonical owner:
`fixture.config.json`.

On the next browser-development launch, an empty profile store is seeded with a
`Local AIO Kafka` profile that includes the owned Registry endpoint. Existing
user profiles are never overwritten. These host-owned service endpoints use
explicit IPv4 loopback (`127.0.0.1`); they are independent of the renderer's
public browser hostname. The Registry reuses the profile OAuth token owner and
does not introduce a separate client secret.

## Laptop resource behavior

Container health reads the kernel's local listening-socket table at bounded
intervals. It opens no client connection and produces no application log
traffic. Complete OAuth, TLS, Kafka, seed-message and Schema Registry
verification still runs once through the fixture CLI's `start` command before
ownership evidence is published. The single-node broker uses a 256 MiB initial and 512 MiB maximum
heap, and the topology does not run a continuous producer.

The fixture remains active until explicitly stopped. At the end of a work
session, [stop the owned fixture](#stop-an-owned-fixture) to free its resources.

## Verify the real fixture

```bash
STREAMSKOPE_TEST_FIXTURE_NAME=streamskope-kafka \
  npx --no-install vitest run --config config/vitest.config.ts test/kafka/fixture-real.test.ts
```

The test independently retrieves a token, lists `test`, joins a new consumer
group, and consumes the exact seed payload.

The live browser acceptance test also inspects `test-value`, checks
compatibility, registers a temporary schema, reads it back, and permanently
deletes it through StreamSkope:

```bash
node tools/package/e2e.mjs web \
  test/e2e/web-real-connection.spec.ts \
  --grep "live Registry"
```

## Stop an owned fixture

```bash
node --import tsx tools/dev/kafka-fixture/cli.ts stop --name streamskope-kafka
```

Cleanup requires the matching ownership record. It removes only that lab,
record, Containerlab directory, and generated certificate directory. It will
not destroy a name that StreamSkope does not own.

## Share one AIO host across operating systems

Run the broker on one trusted Linux AIO host. Linux, macOS and Windows native
recovery clients use the same connection settings:

| Setting | Client value |
| --- | --- |
| `STREAMSKOPE_TEST_KAFKA_ENDPOINT` | Reachable Kafka bootstrap host and port |
| `STREAMSKOPE_TEST_OAUTH_ENDPOINT` | Reachable OAuth token URL |
| `STREAMSKOPE_TEST_CA_PATH` | Local path to a copy of the host's public `ca.pem` |

The default topology advertises `localhost` and its configured external Kafka
port. For remote clients, use an SSH tunnel that preserves that port, for example
with the default lab ports:

```sh
ssh -N -L 19093:127.0.0.1:19093 -L 15000:127.0.0.1:15000 USER@AIO_HOST
```

Keep the tunnel open while testing. Set the Kafka endpoint to `127.0.0.1:19093`
and the token URL to
`http://127.0.0.1:15000/rest-gateway/rest/api/v1/auth/token`. Copy only the public
CA certificate to the client. With direct network access, the broker's advertised
endpoint must also be reachable from the client and covered by its certificate.
Keep this fixture on a trusted test network; do not expose its demo credentials
or services to the public internet.

Native recovery preflight verifies TLS/OAuth metadata before downloading or
replacing an installer. Missing or unreachable settings fail the rehearsal;
they do not select another broker implementation. Each rehearsal creates a
unique topic and removes that topic during teardown. Clients never stop or
destroy the shared lab.

The manual GitHub recovery workflow takes its Kafka and OAuth endpoints from the
repository variables with the names above. Set `STREAMSKOPE_TEST_CA_PEM` to the
public PEM certificate; the workflow writes the local CA file on each runner.
Every runner needs a route to these endpoints. This workflow does not establish
an SSH/VPN connection or make a private Orb lab reachable automatically. A failed
preflight is a failed check, and an unexecuted native rehearsal is not a pass.

## Verify an existing external fixture

```bash
node --import tsx tools/dev/kafka-fixture/cli.ts attach \
  --kafka 127.0.0.1:9093 \
  --oauth http://127.0.0.1:5000/rest-gateway/rest/api/v1/auth/token \
  --ca /absolute/path/to/ca.pem
```

Attach mode performs OAuth and Kafka readiness without creating an ownership
record, producing a seed, changing the external lab, or stopping it.

## Generated files

Generated files are ignored:

- `ownership/<name>/certs/`
- `ownership/records/`
- `clab-*/`
- Containerlab annotations

Do not commit certificates, JKS stores, ownership records, or Containerlab
runtime output.
