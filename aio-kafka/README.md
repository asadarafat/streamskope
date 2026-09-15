# StreamSkope secure Kafka fixture

This Containerlab fixture provides a single Kafka broker with TLS and
OAuthBearer authentication, a small OAuth token/JWKS service, and a
Confluent-compatible Karapace Schema Registry. It is development and validation
infrastructure; it is not included in StreamSkope desktop packages.

Credentials in `fixture.config.json` are public, development-only test values.
Never reuse them for production or expose this fixture to untrusted networks.

## Prerequisites

- Node.js 24
- Docker
- Containerlab
- `keytool` from a JDK or JRE

Run commands from the StreamSkope repository root.

## Start the web workbench and fixture together

```bash
npm run dev:web
```

This defaults to `http://clab.orb.local:5173/`. It starts an absent owned fixture,
resumes its stopped containers, checks Kafka/OAuth/Schema Registry readiness, and
offers the session-only Local AIO Kafka profile. Running containers, data and
certificates are retained; Kafka endpoints stay on loopback. Foreign containers
or conflicting fixture settings require explicit recovery, not automatic deletion.

Repeat the command to reuse the server. Changing `STREAMSKOPE_DEV_PUBLIC_HOST`
gracefully replaces a verified launcher from this checkout on Linux/macOS;
unrelated port owners are never killed. Explicit loopback web access remains available:

```bash
STREAMSKOPE_DEV_PUBLIC_HOST=127.0.0.1 npm run dev:web
```

The default web hostname listens beyond loopback: use it only on a trusted
development network. Gateway authentication and origin checks remain enabled.

## Start an owned fixture

```bash
npm run fixture:start -- \
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
verification still runs once through `fixture:start` before ownership evidence
is published. The single-node broker uses a 256 MiB initial and 512 MiB maximum
heap, and the topology does not run a continuous producer.

The fixture remains active until explicitly stopped. At the end of a work
session, [stop the owned fixture](#stop-an-owned-fixture) to free its resources.

## Verify the real fixture

```bash
STREAMSKOPE_TEST_FIXTURE_NAME=streamskope-kafka npm run test:kafka
```

The test independently retrieves a token, lists `test`, joins a new consumer
group, and consumes the exact seed payload.

The live browser acceptance test also inspects `test-value`, checks
compatibility, registers a temporary schema, reads it back, and permanently
deletes it through StreamSkope:

```bash
node tools/run-playwright-e2e.mjs web \
  test/e2e/web-real-connection.spec.ts \
  --grep "live Registry"
```

## Stop an owned fixture

```bash
npm run fixture:stop -- --name streamskope-kafka
```

Cleanup requires the matching ownership record. It removes only that lab,
record, Containerlab directory, and generated certificate directory. It will
not destroy a name that StreamSkope does not own.

## Verify an existing external fixture

```bash
npm run fixture:attach -- \
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
