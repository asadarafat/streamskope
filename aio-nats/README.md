# StreamSkope secure NATS fixture

This persistent development lab is a sibling of `aio-kafka`. It runs one
digest-pinned NATS server with verified TLS and a generated token. The
pinned server definition also powers disposable real-provider and browser tests;
those tests create separate containers and never remove this lab.

This fixture is development infrastructure. It is excluded from desktop
packages. Keep it on loopback and do not reuse its credentials for production.

## Prerequisites

- Linux arm64 or amd64, including an OrbStack guest
- Node.js 24 and `npm ci`
- Docker, OpenSSL and Containerlab (verified with 0.79.0) with permission to manage labs

Run commands from the repository root. The persistent lab is deployed from
`topology.clab.yml` through Containerlab. Disposable qualification servers use
separate Docker containers. JetStream is not enabled. The fixture requires a
local Unix Docker endpoint; remote Docker contexts are rejected before deployment.

## Start and connect

```bash
npm run dev
```

The web-development launcher ensures both AIO labs are healthy and seeds an
empty, host-owned NATS profile store with a session-only **Local AIO NATS**
profile. Open **Connection Profiles**, connect its NATS row, open
**Live Subscription** and subscribe to `streamskope.fixture.>`.

The NATS lab can also run without the workbench:

```bash
npm run dev -- nats start
npm run dev -- nats status
```

The default server is `nats://127.0.0.1:14222` with token authentication and TLS
certificate verification. Commands print the server, subject and private CA/token
file paths; they never print token values or signing keys. For an installed
desktop app, create a NATS profile using that endpoint, token and CA certificate
on the same Linux host. An app on another host needs an explicitly configured
secure tunnel: the fixture is intentionally not exposed beyond loopback.

Repeat `start` to reuse the verified owned server and certificates. It resumes
stopped containers without replacing their container, network or private volume IDs
or rotating credentials. An existing legacy Docker fixture remains usable;
explicitly stop it and start again to migrate to Containerlab. Generated
certificates last one year. Change the port in `fixture.config.json` only after
stopping the owned fixture; an occupied port or foreign container is never
automatically removed.

## Publish live samples

Start your subscription first, then run in another terminal:

```bash
npm run dev -- nats publish
```

This publishes two messages per second for 60 seconds to
`streamskope.fixture.events`. Each message carries a JSON payload, sequence,
timestamp, content type, fixture identifier and correlation ID. The canonical
subject and payload live in `fixture.config.json`.

For a longer or faster bounded sample:

```bash
npm run dev -- nats publish --seconds 120 --rate 10
```

The maximum is 600 seconds at 100 messages per second. Publication stops after
the requested count; the lab has no background producer. Core NATS does not
retain these messages. A subscription started after publication sees only
subsequent messages; there is no topic inventory, retained seed or history.

## Resource and readiness behavior

The server is capped at 128 MiB, half a CPU and 64 processes. Its root filesystem
is read-only, privileges are bounded and debug/trace logging is disabled. TLS
keys and configuration enter only its owned private volume; the CA signing
key stays outside the container. Secrets are generated inside an ignored private
directory, with directory mode `0700` and file mode `0600`.

Startup verifies TLS and token authentication, then independently subscribes,
publishes and reads back an exact readiness value before saving ownership
evidence. Readiness uses a unique subject and opens no lasting subscription.
Each SDK PONG/message wait has a deadline and closes its owned client on failure;
a stalled server cannot keep a lifecycle command waiting indefinitely.
`status` repeats that round trip; it does not rotate credentials or change the
sample configuration.

## Stop and recover

```bash
npm run dev -- nats stop
```

Stop verifies the recorded Docker daemon identity, full resource IDs, names,
pinned image and UUID labels. Switching to a different Docker daemon blocks
resource reconciliation and private-file cleanup. Legacy Docker-only ownership
records must be used with their original Docker context until migrated.
It uses Containerlab to remove its server, then independently checks its private
volume and management network are absent. A volume used by another container
or a foreign endpoint on that network blocks cleanup. Private certificates and
ownership files are erased only after resource cleanup is confirmed. Repeat stop
is harmless. Existing legacy Docker fixtures retain their original owned-container
cleanup path until explicitly migrated.

If startup fails, check Docker/OpenSSL, the configured port and the private
ownership directory. Do not delete a conflicting resource that belongs to
another project. Interrupted startup has a private UUID intent written before
deployment; retry compensates only matching owned resources. If stop
already removed the container, retry checks remaining volume and network resources
before finishing private file cleanup. A failed daemon
query or foreign name is never treated as successful cleanup. Changed configuration
requires explicit stop; the launcher refuses to claim an unverified resource.
Expired certificates can be renewed by stopping a verified
owned fixture and starting it again.

If a Docker resource-create call fails or times out without returning a full ID,
its daemon-side completion may still be pending. The private intent and files
remain available; a single empty resource listing cannot establish cleanup.
Normal cancellation seals the private deployment adapter and waits for begun
mutations to settle before recording completion. After forced termination, retry
`stop` only when the recorded operation is confirmed to have quiesced. If its
completion is unknown,
establish that the original daemon request has quiesced and independently verify
that all matching owned resources are absent before manually clearing that intent
and its preparation directory. Preserve the evidence when that
cannot be confirmed, and account for other labs before performing Docker recovery.
The launcher never starts a second competing creation request in this state.

Normal cancellation waits for bounded operations and owned cleanup before exiting.
Lifecycle commands serialize through `ownership/.operation.lock`. If a process
was forcibly terminated and the lock remains, verify that no fixture command
is still running before removing that lock directory and retrying. Do not
remove the ownership record of a running fixture: it is required to prove
safe cleanup.

## Qualification

```bash
npx --no-install vitest run --config config/vitest.config.ts \
  test/nats/provider-real.test.ts
```

The four real tests use isolated instances of this shared pinned definition.
They independently verify token/TLS message fidelity, wrong credentials and
CA rejection, IP/DNS certificate matching, permission denial and confirmed
stream stop/reconnect. They preserve the persistent AIO lab and its credentials.

`npm run check` adds the repository's 60-second mixed-message pipeline soak and
configured live EDA/NSP checks. That pipeline soak is distinct from a real NATS
server round trip; record the actual real-provider evidence separately when
qualifying a release.

For an explicit real-server performance qualification, start this lab first and
run:

```bash
npm run dev -- nats start
node --import tsx test/performance/nats-live-soak.ts
```

This standalone soak verifies the existing ownership/readiness, then offers
60,000 records at 1,000 per second over 60 seconds through an independent SDK
publisher and the production provider endpoint. It checks JSON/headers, exact
record accounting, omissions, credential isolation, confirmed repeat stop,
post-stop silence, repeat disconnect and owned client cleanup. It never creates,
stops or removes the persistent server.

The structured result is `dist/performance/nats-live-soak.json`, using the
repository's existing CPU, sampled RSS, event-loop and minimum throughput
budgets. A 65-second load deadline and overall cancellation guard prevent a
stalled run from qualifying an arbitrarily slow workload. The offered-rate ratio
uses the actual measured load duration, so a late completion cannot hide a
throughput shortfall. Queue maxima describe publicly observed counters;
internal queue high-water marks remain unmeasured.

CPU, event-loop and RSS measurements cover this Node process during the load.
Server resources, renderer paint, native IPC and UI interaction latency are
not measured. This real NATS soak and the Kafka pipeline/clone replay are
separate measurements. The command is on demand; `npm run check` retains one
automatic 60-second pipeline soak.

## Generated files

All content under `ownership/` is private and Git-ignored. Do not commit token
files, server configuration, certificates, signing keys, ownership records or
generated topology, Containerlab runtime output or Docker runtime output.
The source topology contains placeholders, not credentials. Each deployment uses
a unique UUID lab and a daemon-assigned subnet; its full UUID labels and resource
IDs establish ownership. Published product documentation belongs in `website/docs`.
