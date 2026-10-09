# Develop StreamSkope

Run commands from the repository root. Start with the task you need; a prose edit
does not require a broker, native desktop toolchain or access to EDA and NSP.

## Prepare a checkout

Use Node **24.21.0 or newer in the 24.x line**, then install the committed graph:

```sh
npm ci
```

Install dependencies on the operating system and CPU that will run them. Native
modules copied from another host are not a qualified installation. The development
launcher can prepare a separate native dependency cache when its health probe
fails; it does not rewrite a shared `node_modules` installation. See
[dependency maintenance](dependencies.md) before updating a lockfile or mitigation.

| Task                                         | Additional prerequisites                                                                                                                               |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Browser workbench with both local fixtures   | Linux AMD64 or ARM64, including a Linux VM; local Unix Docker endpoint, Containerlab, OpenSSL, Java `keytool`, and permission to manage the owned labs |
| Focused unit/architecture tests              | Root npm installation; particular integration tests may require their own fixture                                                                      |
| Production connection profile runtime matrix | Java 17+ with `keytool`, OpenSSL and network/cache access to the pinned Kafka archive; uses disposable loopback endpoints                              |
| Documentation                                | Python 3.11+ with `venv`/pip; browser qualification also needs Playwright browsers and their OS libraries                                              |
| EDA agent source work                        | Go toolchain matching [go.mod](../vendors/streamskope/apps/capture/agent/go.mod)                                                                       |
| Desktop installer                            | A supported native packaging host; see [Build a desktop package](#build-a-desktop-package)                                                             |
| Browser container                            | Linux AMD64 or ARM64 with Docker; Containerlab for deploying it                                                                                        |

Keep live-system credentials in private files outside tracked source. The
[qualification runbook](qualification.md) owns live-check configuration, writes,
cleanup and evidence limits.

## Run the workbench with Kafka and NATS

```sh
npm run dev
```

The launcher verifies or starts **both** owned AIO labs and seeds fresh,
host-owned profile stores with session-only **Local AIO Kafka** and
**Local AIO NATS** profiles. Kafka includes TLS/OAuth, a Schema Registry and a
known seed record. NATS uses verified TLS and a generated token; it has no
continuous sample producer or retained history.

Open the exact browser URL printed by the launcher. Inside OrbStack it derives
the current guest's `.orb.local` hostname; elsewhere it defaults to loopback.
The browser host and renderer normally use ports 4319 and 5173. For explicit
loopback development access:

```sh
STREAMSKOPE_DEV_PUBLIC_HOST=127.0.0.1 npm run dev
```

The named VM endpoint listens beyond loopback and belongs on a trusted development
network. Authentication and origin validation still apply. A remote browser must
reach the configured host; changing only the browser's URL does not change the
server's accepted origin. The launcher never kills an unrelated port owner.

1. Open **Connection Profiles** and connect **Local AIO Kafka**. Open topic `test`
   and inspect its known record. Stop the tail when finished.
2. Connect **Local AIO NATS**, open **Live Subscription**, and subscribe to
   `streamskope.fixture.>`.
3. In another terminal, publish a bounded sample:

   ```sh
   npm run dev -- nats publish --seconds 5 --rate 2
   ```

4. Observe the NATS messages, then stop the subscription and disconnect.

NATS subscriptions receive messages published after they start. These are external
fixture servers inspected by StreamSkope; the application does not use NATS as its
internal messaging bus. Connecting a profile selects the provider workflow in the
shared product frame.

Ctrl-C in the owning development terminal stops its application host. A repeated
launcher can reuse an existing session, and stopping that short-lived invocation
does not stop another session owner. The persistent labs remain available until
explicitly stopped. Follow the owned cleanup procedures in the
[Kafka fixture](../aio-kafka/README.md#stop-an-owned-fixture) and
[NATS fixture](../aio-nats/README.md#stop-and-recover) guides. Preserve their
ownership records and recovery state; do not delete a foreign container or clear
an intent to bypass unconfirmed cleanup.

For custom fixture ports, independent fixture commands, external attachment and
readiness details, use those fixture guides. The launcher implementation is
[tools/dev/start.ts](../tools/dev/start.ts); its process/session ownership is in
[tools/dev/session.ts](../tools/dev/session.ts).

## Make a focused change

Use the [architecture guide](architecture.md) to locate the feature owner, then
run its tests directly:

```sh
npx --no-install vitest run --config config/vitest.config.ts test/unit/acl-review.test.ts
```

Always specify the maintained Vitest config. Add `--maxWorkers=1` when debugging.
Import the module under test directly, keep edge cases at their owning layer,
and use the shared shell tests for navigation rather than duplicating whole UI
flows. Test-only imports do not establish production reachability.

For Electron or browser system tests, use the maintained runner with the required
runtime installed:

```sh
node tools/package/e2e.mjs web test/e2e/web-plugin-installation.spec.ts
```

This example exercises the browser plugin installation surface. It does not prove
live platform behavior or installed desktop recovery. The [qualification
matrix](qualification.md) determines the additional checks required by a change;
focused tests do not replace the normal PR gate. Every PR requires shared, docs,
runtime and final **CI** checks, but need not duplicate that complete run locally.

Choose a broader local scope when needed:

```sh
npm run check
npm run check -- --full
npm run check -- --live eda
```

The default runs shared checks, the 60-second soak and docs without vendor tests.
`--full` adds configured EDA/NSP checks and records unconfigured skips. Explicit
`--live eda`, `--live nsp` or `--live all` runs only the requested vendors and
fails if any selected configuration is missing. Run live checks for affected
integration, authentication, trust, generated-profile or connection/lock/cleanup
behavior and for milestone/release integration claims. A prose edit or unrelated
core change does not require vendor access. Use `npm run check -- --ci` to
reproduce GitHub's three lanes locally; its scope is unchanged.

Each local scope produces its own receipt. Preserve failed receipts when retrying
and never relabel earlier evidence as a new source or a broader scope. See
[reading a local result](qualification.md#read-a-local-result) and
[release attachment](qualification.md#attach-local-acceptance-to-a-draft).

The source CLI and owned consumer sandbox use the existing `dev` entry:

```sh
npm run dev -- cli --help
npm run dev -- sandbox help
```

Follow [CLI protection](../website/docs/guide/read-only-cli.md) and the
[sandbox lifecycle](../website/docs/guide/developer-sandbox.md) for their commands,
fixture ownership and limits.

## Build source and test a development plugin

```sh
npm run build
npm run package -- plugin
```

The default build checks all four TypeScript projects and creates the renderer,
Electron host/preload, worker bundles and separate plugin bundles. Plugin
packaging creates EDA and NSP development artifacts under `dist/plugin-package/`.
Use `npm run package -- plugin eda` or `npm run package -- plugin nsp` to select
one; each packaging invocation clears the previous output for that directory.

Return to `npm run dev`, open **Preferences → Plugins**, refresh the catalog,
and install or update the development package. The development catalog reads
`dist/plugin-package/`; startup does not build packages or download published
versions. Repackage after changing plugin source, then refresh and update again.
The browser host retains installed plugin state in `.cache/development-plugins`.

A compatible development package applies through the real hot lifecycle. Source
edits alone do not replace the installed artifact. Preserve capture/recovery
state. Use a compatible previous build to finish owned work and explicitly remove
an incompatible installation; renaming a published archive or deleting the cache
is not a compatibility migration. Signed releases, desktop compatibility and the
separate EDA application are covered in [releases](releases.md) and
[plugin versioning](../website/docs/plugins/versioning.md).

## Build a desktop package

1. Use a checkout on macOS ARM64, Windows x64 or Linux x64 with the supported Node
   line, and run `npm ci` there.
2. Run:

   ```sh
   npm run package
   ```

3. Wait for package verification and the packaged launch test.
4. Inspect the printed artifact under `dist/installers/`: DMG on macOS, NSIS
   `Setup.exe` on Windows or AppImage on Linux.

A successful local package check covers that native build. It does not establish
Apple signing/notarization, other platforms or live EDA/NSP support. Follow
[release packaging](releases.md) for the native CI matrix and
[qualification](qualification.md) for installed upgrade/recovery acceptance.

## Build and deploy the browser host from source

Source deployment is separate from the [released browser
installer](../website/docs/start/containerlab.md). Use a non-root account on a
Linux AMD64 or ARM64 Docker host with Containerlab. Image builds need access to
the pinned base images and registries, or approved mirrors/caches.

```sh
ELECTRON_SKIP_BINARY_DOWNLOAD=1 npm ci --ignore-scripts
npm run package -- container
mkdir -m 700 streamskope-data
export STREAMSKOPE_UID="$(id -u)"
export STREAMSKOPE_GID="$(id -g)"
clab deploy -t streamskope.clab.yml
```

Use the host's approved Containerlab permissions; if sudo is needed, preserve
the numeric owner and configured bind/port/origin. The root topology selects the
locally built `streamskope:0.0.0-dev` image with pull policy `Never`. Its private
`streamskope-data` bind is outside the generated Containerlab directory. Keep it
separate from installer-managed `/var/lib/streamskope/browser`.

Open the configured URL, normally `http://127.0.0.1:8080`, and follow
[vault setup](../website/docs/start/containerlab.md#2-create-the-vault). The
[manual deployment guide](../website/docs/guide/browser-deployment.md) owns the
trusted-host setup-code procedure, matching-origin access, stop and recovery.
The production host uses an encrypted, browser-unlocked vault; the Vite development
workbench uses session-only profiles.

`npm run build -- web` builds the renderer, browser host, read-only data inspector
and workers without an image. `npm run package -- container --archive` also
writes a Docker save archive and its native evidence under
`dist/container-package/`. Source identity remains `0.0.0-dev`; local staged
receipts cannot stand in for anonymous public-registry delivery evidence. See
[browser compatibility qualification](qualification.md#browser-data-compatibility)
for upgrade/rollback acceptance and its limits. The release installer does not
adopt source deployments.

For documentation setup, serving and browser checks, follow
[maintain the documentation](documentation.md). Generated `dist/`, `.artifacts/`,
private fixture state and `openspec/` are not source changes to include in a PR.
