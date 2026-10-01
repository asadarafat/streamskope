# Development and releases

Use Node 24, `npm ci`, Go 1.24 for the EDA agent, and Python 3.11+ for docs.
Install docs browsers once with `npx playwright install chromium firefox`.

## Commands

Only five commands are exposed in `package.json`. Each calls one matching entry
in `tools/`; helpers live under `tools/dev/`, `tools/build/`, `tools/check/`, `tools/package/`
and `tools/docs/`. Manual source exports and icon regeneration live in
`tools/maintenance/`; evidence helpers live in `test/support/`.

| Command                 | Purpose                                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------------------ |
| `npm run dev`           | Start the browser workbench and local Kafka development fixture                                  |
| `npm run build`         | Check TypeScript and build the renderer and Electron host                                        |
| `npm run check`         | Run all local CI qualification                                                                   |
| `npm run package`       | Desktop packaging; `-- plugin [eda\|nsp]` builds optional plugins, `-- eda` builds local EDA OCI |
| `npm run docs -- serve` | Serve docs locally; use `build` to build them                                                    |

Local CI runs workflow validation, formatting, lint, TypeScript, architecture,
unit/integration tests, EDA source and agent checks, dependency checks, a
**60-second performance soak**, docs qualification and configured live EDA/NSP tests.
The stages are listed directly in `tools/check.sh`; it stops at the first failure.
`npm run docs -- qualify` checks npm command examples in tracked Markdown and the
retrieval guide's required actions and UI labels. It owns documentation tests,
the strict build and Chromium checks. Firefox intro playback checks run when
media, presentation or its toolchain changes, when CI cannot
establish a comparison base, or with `npm run docs -- qualify --media`. Local runs
cache a successful media fingerprint; unchanged prose still gets all routine checks.
CI installs Firefox only when playback is required. Browser checks are separated
into navigation/accessibility, the static server and real media playback helpers;
there are still only five npm commands.
The pinned Zensical search widget has a small compatibility adapter in
`website/docs/assets/search-accessibility.js`. It supplies accessible control names,
combobox selection state, contrast and keyboard focus handling. Recheck the adapter
when upgrading Zensical. Browser qualification requires zero accessibility findings,
including the search dialog, and exercises keyboard selection, filters, closing
and reopening in both themes on desktop and mobile. Its report is retained in
`.artifacts/website/search-accessibility.json`; there are no widget or rule exceptions.
Automated checks alone do not establish full WCAG conformance.
GitHub calls `npm run check -- --ci` to run the same checks without the local
soak or live EDA/NSP stages. It still qualifies documentation in a browser.
The soak exercises the application pipeline at 1,000 records/s with mixed payloads
and clone round trips. Results are in `dist/performance/qualification-soak.json`;
docs results are in `.artifacts/website/`.
Public docs lead with the installed desktop workflow; the optional source checkout
walkthrough lives in `website/docs/start/development.md`. Keep operator facts in
the security, recovery, EDA, NSP and data-handling references, and link to them from
feature guides instead of copying procedures. The existing docs qualification
checks published message limits against the runtime contracts. The shared guide
notice distinguishes development docs from published current-main docs and identifies
the desktop build plus plugin/EDA compatibility. Record operator rehearsals and
their limits in `website/docs/guide/qualification.md`; site browser checks alone
do not validate backup/restore or broker permissions.

For a focused test, run `npx vitest run --config config/vitest.config.ts PATH`.
Vitest uses available CPU capacity, up to four isolated workers, and saves test
results and timings in `.artifacts/ci/vitest.json`. Add `--maxWorkers=1` when
debugging a test. Unit tests should import the module under test directly; use
`test/support/paste-text.ts` for prerequisite text when keystrokes are not the behavior
being tested.
Keep edge cases at their owning layer and exercise shared shell navigation once.
Theme tests check contrast and contract mapping rather than copying design constants.
`tools/check/eda-source.mjs` owns EDA catalog, version and deployment policy checks;
the architecture suite separately checks the plugin's bundled EDA public key.
The source ownership check follows imports from the actual desktop, renderer,
worker and browser development entrypoints. Test-only imports and disconnected
import cycles do not establish runtime ownership. The separately built plugin
backend and renderer are additional production entrypoints. Core source imports
cannot reach plugin implementations. Both hosts load installed plugins optionally;
the EDA plugin uses the application adapter and does not deploy resources through
Kubernetes directly. Generated EDA catalog projections belong on publication
branches, outside the development source tree.
For Electron/web system tests, use `node tools/package/e2e.mjs electron`
(or `web`) with their runtime prerequisites available. Benchmarks and fixture
utilities can be invoked directly through `node --import tsx PATH`.

Within `src/features/kafka`, keep behavior with its owner. The UI's
`StreamSkopeWorkbench` composes views; `useWorkbenchActivity`, `useWorkbenchProfiles`
and `useWorkbenchTopics` own their interaction state and effects. The facade's
`ConsumptionFacadeController` owns presentation queues, flushing and terminal
notifications. The application's `KafkaSessionRequests` owns request supersession,
cancellation and stale-response checks; `KafkaApplicationSession` owns connection
and consumer lifecycles. Preserve the typed host contract between UI and facade.

`src/platform/node` owns shared backend composition, plugin loading, SSH/HTTPS adapters and
file stores. Browser development and Electron use these same modules. Electron
windows, IPC, exit prompts and operating-system secret protection stay in
`src/platform/electron`; shared Node modules cannot import Electron code.

`src/plugins` owns plugin API version 4, independent SemVer, compatibility intervals
and bounded JSON/manifest validation. API 2 and API 3 remain supported for existing
installed and published packages.
`plugins/eda` owns EDA schemas, API/tunnel/session lifecycle and capture UI.
`plugins/nsp` owns NSP workflow definitions, API execution/cleanup, profile onboarding
and its UI. The NSP workflow source is
`plugins/nsp/resources/nsp-capture.workflow.yaml`; the backend embeds its exact bytes
during packaging. Each plugin's backend and renderer are standalone bundles.
UI contributions mount their own
React root through a generic interface; renderer assets are served from the
application origin without Node access or remote script loading. The backend
runs as trusted first-party code in the host process, not as a security sandbox
for arbitrary third-party extensions.

NSP uses the existing core profile test/save path. The helper definition is immutable
and adopted only after exact-content verification; owned executions have unique
request markers and must be removed before saving credentials. Non-secret recovery
identifiers are persisted through the optional `PluginBackendHost.recoveryState`
capability outside version directories. NSP requires that capability and rejects
older hosts that lack it; existing EDA API 2 packages remain compatible. Recovery
writes never contain credentials or workflow output. Removal retains the shared
helper and saved profiles, and refuses to complete when execution cleanup is uncertain.

Trust retrieval commands and identity review live in `useRemoteTrustAcquisition`;
`useTrustAcquisitionOperation` owns pending requests and candidate cleanup.
The application's `TrustAcquisitionLifecycle` owns editor leases, identity
challenges and cancellation deadlines. `KafkaProfileDraftResolver` resolves
trust and bindings; `KafkaProfileService` owns profile persistence and mutations.

The internal host protocol is version **28**. EDA-specific commands/events now
belong to the plugin's own protocol and travel through generic `plugin.execute`
and `plugin.event` envelopes. `plugins.*` commands manage installation and exit.
Upgrade the host, preload and renderer together. Plugin API version 4 is separate
from this internal protocol; the host bridge supplies its current protocol number.
Changes to public plugin capabilities still require explicit compatibility review.

Old EDA profile metadata migrates to a generic plugin-owned source on read,
without requiring the plugin. Saving persists that new representation; older
desktop builds cannot read newly saved plugin metadata. Keep a protected profile
store backup before downgrading. Missing plugins block connect and connection
tests before a stale tunnel endpoint reaches Kafka, while preserving recovery data.

For local EDA, set `STREAMSKOPE_EDA_API_URL`, `STREAMSKOPE_EDA_API_USERNAME`,
and `STREAMSKOPE_EDA_API_PASSWORD`. Optional settings are `STREAMSKOPE_EDA_API_CA`,
`STREAMSKOPE_EDA_API_CLIENT_SECRET`, `STREAMSKOPE_EDA_CAPTURE_PRODUCER`, and
`STREAMSKOPE_EDA_CAPTURE_LOCAL_PORT` (default 19092).
`node --import tsx tools/check/eda-live.ts` skips without connection settings and fails for partial,
unreachable, or invalid configuration. It requires the matching capture app
and an exporting producer, consumes one real Kafka event, then removes its
own temporary session. Generate an event during the test if needed.
TLS verification stays enabled; credentials belong in ignored local configuration.
`dist/ci/eda-live.json` records passed, failed, or skipped. These tests run locally.

For local NSP qualification, set `STREAMSKOPE_NSP_CONFIG` to a private JSON file
containing `apiUrl`, `username`, `password` and `verifyCertificate`. Optional
`brokers` is an array of reachable `host:port` addresses; optional `authentication`
is `auto`, `tls` or `oauth`. Keep this file outside tracked source with restrictive
permissions; do not place credentials in command arguments. Certificate verification
should remain enabled except for an explicitly trusted development lab's API.
Kafka certificate verification remains enabled in either case.

```sh
STREAMSKOPE_NSP_CONFIG=/absolute/path/private-nsp.json node --import tsx tools/check/nsp-live.ts
```

The same harness runs at the end of local `npm run check`; GitHub `--ci` excludes
both live-cluster harnesses. An unset path records a skip, while invalid configuration
or an unsuccessful configured check fails. `dist/ci/nsp-live.json` records the
sanitized result. The harness exercises the generic plugin runtime and core profile
test/create/update/connect path, topic listing, hot removal and retained-profile
behavior. It creates or reuses the owned immutable helper and cleans up its execution;
it does not deploy a broker, produce messages or qualify every NSP version.

Host protocol 26 removes the obsolete `templates.*` catalog commands, `templates.changed`,
and password-only trust acquisition. SSH acquisition requires an explicit recipe.
The desktop preserves `templates/kafka-connection-templates.json` as a read-only migration
source for `recipes.legacy.preview` and `recipes.legacy.convert`; conversion writes only
`trust-acquisition-recipes.json`. Existing converted recipes and saved profiles retain their
storage schemas. Modern recipes initialize independently of missing or corrupt legacy data.
Older renderer/host pairs must be upgraded together. Command-specific response types
and `execute()` inference enforce the existing wire result shapes; this type refinement
did not increment protocol 26. Runtime validation still checks result shapes and
request identifiers at host boundaries.

## GitHub builds

The **CI** workflow runs the required **CI** source/docs check on every PR, push
to main, desktop release tags (`v*`) and plugin tags (`plugins/eda/v*`, `plugins/nsp/v*`). It uses the shared `check` command with
`--ci`; there are no labels, relaxed modes or change-based routing.
Configure the repository's branch rules to require the **CI** status before merging.

Desktop release tags start native and cluster packaging after the shared check passes. The
native runners build, launch and package installers for Linux x64, macOS ARM64
and Windows x64. A Linux runner builds the complete unsigned EDA OCI application
with EDABuilder v26.8.2 and uploads it separately. Desktop installer packaging
belongs to the release runners; local Linux development uses `dev` and `build`.
Local EDA OCI packaging remains available through `npm run package -- eda`.
Before a local EDA build, packaging authenticates using the EDA API settings above
and reads `GET /core/about/version`, comparing `eda.version` with the declared
target. Production timestamp/Git build suffixes are recorded separately from
the release number. Missing settings, an unreachable or untrusted API, an invalid
version or a mismatch stops the build before Docker or EDABuilder runs.
`dist/ci/eda-version.json` records the observed release and full build version;
successful packages include a copy in `dist/eda-package/eda-version.json`.
GitHub builds use the declared target and record `not-checked` for the cluster;
they cannot verify your local EDA. Configured live qualification also checks
the cluster version before starting a capture.
EDA packaging needs Linux and Docker; native desktop packaging needs its matching OS.
Local soak and live EDA/NSP results should be reported in the PR, including skips.

A desktop tag is exactly `v` plus `package.json.version`, such as `v0.2.0` or
`v0.2.0-rc.1`. The app, installer and embedded release identity all derive from
that one version. New release versions reject build metadata; CI run IDs and Git
commits provide traceability. Historical `+build.N` tags remain readable but are
not reused or created by the new release process.

After checks and native/unsigned EDA packaging pass, the desktop tag creates a
**draft release** with three unsigned installers, `SHA256SUMS` and reviewed notes
from `website/docs/releases/TAG.md`. A SemVer prerelease tag marks the GitHub
release as a prerelease; an ordinary version does not. Signing is a separate
property: these desktop installers remain unsigned. Desktop releases no longer
republish plugin packages. Review and publish the draft on GitHub.
When publishing a stable desktop draft, select **Set as the latest release** or
run `gh release edit v0.2.0 --draft=false --latest` for that reviewed version.
Publish release candidates with `--prerelease --latest=false` and plugin releases
with `--latest=false`. Drafts cannot themselves be GitHub's latest release;
independent plugin publications must not replace the desktop download destination.

For example, preparing `v0.2.0` requires `"version": "0.2.0"` in `package.json`
and `website/docs/releases/v0.2.0.md` declaring `release_version: 0.2.0` and
`release_tag: v0.2.0`. An RC requires its own exact version/tag/notes. Do not alter
a published version's contents. Keep historical notes intact and mark pending
features/notes `unreleased: true`. The website's `project.extra.desktop_release`
continues to identify an actually published installer, not the upcoming source
version. Update that download baseline and applicable unreleased notices through
a PR after publication. Source-tag qualification validates the new version's notes
without requiring unpublished download assets to exist.

The **Pages** workflow qualifies current `main` on documentation-related pushes,
manual dispatch from `main`, and desktop release publication. PRs qualify locally
built artifacts without deployment. Every deployment uses current `main`, even
when an older desktop release event triggered it. The build verifies that the
configured documented desktop is a published GitHub release with the exact three
installer assets and `SHA256SUMS` uploaded and nonempty, uploads the qualified
artifact and checks its public revision marker, key pages and bookmark redirects.
Set Pages source to **GitHub Actions** and allow `main` plus desktop release tags
(`v*`) in the `github-pages` environment's deployment rules. A manual run can retry
a failed deployment without a desktop release. Post-deployment failure requires
inspection of the reported URL/revision; retry only after establishing the cause.
Tag-push CI rejects a source version/tag/notes mismatch before packaging. Docs
retain the published download baseline and label source-only changes as unreleased.
Current source plugin declarations are rendered directly from their manifests;
keep published and historical package facts in the compatibility/release references.
Installation links and its release identity are generated from
`project.extra.desktop_release`; keep the download marker in the installation page.
**Launch video** remains a separate manual utility.

## Build a desktop package

1. Open a local checkout of StreamSkope on your target platform.
2. Use **Node 24.x** and install dependencies for that operating system and CPU:

   ```sh
   npm ci
   ```

3. Build on macOS ARM64, Windows x64 or Linux x64:

   ```sh
   npm run package
   ```

   The installer is written to `dist/installers/`: a DMG on macOS,
   an NSIS `Setup.exe` on Windows, or an AppImage on Linux.

4. Wait for package verification and the packaged launch test to finish.

**You should have:** the package at the printed output path. A successful build
checks the package; it does not establish signing, notarization or production support.

## Desktop plugin distribution

`npm run build` builds core and plugin bundles separately. `npm run package -- plugin`
creates both EDA and NSP artifacts under `dist/plugin-package/`. Each has a
`streamskope-NAME-vVERSION.skope-plugin` bundle and
`streamskope-NAME-vVERSION-plugin.json` manifest, where `NAME` is `eda` or `nsp`
and `VERSION` is the independent plugin SemVer described in
[Plugin versioning](website/docs/plugins/versioning.md). For example:

```text
streamskope-nsp-v0.1.0.skope-plugin
streamskope-nsp-v0.1.0-plugin.json
streamskope-nsp-v0.1.0-nsp-capture.workflow.yaml
```

The NSP bundle contains `nsp-capture.workflow.yaml` as a declared SHA256-verified
resource; the separate YAML download contains identical bytes. Maintain that
single source and its manifest digest together. Its deployed helper identity is
immutable: changing behavior or formatting requires a new helper identity and a
reviewed migration. The formatter excludes this YAML to preserve the existing
ownership fingerprint, and Git enforces LF line endings on every platform.
Packages accept only bounded, declared, flat data
resources; missing, extra, duplicate or altered resource files are rejected.
Use `npm run package -- plugin nsp` or `npm run package -- plugin eda` to select
one plugin. Every invocation clears the previous package output before writing
its selected artifacts. Packaging verifies the manifest identity, the exact EDA
target where applicable, and that the installed backend loads independently. The desktop installer excludes these
optional plugin bundles. The integration suite also exercises the actual packaged
renderer in Chromium. Browser installation coverage is available through
`node tools/package/e2e.mjs web test/e2e/web-plugin-installation.spec.ts`.

Release users install through **Preferences → Plugins**, using published assets from
`asadarafat/streamskope`. The installer verifies GitHub asset SHA256 digests and
the manifest/API contract and supported desktop interval, then writes immutable files
and atomically selects the active version. This verifies official repository
provenance and integrity; it does not
claim publisher cryptographic signing. Installed plugins work offline; catalog
refresh and download require GitHub access. Installation, updates and removal
apply immediately in the running application. Each activation has its own request
identity and renderer assets; callbacks from retired views cannot operate the new
instance. Changes serialize with shutdown and owned profile connections.

Active EDA work requires confirmation before cancellation and temporary-resource
cleanup. Cleanup failure leaves the current plugin available for retry. Successful
removal deletes its installation files while retaining saved profile metadata.
Failed backend or renderer activation restores the previous verified version when
available; a stopped capture must be resumed explicitly. Unrelated Kafka sessions
remain available. API 2 introduced these lifecycle hooks; API 3 added explicit
compatibility identities and resources. API 4 uses independent SemVer and an
inclusive minimum/exclusive maximum desktop interval. Host protocol 28 carries
activation identities and versioned snapshots. Existing API 2 and API 3 packages
retain their legacy versions and asset names and remain loadable on the new host.
A compatible API 4 package supersedes either legacy generation; within API 4,
updates follow SemVer precedence. Earlier unreleased API 1 packages
must be rebuilt; profile metadata stays at version 1.

NSP follows the same hot lifecycle. It retains a reusable shared helper workflow
instead of deploying a broker or tunnel. Cancelling, updating or removing the
plugin must confirm owned execution cleanup; failures retain durable recovery
identifiers for a later attempt. The catalog selects the highest compatible version
for each plugin independently and verifies each manifest/package digest. The current
catalog examines the first 100 GitHub releases, with at most 16 manifest candidates
per plugin within that window.

To release a desktop plugin, update its independent `version` in
`plugins/NAME/manifest.json` and push `plugins/NAME/vVERSION`, for example
`plugins/nsp/v0.1.0`. Keep its supported desktop interval and target bounds explicit;
changing plugin contents, workflow bytes or compatibility declarations requires a
new plugin version. The CI workflow validates the tag, runs shared checks,
packages only that plugin and creates its draft release. Review and publish it;
native desktop/EDA OCI packaging and Pages do not run for that tag. A desktop
release is not required for a plugin fix supported by the current host API.

The source prepares desktop **0.2.0**, plugin API **4** and independent **0.1.0**
EDA/NSP plugins, each declaring **>=0.2.0, <0.3.0**. Publish the supporting desktop
before announcing API 4 plugins as usable. The original **v0.1.0+build.1** and its
API **3** assets remain published and immutable. Keep source-only changes marked
unreleased until their supporting desktop and plugin releases are available.

Desktop plugin packaging does not require cluster access. It validates the
manifest and includes the declared compatibility range, but does not prove that
range works. Qualify every supported target release before widening the inclusive
numeric bounds; do not infer support from a wildcard, an API version or a newer
product's release date. The EDA plugin reads `/core/about/version` before discovery,
cluster application checks/installation and capture deployment. The NSP plugin
reads `/sdn/api/v4/system/version` before creating or executing its helper workflow.
Both reject unknown or out-of-range product versions before those operations.
The separate EDA cluster application build and configured live checks retain their
API-based target validation. Owned NSP execution cleanup remains available for
recovery after a target upgrade; cleanup does not create a new execution.

Browser development stores plugin installations under `.cache/development-plugins`;
desktop installations live under the application's user-data `plugins` directory.
Development uses the same immediate installation lifecycle. Source changes alone
do not replace an installed artifact; build and qualify the package before release.

The **Publish EDA application** workflow is manual from main. Configure the
`eda-production` environment with `EDA_APP_SIGNING_KEY_B64` and
`EDA_APP_SIGNING_KEY_PASSWORD`, matching the public key shipped with the app.
Its GitHub token needs repository and GHCR write access. It builds, signs and
verifies the image, then publishes a generated versioned catalog branch and tags.
Existing image/catalog versions are rejected. If publication stops halfway,
inspect the registry and catalog before recovering; the workflow does not overwrite them.
The unsigned OCI artifact from local/release-tag builds is for development, while this workflow
produces the signed EDA Store application.

## Versions and clean public history

The app and installer share `package.json.version`, currently **0.2.0**. Each
plugin declares its own SemVer in `plugins/NAME/manifest.json`, currently **0.1.0**
for EDA and NSP. The equality of their initial numbers does not couple releases.
A plugin bug fix can become **0.1.1** without changing the app or other plugin.

API 4 manifests separate identity from requirements:

```json
{
  "version": "0.1.0",
  "apiVersion": 4,
  "compatibility": {
    "streamskope": { "minimum": "0.2.0", "maximumExclusive": "0.3.0" },
    "target": { "system": "eda", "minimum": "26.8.2", "maximum": "26.8.2" }
  }
}
```

There is no API 4 `revision` or duplicated desktop release field. Use SemVer
patch/minor/major changes according to compatibility; below **1.0.0**, put breaking
changes in a new minor version and compatible fixes in a patch. Removing a supported
host or target release is a breaking change. A qualified wider target interval can
ship as a minor release. Keep `-rc.N` prereleases distinct; do not create new
`+build.N` release identities. Immutable plugin identity is its ID plus version,
including compatibility and resource declarations: never reuse it with new bytes.

Preview hosts require an explicit prerelease minimum with the same core version,
such as `0.2.0-rc.1` for host `0.2.0-rc.2`. A stable interval ending before `0.3.0`
does not implicitly support `0.3.0-rc.1`. Stable desktop catalogs exclude prerelease
plugins; preview desktops can select them when explicitly compatible. Update the
manifest for a qualified RC before packaging it.

The current source targets EDA **26.8.2–26.8.2** and NSP **26.4.0–26.4.0**. Host
bounds are inclusive/exclusive; target bounds remain inclusive/inclusive and exact
numeric versions. Record live qualification before claiming observed support.
Read [Plugin versioning](website/docs/plugins/versioning.md) for selection,
legacy-generation migration and component responsibilities. Installed API 2/3
packages and original release names remain unchanged; do not rewrite their version
strings or compare their numeric values directly with API 4 SemVer.

The **EDA cluster application is a separate artifact** and retains the exact full
target EDA version: **EDA 26.8.2 → app v26.8.2**. `EDA_TARGET_VERSION` in
`plugins/eda/contracts/eda-capture-types.ts` is authoritative; the cluster manifest,
bundled agent and both EDA desktop plugin target bounds must match it. App fixes do not
increment the cluster application version independently or add suffixes. Existing
published versions remain protected from overwrites; inspect partial publications
before recovery. EDABuilder v26.8.2 and EDA Core API v6.0.0 are separate tool/API
versions. Regenerate signed catalog output through EDABuilder.

To start a new GitHub repository from the current source without old local
commits, export into a new directory. Deleting and recreating the same GitHub
repository does not free previously published immutable tag names; use a new,
unused tag there.

```bash
npm run check
node tools/maintenance/export-source.mjs /absolute/path/to/streamskope-public
cd /absolute/path/to/streamskope-public
npm ci
npm run check
# Review the source before creating the first public commit and tag.
git add .
git diff --cached --stat
git commit -m "chore: initialize StreamSkope source"
# Select an unused release tag before publication.
```

The export includes current uncommitted public source and starts `main` with no
commits, tags or remotes. It excludes ignored files and historical generated
`apps/` projections, and rejects symlinks/existing destinations. Review for secrets
before publication. Add the replacement remote and push only after arranging the
GitHub repository replacement. Exporting leaves development history intact.
