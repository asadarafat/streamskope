# Releases and publication

Release from qualified `main`, review the generated draft, and publish only the
acceptance you can substantiate. Product versions are assigned by release CI;
ordinary PRs qualify source without assigning a version or publishing packages.
Use [development](development.md) for local builds and
[qualification](qualification.md) for check results and release evidence.

## Prepare a release

1. Merge the intended changes through a normal PR with the required **CI** status
   and an up-to-date branch. PR CI qualifies GitHub's temporary merge revision;
   merging to `main` does not repeat it. Release-note labels never bypass checks.
2. Review the selected component's pending changes and short release highlights
   using the [release-notes procedure](#release-notes). Confirm the previous
   publication's archive is present on `main`; release preparation rejects an
   incomplete archive, baseline or component index.
3. Review relevant [local and native acceptance](qualification.md), including
   unselected scopes, skips and limitations. PRs need not repeat complete local
   qualification, but milestone/release integration claims still require core
   and the relevant live/native evidence. Packaging alone does not establish
   live EDA or NSP support. Widen compatibility declarations only after
   qualifying those targets.
4. Open **Actions → Release → Run workflow**, select **main**, choose **desktop**,
   **eda** or **nsp**, and enter its next bare SemVer, such as `0.2.0` or
   `0.2.0-rc.1`. These are examples, not reserved versions.

The [Release workflow](../.github/workflows/release.yml) freezes the selected
source and notes, then invokes the same shared, docs and runtime CI lanes again
before packaging. Tags do not trigger it. Keep branch rules requiring a PR, the
**CI** status and an up-to-date branch, without bypasses.

| Component | Version and tag                                                           | Packaging after CI                                                                                                                   |
| --------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `desktop` | App, desktop installers and browser image share `VERSION`; tag `vVERSION` | Three native desktop installers, two native browser images/archives, registry and installer qualification, unsigned EDA OCI artifact |
| `eda`     | Independent plugin SemVer; tag `plugins/eda/vVERSION`                     | EDA plugin primary package, signed portable package and shared manifest                                                              |
| `nsp`     | Independent plugin SemVer; tag `plugins/nsp/vVERSION`                     | NSP plugin primary package, signed portable package, shared manifest and workflow resource                                           |

A plugin release does not run desktop or EDA OCI packaging or deploy Pages. A
desktop release does not republish plugins. The EDA cluster application has its
own [version and publication procedure](#eda-cluster-application).

## Version and source identity

Keep `package.json`, its lockfile and source plugin manifests at `0.0.0-dev`.
Release CI stamps only a disposable build checkout using the selected input; it
does not commit a version bump to `main`. Source archives at the release tag
therefore retain the neutral development identity. Compatibility bounds express
requirements, not an assigned next app version.

Use SemVer for the app and each desktop plugin independently. Below `1.0.0`, put
breaking changes in a new minor version and compatible fixes in a patch. Removing
a supported host or target version is a breaking plugin change; a qualified wider
target interval can ship in a minor release. Keep `-rc.N` prereleases distinct.
New releases reject build metadata and reserved development identities. Historical
`+build.N` tags remain readable and are never reused. CI run IDs and source commits
provide build traceability.

The final draft step reserves its tag against the qualified source before
creating the release. Existing tags and releases are rejected. After a partial
failure, inspect the existing draft, tag and any registry publication before
retrying. Choose an unused identity or deliberately resolve an unpublished failed
identity; the workflow never overwrites or deletes it automatically.

To reproduce packaging, use a disposable checkout of the exact tag, install its
locked dependencies, and apply the recorded component and version:

```sh
node --import tsx tools/package/release-version.ts COMPONENT VERSION --stamp
```

Then follow the relevant packaging procedure. This reproduces the assigned build
inputs; it does not reproduce missing acceptance evidence or signing authority.

## Review and publish the draft

Desktop release runners build and launch unsigned installers for Linux x64,
macOS ARM64 and Windows x64. Separate native Linux AMD64 and ARM64 jobs build,
save, reload and inspect browser images. The registry job publishes the qualified
images at `ghcr.io/asadarafat/streamskope:VERSION`, checks the multi-platform
index and anonymous pulls, then promotes the version tag. Published deployment
uses its exact index digest; it does not use a floating `latest` image.

The registry job needs `packages: write`, repository Actions access to the GHCR
package, and public package visibility. This is separate from the archive App's
[Actions read permission](#release-documentation-app). A private or inaccessible
candidate fails delivery qualification. Both native installer jobs then exercise
the stamped installer against public GHCR, including installation, initialized
vault/profile persistence, workers, repeated installation, graceful resume and
upgrade/rollback. See [browser qualification](qualification.md#browser-data-compatibility)
for exact evidence requirements and the limits of local staged-image rehearsal.

Before publishing a desktop draft, verify:

- Three unsigned installers and six browser payloads: AMD64/ARM64 Docker save
  archives, online/offline Containerlab topologies, the image manifest and
  `install-browser-workbench.sh`.
- `SHA256SUMS` covers those nine payloads and `qualification-vVERSION.json`.
  Assembly checks their source, version, architecture and image identities.
- The qualification report contains the actual release-run CI and package
  evidence. Local checks remain explicitly unrecorded until a compatible receipt
  is attached. Follow [draft enrichment](qualification.md#attach-local-acceptance-to-a-draft)
  before publication; review executed checks, skips and scope limits. Attachment
  accepts one passing source-bound bundle containing core stages, plus the
  selected plugin's live stage for a plugin draft. Standalone live retries do not
  combine with a failed or separate core bundle into release acceptance.
- Generated changes, reviewed highlights, upgrade instructions and limitations
  describe the actual artifacts. Inspect the retained `release-changelog` and
  `desktop-release-notes` artifacts as well as the draft body.

The complete unsigned EDA OCI application is a separate Actions artifact; it is
not one of the desktop release downloads. Installer build success does not mean
the app is signed or notarized. A draft does not expose its release downloads;
the qualified browser image has already passed public registry promotion.

The current browser manifest is schema 4. It binds the data contract, read-only
inspector and rendered installer to exact files and source. Historical schemas
1/2 remain verifiable without an installer; schema 3 metadata remains readable
for pinned resume, but full verification of its installer needs its historical
source. Keep [operator deployment](../website/docs/guide/browser-deployment.md)
and [data compatibility](qualification.md#browser-data-compatibility) as the
owners of installation and recovery constraints.

For plugin drafts, verify the primary package, signed portable package, shared
manifest, declared resources, qualification report and checksums. The portable
file must enclose the exact primary bytes. Inspect `plugin-release-notes` and the
component's target/host compatibility before publishing. Plugin packaging has no
cluster access and does not validate an advertised target interval by itself.

Publish the reviewed draft in GitHub. For a stable desktop release, select
**Set as the latest release** so the browser quick-install entry point resolves
to it. Publish prereleases and independent plugins without changing that desktop
destination. A SemVer prerelease is marked as such automatically; signing status
is a separate property.

## Release notes

Use a Conventional Commit PR title describing the user-visible outcome, such as
`fix(eda): recover capture after restart`. The collector groups merged PRs into
Security, Breaking changes, Features, Fixes and Other changes. Titles and labels
do not choose the next release version.

| Label                | Include the PR in                                          |
| -------------------- | ---------------------------------------------------------- |
| `component:desktop`  | Desktop releases                                           |
| `component:eda`      | EDA Connector plugin releases                              |
| `component:nsp`      | NSP Connector plugin releases                              |
| `component:shared`   | All three release families                                 |
| `release-notes:skip` | None; retain the deliberate omission in selection evidence |

Multiple component labels are allowed. Without them, the collector identifies
plugin-owned changes from these paths:

- `plugins/eda/` and `plugins/nsp/` select the corresponding plugin.
- Matching `eda-` or `nsp-` filenames under `test/unit/`, `test/integration/`,
  `test/architecture/` and `test/support/` select that plugin, as do
  `website/docs/plugins/eda.md`, `website/docs/plugins/nsp.md` and the matching
  `website/docs/guide/eda/` or `website/docs/guide/nsp/` directory.
- `vendors/streamskope/apps/` selects EDA.

A PR touching only these paths selects all represented plugins. Any other path
makes an unlabelled change shared. Explicit labels override inference; review
them especially for tooling and documentation changes. The
[collector](../tools/package/release-changelog.ts) owns this rule.

The baseline is the nearest ancestral published release of the same component.
Stable releases use a stable baseline, including eligible historical `+build.N`
tags, so intervening release-candidate changes remain included. Drafts, unrelated
components and sources outside the selected ancestry are ineligible. Without a
baseline, notes explicitly cover full history. Selection stops at the exact
chosen source even if `main` advances. Unassociated commits appear separately
without duplicating merged PR entries.

For native GitHub stacks, the stack's target branch determines whether a merged
PR belongs to `main`; upper PRs can retain intermediate base branches. Each PR
keeps its own labels/title, and its merge commit must belong to the selected
mainline history in the same repository.

Keep short reviewed highlights in
[`website/docs/releases/unreleased.md`](../website/docs/releases/unreleased.md),
[`plugins/eda/RELEASE_NOTES.md`](../plugins/eda/RELEASE_NOTES.md) or
[`plugins/nsp/RELEASE_NOTES.md`](../plugins/nsp/RELEASE_NOTES.md). The desktop page
keeps `unreleased: true`, without `release_tag` or `release_version`. Use absolute
published documentation URLs in commentary because the body also appears on
GitHub. Do not duplicate the full PR inventory by hand.

Inspect pending changes from committed source with:

```sh
npm run docs -- pending
npm run docs -- pending --component eda
```

This uses `GH_TOKEN` or GitHub CLI authentication, fetches release tags without
rewriting them, and writes Markdown/JSON under `.artifacts/release-pending/`.
It excludes uncommitted work, retains omitted PR evidence and keeps prerelease
changes pending until stable publication. API failure stops generation rather
than reporting an empty list. Normal static, PR and docs qualification does not
query GitHub for this inventory, and the command assigns no upcoming version.

## Publish and synchronize documentation

The final **published GitHub release body** is authoritative, including edits
made while reviewing the draft. Publication opens or updates one managed
`automation/release-docs` PR to archive it unchanged. The archival PR synchronizes
repository history and future snapshots; it does not deploy Pages or rewrite
an existing snapshot. Normal PR CI qualifies its exact head before a protected
merge. Closely spaced component publications converge on the same serialized PR.

The [Pages workflow](../.github/workflows/docs.yml) deploys only the highest
published stable desktop SemVer. It ignores publication dates, plugin versions
and GitHub's mutable **Latest** designation. PRs, main pushes, tag creation,
drafts, prereleases and plugin publication do not deploy Pages; there is no
manual Pages dispatch. Prereleases remain downloadable on GitHub.

Pages verifies the immutable release and tag source, checks out that exact
commit, and repeats latest-version selection before deployment. It stamps a
disposable checkout and prepares the publication body, download baseline and
plugin availability snapshot; newer `main` is never presented under an older
release label. The archive job is independent: an archive handoff does not block
or replace release-pinned Pages deployment. See
[documentation maintenance](documentation.md) for page metadata, snapshots,
assets and authoring checks.

To recover deployment, inspect the failed Pages job and rerun that release's run
if it still represents the highest stable desktop. Configure Pages source as
**GitHub Actions** and allow desktop tags (`v*`) in the `github-pages` environment.
Existing runs retain their original workflow revision.

Archive reconciliation preserves commentary changed since its published source,
and clears only matching shipped highlights. Prerelease archival preserves
stable-release highlights. An actual changed archive PR with preserved commentary
requires review. If records already match `main`, newer highlights are advisory
and do not make the archive incomplete. Existing differing archive bodies need
a reviewed correction; immutable tags/assets do not make GitHub note text
immutable. Release preparation blocks the next component release until required
archives, baselines and qualification links are present.

## Release documentation App

Automatic archival uses a private GitHub App installed for this repository only:

- **Contents: write**, **Pull requests: write**, **Actions: read**.
- Repository variable `STREAMSKOPE_RELEASE_APP_CLIENT_ID` and secret
  `STREAMSKOPE_RELEASE_APP_PRIVATE_KEY` configured together.
- Repository settings permitting Actions-created PRs and auto-merge, with normal
  branch protection and required CI intact.

Verify the actual App owner, selected repository, installation and permissions
before provisioning credentials. The pinned token action requests the explicit
owner and single repository, then revokes its installation token after the job.
With neither setting, archival uses the built-in token and manual handoff. One
setting alone fails before checkout; invalid App credentials never fall back.

App registration, installation and real App-triggered normal PR CI need separate
owner verification. **App activation is pending** until that evidence exists;
workflow tests alone do not establish it.

Keep the original PEM outside Git and release artifacts in a mode `0700`
directory, with file mode `0600`, plus an encrypted owner-controlled backup.
GitHub cannot return its stored secret. Rotate by creating a replacement key,
updating the secret, verifying scoped token creation and a genuine App-triggered
PR run, then revoking the old key. Never print private keys or tokens.

## Recover finalization

The records job invokes the [reconciliation tool](../tools/package/release-documentation.ts)
once:

```sh
node --import tsx tools/package/release-documentation.ts reconcile
```

That command owns reconciliation and bounded merge observation. Read its summary
and `finalization_status`/`archive_complete` outputs; a green job can mean a
completed handoff rather than a merged archive.

| Finalization          | Meaning and next action                                                                      |
| --------------------- | -------------------------------------------------------------------------------------------- |
| `unchanged`           | Records already match main; archive complete. Preserved new highlights can remain advisory.  |
| `merged`              | The exact managed PR's protected merge and bound CI success were observed; archive complete. |
| `approval-required`   | Approve an actually blocked PR run, or use the no-run procedure below.                       |
| `review-required`     | Review preserved commentary, then complete a normal protected merge.                         |
| `awaiting-maintainer` | CI is active or successful; manual mode leaves the merge to a maintainer.                    |

Pending results include the exact PR/head and `archive_complete=false`.
`review_required=true` is a legacy guard for every handoff, so older publication
workflows skip their former shell poll. `approval_required` identifies the
approval handoff specifically.

For an actually blocked workflow, select **Approve workflows to run** on the PR,
wait for normal CI, then retry the records job or complete its protected merge.
The built-in token cannot approve its own blocked workflow. If no PR run exists,
a maintainer can close and reopen **the same PR** using their GitHub session.
The normal `pull_request` reopened event qualifies it. Preserve its branch and
metadata; an empty trigger commit violates the automation ownership checks. A
dispatched workflow or manually posted status cannot substitute for ordinary CI.

Terminal failed, cancelled or timed-out CI remains a failure in either token
mode; automation never reruns it or calls it an approval handoff. API/ownership
errors, closure without merge and exhausted waits also fail. Automatic mode
waits up to 90 seconds for CI appearance and 25 minutes for finalization, with
finite attempt limits. When main advances, it refreshes only the exact clean
owned checkout, regenerates the PR and reassesses the new head. Changed executing
toolchain/dependency inputs require a fresh job, not an in-process refresh.
Unchanged generated trees reuse their commit and existing CI.

Retrying archival does not republish packages. Older runs keep their original
workflow definition: a run requesting Actions write can fail before reaching the
current command when the App permits only read. Keep the narrower App permission.
Finish the same PR through normal protected merge, or run the reconciliation
command from current source in a separate clean main checkout with
`GITHUB_REPOSITORY` and an authenticated maintainer's `GH_TOKEN` supplied privately.
A future publication uses the updated workflow. Confirm synchronized main before
starting the next release; requesting merge is not proof it completed.

## Desktop plugin distribution

Each EDA/NSP plugin uses independent SemVer and explicit host/target compatibility
in its [EDA manifest](../plugins/eda/manifest.json) or
[NSP manifest](../plugins/nsp/manifest.json). Host intervals are inclusive
minimum/exclusive maximum; target bounds are inclusive exact numeric versions.
Changing package bytes, workflow resources or compatibility requires a new plugin
version. Stable catalogs exclude prerelease plugins; preview hosts require an
explicit compatible prerelease minimum. See
[plugin versioning](../website/docs/plugins/versioning.md) for selection and
legacy API 2/3 migration rules. Do not rewrite legacy package identities or compare
their numeric values directly with current SemVer packages.

Production packaging requires `STREAMSKOPE_PLUGIN_SIGNING_KEY_B64`: a
base64-encoded Ed25519 PKCS8 PEM private key matching the public registry in
[`publishers.ts`](../src/platform/node/plugins/publishers.ts). Release CI supplies
it only to the selected plugin packaging step after qualification. Missing or
mismatched keys fail. Local development packages remain unsigned and need no
secret. Preserve the protected key backup outside Git; publish a supporting host
with the new public key before signing packages with a rotated key.

The signed portable envelope binds publisher identity and the exact primary
package bytes. Assembly verifies its signature, payload equality and shared
manifest. Do not create a separate portable manifest. GitHub installation checks
official repository provenance and asset digests; native file installation
requires the signed portable envelope. These are different trust checks. Runtime
limits and hot lifecycle ownership belong to
[architecture](architecture.md#plugin-lifecycle-is-a-transaction-boundary); use the
[offline guide](../website/docs/plugins/offline.md) for user installation.

The NSP bundle declares `nsp-capture.workflow.yaml` as a SHA256-verified resource;
the standalone download has identical bytes. Its helper identity is immutable:
behavior or formatting changes require a new helper identity and reviewed
migration. Keep its single source/digest together, preserve LF endings, and retain
its formatter exclusion. Missing, extra, duplicate or altered resources fail
package validation.

Package compatibility is a declaration, not live proof. Qualify every supported
target release before widening it. EDA reads `/core/about/version` before
discovery, cluster application checks/installation and capture deployment. NSP
reads `/sdn/api/v4/system/version` before creating/executing its helper. Unknown or
out-of-range versions are refused. Owned NSP execution cleanup remains available
after a target upgrade and does not start a new execution.

Publish compatible supporting hosts and plugins before announcing availability.
Offline users need the compatible signed portable packages ahead of an upgrade.
Keep this a compatibility/publication decision rather than a fixed future-version
plan. Use [source plugin QA](development.md#build-source-and-test-a-development-plugin)
for development artifacts and
[live qualification](qualification.md) for observed target behavior.

## EDA cluster application

The EDA cluster application is separate from the desktop EDA plugin. Its version
equals the exact target EDA release; app fixes do not independently increment that
version or add suffixes. `EDA_TARGET_VERSION` in
[`eda-capture-types.ts`](../plugins/eda/contracts/eda-capture-types.ts) is
authoritative. The cluster manifest, bundled agent and both EDA plugin target
bounds must agree. EDABuilder and EDA Core API versions are separate tool/API
identities. Existing image and catalog versions cannot be overwritten.

Local `npm run package -- eda` requires Linux/Docker and configured EDA API access.
It reads `GET /core/about/version` and compares `eda.version` before invoking
Docker/EDABuilder; unavailable, untrusted, invalid or mismatched responses stop
the build. Release/build suffixes are recorded separately in
`dist/ci/eda-version.json` and the successful package's copy. GitHub unsigned
packaging uses the declared target and records the cluster as `not-checked`.
Refer to [live EDA configuration](qualification.md#live-eda) for local settings.

The manual [Publish EDA application workflow](../.github/workflows/eda-app-release.yml)
runs from main using the `eda-production` environment. Configure
`EDA_APP_SIGNING_KEY_B64` and `EDA_APP_SIGNING_KEY_PASSWORD` matching the shipped
public key. This signing key is separate from desktop-plugin signing. The job
needs repository/GHCR write access, builds/signs/verifies the image, and publishes
the generated versioned catalog branch/tags through EDABuilder. It rejects
existing identities. After partial publication, inspect both registry and
catalog before recovery; do not overwrite either. Local and desktop-release
unsigned OCI artifacts are development outputs, not the signed EDA Store app.
