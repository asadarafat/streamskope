# Qualification and release evidence

Run `npm run check` before requesting a merge. It executes shared checks, the
60-second application-pipeline soak, docs and configured live EDA/NSP checks. The
command records a separate qualification bundle for each run under
`.artifacts/qualification/` and prints its location. Keep the complete bundle when
preparing a release; a console transcript alone does not establish acceptance.

Use `npm run check -- --ci` with Docker and Chromium to reproduce the GitHub
shared, docs and real-provider/browser lanes. GitHub runs those lanes in parallel
and retains their reports and aggregate index in the `qualification` artifact.
The existing five npm commands remain the entry points.

## Read a local result

The receipt records the source revision and Git tree, whether the checkout was
dirty, source agreement before and after execution, platform/architecture and
Node version. Each stage has an explicit outcome and hashes of its copied
evidence. A failed stage leaves later stages unexecuted. Unconfigured live systems
are recorded as skipped, even when the overall command exits successfully.

For release acceptance, run from a clean committed checkout and leave its source
unchanged until qualification finishes. A dirty run is useful development
feedback, but cannot establish release acceptance. Git index flags that hide
changes (`assume-unchanged` or `skip-worktree`) are rejected. A normal interruption
records failure and leaves later stages unexecuted; host loss or forced termination
can leave an incomplete bundle, which is rejected. Do not edit receipts, copy old
results into a new run, or relabel the execution revision after a squash merge.

Private connection settings stay outside the bundle. The collector retains safe
target-version and certificate-verification facts, check outcomes and declared
measurement limits. It excludes connection credentials, endpoint addresses,
broker payloads and raw exception text from the shareable report.

## Review release acceptance

Release CI assembles the qualification report from its own source index, package
outputs and native browser-installer receipts before creating the draft. It
verifies source/run identity and evidence hashes, and records local checks as
unrecorded until a compatible local bundle is supplied. Publication never turns
an unexecuted check into a pass.

| Change or claim                                        | Evidence required                                                                                                                              |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Any source change                                      | Successful required PR CI for the current base; local qualification and explicit skips in the PR                                               |
| Desktop distribution                                   | Exact release source, three native package/launch jobs and the downloaded installer hashes                                                     |
| Browser distribution                                   | Native AMD64/ARM64 archive and installer lifecycle jobs, image identity, read-only data inspection, encrypted persistence and graceful restart |
| Local pipeline performance                             | The unchanged 60-second pipeline soak; this does not measure broker throughput or rendered UI endurance                                        |
| EDA or NSP connection/lifecycle behavior               | Configured live checks for the affected target version, known record receipt and confirmed owned-resource cleanup                              |
| Native protected-storage or installed upgrade behavior | An explicit installed/native rehearsal; source-host live checks do not establish it                                                            |
| Long-running broker or UI behavior                     | A separate real-provider/rendered-product endurance run; a one-minute pipeline run does not establish it                                       |

The report separates these scopes. Review gaps against the changed behavior before
publishing. A passing source gate alone is insufficient evidence for a native,
live-system or recovery claim. Existing historical reports retain their original
scope and identity.

## Browser data compatibility

Browser images contain a standalone read-only data inspector. Native archive and
installer qualification run it after a confirmed graceful stop, using the exact
image ID with a read-only root and data mount, no network or capabilities, and the
application's non-root UID/GID. Qualification compares the complete disposable
data inventory before and after inspection, then restarts and unlocks the host to
verify the saved profile. The receipt binds inspection to the source, image and
architecture that actually ran.

The inspector checks supported outer storage formats and retained plugin package
integrity without opening the vault or executing plugins. It reports encrypted
content authenticity, protected profile schemas, remote resource cleanup and host
quiescence as unverified. The native lifecycle checks establish their own stop and
unlock results; a standalone successful inspection does not establish those facts.
Unsafe paths, unknown formats, pending plugin changes, nonempty recovery journals
and saved plugin-managed profile sources block inspection. Preserve those records
and resolve ownership through the supported application or recovery procedure;
deleting profile metadata is not proof of cleanup.

Current browser manifests declare the data contract and inspector in schema 4.
Historical manifests remain readable for pinned resume; they do not acquire this
capability retrospectively. New release assembly validates the current inspector
and exact installer bytes. An older schema 3 installer requires qualification from
its own source, rather than reconstruction using today's template.

Local staged-image receipts are separate from public-registry delivery evidence
and cannot satisfy release acceptance. The installer uses this inspection as one
part of its [owned maintenance transaction](../website/docs/guide/browser-host.md#upgrade-an-installer-managed-host),
alongside confirmed graceful shutdown, the held original vault lease, a complete
private backup and locked target readiness.

Native installer qualification separately exercises an actual transition from
the exact reviewed 0.10.3 image: initialize a vault and saved profile, check and
upgrade while unlocked, unlock the target and verify persistence/native workers,
roll back, unlock the predecessor and verify a pinned rerun. Both transitions
must have complete backup integrity evidence. Public receipts require the exact
public target image and anonymous registry delivery; a source build or sealed
local rehearsal does not establish that scope. A local rehearsal may use a real
locally staged target without publishing an image, but its receipt remains local.

The first reviewed legacy predecessor is 0.10.3; these checks do not establish
automatic migration from every historical release. Readiness proves the locked
host started, while the authenticated native fixture establishes its own saved
profile and unlock result. Neither establishes remote plugin-resource cleanup.

## Attach local acceptance to a draft

After Release CI creates the draft, use the existing package entry point with
the full local bundle path:

```sh
npm run package -- qualification vVERSION --local .artifacts/qualification/RUN_ID
```

For a plugin, use its release tag, such as `plugins/eda/vVERSION`. The command
requires a draft with matching source and verified payloads. A clean PR commit can
qualify a squash-merged release only when Git independently verifies identical
trees; the report retains both commit identities and the equivalence decision.
Different trees require a new run. Attaching evidence to a plugin draft also
requires that plugin’s live stage to have passed; a desktop report retains explicit
live skips without converting them into passes.

The command updates only the qualification report and its checksums. It refuses
published/immutable releases and preserves the packaged payloads. If an upload is
interrupted, inspect the reported recovery information and the draft's two
metadata assets before publication; updating two GitHub assets is not an atomic
operation. Originals are retained under the reported
`.artifacts/qualification-drafts/update-*/original/` directory. If only one upload
succeeded, confirm the release is still a draft and restore those two original
metadata files before retrying. Do not overwrite concurrently changed packages or
restore metadata to a published release.

Review the report, `SHA256SUMS`, source and remaining gaps, then publish the draft
through the normal release process. The published documentation links the exact
qualification asset delivered with that release.
