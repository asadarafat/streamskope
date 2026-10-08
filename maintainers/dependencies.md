# Dependency maintenance

Dependency changes follow the normal reviewed PR and qualification path. Keep
exact versions and lockfiles together, inspect the upstream change, and retain
independent regressions for behavior StreamSkope relies on. Dependency updates
do not assign a product release version or merge automatically.

## Review an update

1. Review the manifest and complete lockfile diff, including nested dependencies,
   license changes and runtime versus development classification.
2. Check the [mitigation inventory](../tools/check/dependency-maintenance.ts).
   A changed mitigated dependency needs deliberate review even when only its
   parent was upgraded. Do not edit hashes or reviewed versions merely to make
   the check pass.
3. Install the selected dependency graph with `npm ci`, then run the relevant
   focused regressions and `npm run check`. Record configured live checks and
   explicit skips using the [qualification procedure](qualification.md).
4. Require the ordinary PR CI gate before merging. Changes affecting packaged
   dependencies also need the relevant native/package evidence before
   [release draft review](releases.md#review-and-publish-the-draft).

The root [dependency policy](../tools/check/dependencies.ts) checks exact direct
versions, lockfile agreement, registry integrity and reviewed licenses. Its
offline maintenance check requires one reviewed entry per active mitigation and
checks every matching root or nested lockfile instance. Review dates record when
evidence was assessed; they are not a claim that upstream status can never change.
This root-lock policy does not validate the separate promotional-media dependency
graph under `website/promo`.

### Qualify the dependency graph that changed

The fixture and promotional tooling have separate installation inputs. Passing
root checks alone does not qualify an update to either graph:

- **Kafka OAuth fixture:** rebuild the [OAuth image](../aio-kafka/images/oauth-service/Dockerfile)
  from its digest-pinned Python base and exact requirements, then run `pip check`
  inside that image. Exercise valid and invalid credentials against the running
  service and verify its RS256 token through its published JWKS, including the
  expected issuer, audience and expiry. Record the actual image ID and input
  hashes; tests against a previously running image do not validate changed pins.
- **Promotional media:** install the separate lock with
  `npm --prefix website/promo ci`, then exercise its native image processing and
  render a bounded sample with the installed renderer. Verify decoded frames,
  dimensions and duration. `npm run docs -- qualify` checks the documentation and
  existing media playback; it does not install or execute the promotional
  renderer. A sample render qualifies the changed tooling, not a new public video.

Record the tested platform and dependency versions, relevant audit findings and
any untested platforms. Keep disposable qualification outputs separate from
approved documentation assets.

## Automated proposals

[Dependabot configuration](../.github/dependabot.yml) checks weekly for updates:

| Dependency graph    | Proposal scope                                                                                                             |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Root npm            | Separate production and development minor/patch groups; majors and the four mitigated packages remain outside those groups |
| `website/promo` npm | Promotional-media tooling, separate from the root npm graph                                                                |
| GitHub Actions      | Repository workflows                                                                                                       |
| Python              | Documentation toolchain and the Kafka fixture's OAuth service                                                              |
| Go                  | EDA capture agent; currently no third-party module requirements                                                            |

Each entry limits open version-update PRs. The four mitigations are excluded from
routine groups, not ignored. A parent update can still change a mitigated nested
package, so the offline validator remains necessary. Group rules apply only to
version updates; they do not group security updates. These behaviors use the
[standard Dependabot options](https://docs.github.com/en/code-security/reference/supply-chain-security/dependabot-options-reference).

Repository dependency alerts and automatic security-update proposals are separate
GitHub settings. The configuration file does not establish that either is enabled;
maintainers must verify them in the repository's security settings. Security
proposals still require review and the normal CI gate. See
[GitHub's security-update configuration](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/secure-your-dependencies/configure-security-updates).

## Active mitigations

The [maintenance inventory](../tools/check/dependency-maintenance.ts) is the
authoritative record of reviewed versions, owners, dates, upstream evidence and
retirement criteria. All four entries below were reviewed on **2026-10-08** and
are owned by **@asadarafat**. No newer upstream candidate has been accepted by
this review.

| Package                         | Scope                       | Reviewed upstream status                                                                                                                                                                                                                                         | Implementation and acceptance                                                                                                                             |
| ------------------------------- | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `node-forge@1.4.0`              | Runtime truststore handling | Proposed fix: [RSA validation PR](https://github.com/digitalbazaar/forge/pull/1152), [advisory](https://github.com/advisories/GHSA-86w9-cpqp-85rv)                                                                                                               | [Backport](../tools/check/forge-patch.ts), [signature regression](../test/integration/forge-security-patch.test.ts)                                       |
| `braces@3.0.3`                  | Development/build tooling   | Reported; no upstream replacement qualified: [nesting issue](https://github.com/micromatch/braces/issues/70), [advisory](https://github.com/advisories/GHSA-vfj7-8cjw-p6xm)                                                                                      | [Exact-source mitigation](../tools/check/build-dependency-patch-data.ts), [behavior regressions](../test/unit/build-dependency-patches.test.ts)           |
| `http-cache-semantics@4.2.0`    | Development/build tooling   | Disputed upstream advisory; StreamSkope retains its stricter cache policy: [discussion](https://github.com/kornelski/http-cache-semantics/issues/56), [advisory](https://github.com/advisories/GHSA-ch52-4w7c-c8xp)                                              | [Policy correction](../tools/check/build-dependency-patch-data.ts), [cache regressions](../test/unit/build-dependency-patches.test.ts)                    |
| `@nats-io/transport-node@3.4.0` | Runtime NATS transport      | Local lifecycle and TLS identity correction: [pending connection issue](https://github.com/nats-io/nats.js/issues/435), [reviewed source](https://github.com/nats-io/nats.js/blob/95e76e79d9feaa0a0bf3b0e8da526ec5a3460979/transport-node/src/node_transport.ts) | [Exact-source corrections](../tools/check/runtime-dependency-patch-data.ts), [public SDK regressions](../test/integration/nats-sdk-runtime-patch.test.ts) |

### Forge security backport

The RSA validation backport uses upstream commit
`ceba34402e329f0365134f23fe19898756527d65`. It preserves the real package version,
registry integrity and licenses. `dev`, `build`, `check` and `package` apply it
before using application dependencies, including fresh production installs for
native packaging. Packaging verifies the patched bytes after extracting the
final ASAR and records the hash. Unknown versions, changed files, missing copies
and unlisted resolutions fail verification.

Raw `npm audit` can still report an advisory because a local backport does not
change the upstream version. The [audit checker](../tools/check/audit.ts) accepts
only the reviewed finding after verifying every affected installed copy. It
accepts indirect findings only when all causes are accounted for; other
high/critical findings, malformed reports and audit-service failures fail CI.
[Audit regression tests](../test/unit/dependency-audit.test.ts) qualify that
boundary separately from the RSA behavior and existing JKS/PKCS12 tests.

### Build dependency mitigations

The braces change bounds nested parsing and recursive AST walkers while retaining
ordinary expansion. The cache change requires revalidation when cache policy
forbids storage or reduces freshness to zero, even if a client permits stale
responses. Positive-lifetime public entries retain permitted stale reuse.

The published `http-cache-semantics@4.3.0` candidate was tested on 2026-10-08 and
still allowed the cookie, no-cache, no-store and private cases that StreamSkope's
regression policy rejects. Its existence is not sufficient to retire the local
correction. This records the project's acceptance decision, not a resolution of
the disputed upstream advisory. The candidate is available from the
[official npm registry](https://registry.npmjs.org/http-cache-semantics/4.3.0).
A deliberate change to that cache policy requires explicit review of its behavior,
mitigation and audit accounting; a version bump alone does not authorize it.

`check`, `build` and native `package` apply the pinned build changes and verify
package identity, development-only classification, resolution and complete file
hashes. Repeated application is idempotent. Both packages remain excluded from
production dependencies. Their exact advisory/version/path accounting does not
exempt new findings or unverified copies from the complete audit.

### NATS runtime corrections

The correction retains ownership of a socket as soon as dialing begins and
destroys it when setup closes before INFO/TLS completes. It preserves error
observation and fallback to another server. TLS upgrade uses the selected DNS
name or IP as the verification host and retains DNS SNI; profiles cannot disable
certificate verification. These are runtime behavior corrections and grant no
npm-audit exemption.

`dev`, `check`, `build` and `package` verify the allowed version, registry
integrity, consumer resolution and complete source hashes. The independent
[socket probe](../test/support/nats-sdk-socket-probe.mjs) and
[TLS probe](../test/support/nats-sdk-tls-probe.mjs) exercise public SDK behavior;
qualification also exercises the SDK in the bundled host.

## Retire a mitigation

Use an unpatched official candidate in a disposable checkout and preserve its
version, integrity and actual test results. A closed issue or newer version is
not acceptance evidence. Require the relevant existing behavior before removing
the mitigation:

- **Forge:** reject malformed RSA signatures containing extra DigestAlgorithm
  children while retaining valid signatures and truststore behavior.
- **Braces:** bound deeply nested parse, compile, expand and stringify operations
  while retaining ordinary expansion.
- **Cache policy:** refuse stale revival of cookie, no-cache, no-store and private
  entries while retaining permitted public stale reuse.
- **NATS:** release pending INFO/TLS setup sockets, preserve server fallback and
  enforce DNS/IP TLS identity and authentication through the public SDK.

Upgrade all affected direct and transitive copies together. Remove the retired
patch data, application hooks, maintenance entry and any corresponding audit
exception in the same reviewed change; retain other active mitigations and their
audit checks. Remove that package from the routine-group exclusions once its
special handling is gone. Keep the independent behavior regressions, adapting
them to test the upstream implementation without requiring obsolete patch bytes.
Run full qualification and relevant package checks before claiming retirement.
