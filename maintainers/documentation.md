# Maintain the documentation

Edit operator content in `website/docs/` and maintainer procedures in
`maintainers/`. Use the source preview to review a change, then qualify it before
requesting a merge. Publication follows the [release procedure](releases.md#publish-and-synchronize-documentation);
a successful preview build does not update GitHub Pages.

## Preview and qualify

Prepare the [checkout](development.md#prepare-a-checkout) with Node 24.x and
`npm ci`, plus Python 3.11 or newer with venv support. The docs command installs
the exact [Python requirements](../website/requirements.txt) into
`.cache/zensical/` and refreshes them when their fingerprint changes. A global
Zensical installation is unnecessary.

Install the browser engines for local qualification once:

```sh
npx --no-install playwright install chromium firefox
```

On Linux, install the Playwright system libraries if they are missing. Chromium
owns routine browser checks; Firefox owns actual intro-video playback.

| Command                                              | Expected result                                                                                                         |
| ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `npm run docs -- serve`                              | Development preview at `http://127.0.0.1:8002/`; stop with Ctrl+C.                                                      |
| `npm run docs -- serve --host 127.0.0.1 --port 8003` | Preview on a different unused port.                                                                                     |
| `npm run docs -- build`                              | Clean strict build in `dist/site/`, with rendered local link, anchor and asset checks.                                  |
| `npm run docs -- check`                              | Inspect an existing `dist/site/`; this does not rebuild or run browser tests.                                           |
| `npm run docs -- qualify`                            | Source-policy checks, documentation tests, strict build, Chromium navigation/accessibility and selected media playback. |
| `npm run docs -- qualify --media`                    | The same qualification with full intro playback required.                                                               |

For a browser outside the development host, choose a host interface it can reach
using `--host`; loopback refers to the machine running the server. Do not assume
that a personal VM hostname works for other contributors.

Qualification checks tracked Markdown npm commands, documented message limits
against executable contracts, and the secret-retrieval guide's required actions
and UI labels. It also checks source/release metadata and runs the Python and
JavaScript tests in `test/docs/`. A page rendering successfully alone does not
establish that its instructions match the product.

## Source ownership

| Location                                                                                      | Responsibility                                                                        |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| [website/docs](../website/docs)                                                               | Operator guides, release archives and approved media.                                 |
| [website/zensical.toml](../website/zensical.toml)                                             | Navigation, theme and shared site configuration.                                      |
| [website/overrides](../website/overrides) and [docs assets](../website/docs/assets)           | Templates, notices, styling and browser behavior.                                     |
| [tools/docs.py](../tools/docs.py) and [tools/docs](../tools/docs)                             | Build, publication preparation, policy checks, browser qualification and evidence.    |
| [test/docs](../test/docs)                                                                     | Regression tests for content rules, publication selection and accessibility adapters. |
| [website/requirements.txt](../website/requirements.txt) and [website/promo](../website/promo) | Pinned site dependencies and the separate promotional renderer dependency graph.      |

## Write or update a guide

Start with the operator's task, prerequisites, actions and expected result. Include
the relevant failure or cleanup path. Use the product's current UI labels and
verify the sequence against the owning implementation or a recorded rehearsal.
Keep provider selection in Connection Profiles and call the product's NATS
support **NATS**; explain core subscriptions versus JetStream only where that
technical distinction affects the task.

Link to the existing [security](../website/docs/guide/security.md),
[recovery](../website/docs/guide/recovery.md) and
[data-handling](../website/docs/guide/data-handling.md) references instead of
copying their procedures and limits into each feature guide. Keep plugin-specific
instructions under the [plugin guides](../website/docs/plugins). Add a navigation
entry for a new operator page, preserve useful bookmarks, and check the narrow
layout as well as both themes.

Record the actual source, target, actions, result and limits of an operator
rehearsal. Link the exact evidence from the
[qualification page](../website/docs/guide/qualification.md); do not replace a
historical failure with a current success or present source-only checks as proof
for an installed release. Site browser checks do not establish broker permissions,
backup recovery or live integration.

## Version and plugin notices

Local and PR previews identify source instructions as unreleased. A stamped
candidate is still a preview. Published documentation uses the release's immutable
source and its prepared download baseline; it must not label newer main as an
older released product. Publication preparation changes its disposable checkout,
not main or the source tag.

Only the development release-notes page uses generic `unreleased` metadata; that
page is removed before publication. Plugin procedures use `plugin_scope: eda`,
`nsp` or `all`. Requirements come from the release checkout's plugin manifests.
Keep the installation page's download marker so generated links follow the
prepared `project.extra.desktop_release` value.

Publication preparation verifies compatible packages against the production
catalog and saves a timestamped, source-bound availability snapshot. The build
reuses that snapshot. A catalog failure stops preparation. Local previews stay
offline and say availability was not checked; they do not claim no package exists.
Catalog availability does not prove every source procedure shipped in the selected
plugin or that live behavior passed. See [plugin versioning](../website/docs/plugins/versioning.md)
for the compatibility contract and [releases](releases.md) for publication ownership.

## Search and accessibility

The pinned Zensical search widget has a small
[compatibility adapter](../website/docs/assets/search-accessibility.js) for
accessible control names, combobox selection, contrast and focus handling. Recheck
it when updating Zensical. Browser qualification exercises keyboard selection,
filters, closing and reopening on desktop/mobile in both themes.

The checks require zero accessibility findings, including the search dialog;
there are no widget or rule exemptions. Inspect
`.artifacts/website/search-accessibility.json` and the captured pages when a check
fails. Automated results do not establish full WCAG conformance.

## Screenshots and media

For app screenshots, first prepare the real AIO Kafka fixture using the
[development guide](development.md#run-the-workbench-with-kafka-and-nats). Then run:

```sh
node --import tsx tools/docs/capture.ts --profiles-only
```

Omit `--profiles-only` only after preparing the broader capture's curated
`orders.events` record (`ord-1042`) and `orders-workers` group state required by
[the capture assertions](../tools/docs/capture.ts). Starting the default fixture
alone does not create that curated state. The tool captures the actual
connected app in both themes and copies images to `website/docs/assets/` only
after the requested views succeed. It writes capture details under
`.artifacts/website/aio-captures/`. The profiles capture records source and image
digests; retain the exact revision and relevant capture evidence in the review
for every refreshed asset. Inspect the images before committing them. Failed
capture is not permission to substitute fabricated product state.

Routine docs qualification plays the existing approved video masters when media,
presentation or toolchain inputs change, when CI cannot determine the comparison
base, or with `--media`. Local runs cache a successful fingerprint; unchanged
prose still gets routine checks. CI installs Firefox when playback is selected.
The separate **Launch video** workflow remains a manual media utility.

Playback does not install or execute `website/promo`'s renderer. When that graph
changes, follow [dependency-graph qualification](dependencies.md#qualify-the-dependency-graph-that-changed)
and render a bounded sample with its actual dependencies. Keep sample output
separate from approved public media. A sample tooling pass is not a new public
video's acceptance.

## Evidence and publication

Successful qualification writes `.artifacts/website/qualification.json` and
`browser-checks.json`, with explicit media `passed` or `skipped` outcomes and
browser evidence hashes. Screenshots and search evidence remain alongside them.
The built site's `documentation.json` records its publication identity.

Use the [qualification procedure](qualification.md) to retain source-bound
acceptance and its limits. Follow the [release publication procedure](releases.md#publish-and-synchronize-documentation)
for Pages and archive handoffs. Inspect a failed publication before retrying its
exact release; a successful build of current main cannot repair an older release's
identity.
