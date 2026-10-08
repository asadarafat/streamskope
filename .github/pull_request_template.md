## Outcome

<!-- State the change and why it matters. For changed behavior, include a concrete acceptance example. -->

<!-- Use a Conventional Commit title that describes the outcome. Review release-note labels: component:desktop, component:eda, component:nsp, or component:shared; multiple are allowed. Without labels, known plugin-owned files select those plugins and other paths count as shared; maintainers/releases.md defines the mapping. Use release-notes:skip only for a deliberate changelog omission. Labels never bypass CI or assign versions. Pending changes are derived from merged PRs and published component baselines; add short reviewed upgrade/limitation highlights to the relevant component commentary when needed. -->

## Verification

<!-- List checks actually run and their results; identify important unverified behavior. Run `npm run check` locally and report any live EDA/NSP skip. GitHub qualifies shared, documentation and runtime lanes on every PR; merging to main does not repeat CI. Manual Release qualifies its selected source again and assigns the component/version only in its build checkout. Desktop release checks cover three native installers, native Linux AMD64/ARM64 browser archive and installer lifecycles, and the separate unsigned EDA artifact; plugin releases check the selected plugin. For browser/container changes, record the tested architecture and any Containerlab, broker or live plugin checks actually run. Report skips explicitly. -->
