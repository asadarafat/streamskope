## Outcome

<!-- State the change and why it matters. For changed behavior, include a concrete acceptance example. -->

<!-- Use a Conventional Commit title that describes the outcome. Review release-note labels: component:desktop, component:eda, component:nsp, or component:shared; multiple are allowed. Without labels, known plugin-owned files select those plugins and other paths count as shared; maintainers/releases.md defines the mapping. Use release-notes:skip only for a deliberate changelog omission. Labels never bypass CI or assign versions. Pending changes are derived from merged PRs and published component baselines; add short reviewed upgrade/limitation highlights to the relevant component commentary when needed. -->

## Verification

<!-- List checks actually run, their results and material unverified scope. Run focused local checks for changed behavior; duplicate full local qualification is not mandatory for every PR. Follow maintainers/qualification.md#choose-evidence-by-impact for additional real-provider, security, native and soak checks. Run selected live EDA/NSP checks when integration, authentication, trust, generated profiles or connection/lock/cleanup behavior affects them, and for milestone/release integration claims. `npm run check` is core only; `--full` adds configured vendors; `--live eda|nsp|all` requires each selected vendor's configuration. Report unselected, skipped and failed scopes honestly, and link distinct receipts rather than combining or relabeling them. Every PR still requires shared, docs, runtime and final CI checks; main does not repeat them. Release CI qualifies its source again before packaging. Record actual native architectures and package/Containerlab scenarios when relevant; publication is separate evidence. -->
