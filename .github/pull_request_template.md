## Outcome

<!-- State the change and why it matters. For changed behavior, include a concrete acceptance example. -->

<!-- Use a Conventional Commit title that describes the outcome. Review release-note labels: component:desktop, component:eda, component:nsp, or component:shared; multiple are allowed. Without labels, known plugin-owned files select those plugins and other paths count as shared; CONTRIBUTING.md defines the mapping. Use release-notes:skip only for a deliberate changelog omission. Labels never bypass CI or assign versions. Add short upgrade/limitation notes to the relevant component's unreleased commentary when needed. -->

## Verification

<!-- List checks actually run and their results; identify any important unverified behavior. Run `npm run check` locally and report its result, including any live EDA/NSP skip. GitHub qualifies source/docs on every PR and main push. The separate manual Release workflow assigns the selected component/version in its build checkout, then verifies native desktop packages plus the unsigned EDA application, or the selected plugin. -->
