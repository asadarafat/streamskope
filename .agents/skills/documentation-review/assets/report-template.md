# Documentation review: {scope}

Template instructions: replace braces with actual evidence. Scale detail to
quick/standard/deep scope; keep the eight sections, but use brief “not applicable”
or “not assessed” explanations where appropriate. Never pad findings or invent
data to fill a table. Return in conversation unless a report destination is
explicitly requested/authorized. Do not create this file in the reviewed project
as a side effect of using the template.

## 1. Executive summary

{Overall assessment bounded by the reviewed scope, not an official compliance
claim. Strengths worth preserving, with evidence. The three most valuable
improvements or fewer; no improvements is a valid outcome.}

## 2. Scope and evidence

- **Revision/worktree:** {commit ID or limitation; relevant staged/unstaged/untracked changes}
- **Comparison, if any:** {resolved base/head IDs, merge-base or direct diff semantics; treatment of local changes}
- **Product/documentation version:** {per component as needed; evidence or unknown}
- **Source versus site:** {paths inspected; published URLs/version checked, or source-only}
- **Depth/audiences:** {requested depth and stated/inferred audiences}
- **Policies:** {terminology/house-style source and justified exceptions}
- **Boundaries:** {generated inputs/output, archives, translations, third-party content}

| Coverage                           | Count or bounded inventory                      | Selection rationale    |
| ---------------------------------- | ----------------------------------------------- | ---------------------- |
| Discovered                         | {count with discovery method/roots, or unknown} | {inventory boundaries} |
| Deeply reviewed                    | {distinct pages and paths}                      | {risk/reader value}    |
| Section-only sample                | {pages and section headings}                    | {why partial}          |
| Excluded / discovered but not read | {count/categories or paths}                     | {reason}               |

Keep coverage categories non-overlapping, or explicitly explain overlaps. Do not
claim “all pages” from a glob count or extrapolate a sample's pass to the corpus.

| Journey   | Reader goal and entry path          | Mode and outcome                                              |
| --------- | ----------------------------------- | ------------------------------------------------------------- |
| {journey} | {audience, start, transitions, end} | {document walkthrough / executed validation; observed result} |

{Checks executed and checks not performed; assumptions; unavailable versions,
environments, rendering, network access or other limitations.}

## 3. Framework assessment

| Framework               | Assessment | Findings or positive evidence                                           |
| ----------------------- | ---------- | ----------------------------------------------------------------------- |
| Diátaxis                | {status}   | {evidence}                                                              |
| Google writing guidance | {status}   | {evidence and exceptions}                                               |
| C4                      | {status}   | {needed views or why not applicable; source versus rendered inspection} |
| IBM-informed quality    | {status}   | {relevant characteristics and evidence}                                 |
| Optional reuse checks   | {status}   | {evidence or applicability reason}                                      |

Use only **Meets reviewed criteria / Partial / Material gaps / Not applicable /
Not assessed**. These are local assessments, not framework certification or
numeric scores. Assess engineering checks separately under validation.

## 4. Page-purpose map

| Current location / section                       | Audience | Reader goal | Primary type                                                    | Competing purpose and discovery route       | Recommended disposition                         |
| ------------------------------------------------ | -------- | ----------- | --------------------------------------------------------------- | ------------------------------------------- | ----------------------------------------------- |
| {path and actual line range, or URL and heading} | {role}   | {need}      | {tutorial / how-to / reference / explanation / named exception} | {observed interference or none; entry link} | {keep / tighten / link / justified restructure} |

## 5. Prioritized findings

### DR-001 — {concise problem}

- **Severity / confidence:** {P0–P3 with impact; high/medium/low with reason}
- **Origin, for change reviews:** {new / pre-existing / origin unverified; comparison evidence}
- **Audience and goal:** {who is trying to do what}
- **Location and evidence:** {exact paths and actual line ranges; URLs and headings;
  short observed passage; implementation or validation support where applicable}
- **Evidence class and limits:** {observed documentation / implementation-supported /
  executed validation / reasoned recommendation / unverified assumption; contradictions}
- **Framework / local rule:** {one or more applicable IDs from the rubric}
- **Why it matters:** {reader consequence, not just a stylistic preference}
- **Recommended change:** {specific, smallest useful action; proposal, not an edit already made}
- **Acceptance criteria:** {observable corrected state and how to verify it}
- **Effort / dependencies:** {small/medium/large; brief reason and prerequisites}

{Repeat for distinct root causes, in severity order, retaining stable IDs across
iterations where possible. If no findings, state that within the reviewed scope.
Keep unresolved verification questions separate from asserted defects.}

## 6. Improvement plan

| Phase                         | Finding IDs / proposed action | Affected files | Dependencies                        | Acceptance criteria / effort |
| ----------------------------- | ----------------------------- | -------------- | ----------------------------------- | ---------------------------- |
| Immediate                     | {small useful fixes}          | {paths}        | {if any}                            | {observable completion}      |
| Next increment                | {coordinated improvements}    | {paths}        | {evidence or prior work needed}     | {observable completion}      |
| Structural, only if justified | {identified reader benefit}   | {paths/owners} | {links/redirects/versions affected} | {observable completion}      |

{Omit inapplicable rows; do not recommend a platform migration or rewrite without
evidence. Explain the priority order by reader impact and dependencies.}

## 7. Representative improvements

{Zero to three justified examples. Label each “Proposed wording” or “Proposed
outline”; tie it to a finding. Cite the original before text or structure. Preserve
technical meaning, warnings and literal identifiers. Mark unresolved values as
unverified placeholders; never present invented runnable commands as validated.}

## 8. Validation and sources

| Check   | Method / environment / revision                         | Result and what it establishes         |
| ------- | ------------------------------------------------------- | -------------------------------------- |
| {check} | {read-only inspection, actual command or rendered view} | {observed result; untested boundaries} |

- **Remaining verification:** {unrun runtime steps, unresolved contradictions, access limitations}
- **Scope preservation:** {review-induced changes, if any; authorized report path only}
- **Standards:** {primary links from standards.md, applied principles, fresh checks
  versus bundled offline guidance; do not claim inaccessible material was read}
- **Project exceptions:** {policy source, justification and assessment effect}
