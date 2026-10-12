# Documentation review rubric

Read the sections relevant to the selected scope, following [SKILL.md](../SKILL.md).
Framework roles and source limitations are in [standards](standards.md).
All IDs, thresholds, and evidence requirements below are local conventions.

## Assessment conventions

Evaluate a reader need, not a page count. Apply a rule only when its applicability
condition holds. Record evidence before concluding that there is a defect.
Use **Meets reviewed criteria / Partial / Material gaps / Not applicable /
Not assessed** for each framework, with positive evidence or finding IDs.
“Meets” is bounded by the sample; “Not assessed” means insufficient inspection;
“Not applicable” needs a reason, such as no architecture question in scope.

| Severity | Reader impact | Calibration |
| --- | --- | --- |
| P0 — Critical | Credible risk of data loss, credential exposure, or similarly severe consequences. | Identify a plausible action and consequence supported by evidence; hypothetical danger alone is insufficient. |
| P1 — High | Blocks a core task or materially misrepresents important behavior. | Examples include a required unsupported flag or incorrect access requirements. |
| P2 — Medium | Significant confusion, extra work, or poor discovery. | Examples include a hidden prerequisite or a task path that repeatedly sends readers to unrelated content. |
| P3 — Low | Local editorial or presentation improvement. | A vague label with an otherwise clear path is usually here, not P1. |

Confidence is separate: **high** = direct, consistent evidence for the claim;
**medium** = supported inference with a stated gap; **low** = tentative hypothesis.
Unconfirmed risks usually belong under remaining verification rather than being
asserted as defects. Static source evidence can yield high confidence about a
document/source contradiction without establishing runtime success.

Effort is relative to the repository: **small** = localized correction; **medium**
= coordinated edits or a focused new explanation; **large** = substantial content
or publication restructuring. Give a reason, dependencies, and testable acceptance
criteria. Do not attach invented time estimates or fixed finding quotas.

For each finding use the complete record in [report-template](../assets/report-template.md).
Consolidate the same problem across frameworks; e.g. an incorrect default can map
to DX-03, IBM-02, and ENG-01 but remains one finding.

## Diátaxis: purpose and information architecture

| Local rule | Applies when | Observable evidence to inspect | Common false positives / boundary |
| --- | --- | --- | --- |
| DX-01 — Learning path | Content promises a beginner lesson or learning experience. | Defined outcome, usable example inputs and prerequisites, manageable steps, checkpoints and recognizable results; necessary cleanup; choices or theory that interrupt learning. | A small contextual explanation is useful. Not every how-to must teach a novice. |
| DX-02 — Task completion | A page promises to solve a recognizable real-world problem. | Prerequisites/assumptions, adaptable actions, relevant choices, verification and recovery; warnings beside the consequential action. | Do not demand tutorial pacing or cleanup when no resources are created. |
| DX-03 — Reliable lookup | Readers consult API, CLI, configuration, protocol, or other reference facts. | Predictable structure and precise behavior; applicable parameters, types, defaults, units, limits, permissions, outputs and errors; facts accessible without following a lesson. | Short usage examples are legitimate. A coherent long reference with good anchors need not be split. |
| DX-04 — Understanding | A page promises concepts, rationale, or architecture explanation. | Relationships, constraints, alternatives and trade-offs that help the reader reason about the system. | Instructions alone may miss the goal; explanations need not become procedures. An evidence-backed perspective is allowed. |
| DX-05 — Purpose interference | A page or section serves multiple needs. | Identify the specific transition where a competing purpose impedes the primary reader goal; show extra decisions, interrupted sequence, or buried facts. | Multiple headings, brief context, or examples are not evidence of interference. README overviews, hubs, release notes, governance, licenses and policies need no forced type. |
| DX-06 — Discovery and connections | Assessing navigation or tracing a journey. | Entry points lead to appropriate help; related tasks, learning, facts and concepts are linked where useful; identify actual dead ends or irrelevant detours. | Four named folders are not proof of quality. Feature-based navigation and unequal quantities of the four types can be appropriate. |

**Proportionate example:** if a lesson interrupts its first action with an
independent 60-option catalogue, keep the defaults needed for the lesson and link
to an existing or focused reference. Do not split merely because the page is long.

## Google writing conventions, supported by Technical Writing courses

| Local rule | Applies when | Observable evidence to inspect | Common false positives / boundary |
| --- | --- | --- | --- |
| G-01 — Audience and terminology | All authored prose. | Consistent terms, explained unfamiliar abbreviations, vocabulary matched to reader knowledge, clear subjects/verbs/pronoun references. Quote a brief concrete ambiguity and explain its cost. | Domain terms and project spellings can be correct. Do not flatten precise terminology or apply English grammar to translations. |
| G-02 — Direct, ordered instructions | Procedures and actionable guidance. | Clear actor/action, prerequisites and conditions before dependent actions, logical ordering, identified optional steps and meaningful outcomes. | Passive voice is sometimes clearest. A long sentence is not inherently a defect; diagnose comprehension, not word count. |
| G-03 — Headings and links | Titles, sections, cross-references. | Descriptive titles, coherent hierarchy, meaningful link text, clear destination and purpose. | House-style capitalization is a justified exception when documented; not every repeated “Learn more” blocks a task. |
| G-04 — Interface and example formatting | UI labels, commands, code, placeholders, samples. | Identifiers match the product; UI/code formatting consistent with house style; substitutions explained; relevant expected output and interpretation supplied. | Reference examples need not become full lessons. Formatting changes must not alter a literal identifier or runnable syntax. |
| G-05 — Accessible, inclusive language | Authored prose and descriptions of visuals/UI. | Instructions do not depend only on color or position; informative link and image descriptions; unnecessary idioms or exclusionary language identified in context. | Do not demand alt text for purely decorative images or infer actual contrast/keyboard failure from source alone; rendered checks are ENG-04. |

**Proposed rewrite example, not an implementation claim:** if evidence shows an
operator must restart a worker, replace “It should then be restarted” with
“Restart the worker after saving the configuration.” Do not invent a restart
command or order of operations; verify those independently.

## C4: relevant architecture views

First identify the audience's architecture question. Use this section for static
architecture diagrams and appropriate supporting views; a screenshot or sequence
diagram is not a failed C4 diagram. Recommend a missing view only to resolve an
identified understanding gap. Mark C4 not applicable otherwise.

| Local rule | Applies when | Observable evidence to inspect | Common false positives / boundary |
| --- | --- | --- | --- |
| C4-01 — View and scope | An architecture view is relevant. | Title, scope, understandable abstraction, identifiable system boundary; views answer the audience's question. | No requirement for all four levels. Context/container views can be enough. Do not invent components for a template. |
| C4-02 — Elements | The view contains people or software elements. | Names, roles/responsibilities, understandable type and ownership; appropriate technology detail. | A C4 container is an application or data store boundary, not necessarily a Docker container. A package directory alone does not establish runtime structure. |
| C4-03 — Relationships and notation | The view depicts communication or dependencies. | Intent labels, correct direction, appropriate protocols, explainable shapes/colors/lines and a legend when needed. | Avoid implementation detail in a system-context view. A self-explanatory diagram does not need a large decorative legend. |
| C4-04 — Coherence and useful coverage | Comparing architecture claims with prose/source or evaluating an architecture gap. | Consistent names/boundaries/relationships; deployment and dynamic views where placement or runtime ordering cannot otherwise be understood. Cross-check relevant source and record uncertainty. | No demand for a particular syntax or tool. Intentional conceptual simplification is legitimate. Source inspection does not establish rendered readability. |

## IBM-informed cross-cutting quality

Apply these lenses across the reviewed corpus and reader journeys; do not generate
nine findings per page. These are local repository checks informed by the nine
named characteristics, not claims about uninspected chapters of the book.

| Local rule / characteristic | Applies when | Observable evidence to inspect | Common false positives / boundary |
| --- | --- | --- | --- |
| IBM-01 — Task orientation | Evaluating a page's contribution to a reader need. | A recognizable purpose and information that advances it. | Understanding and lookup are legitimate needs; do not turn all prose into steps. |
| IBM-02 — Accuracy | Content makes consequential factual claims. | Agreement with same-version interfaces, configuration, tests, release evidence and clearly identified runtime observations. | Source/tests are evidence, not deployment proof; development docs can intentionally differ from released docs. |
| IBM-03 — Completeness | The reader is promised a bounded outcome. | Information necessary to reach and recognize that outcome, including important prerequisites and limits. | Completeness is relative to scope/audience; no demand to document every internal function. |
| IBM-04 — Clarity | Wording or organization could change interpretation. | Unambiguous actor, action, state and terminology; a concrete confusing passage. | Difficult subject matter is not itself bad writing. |
| IBM-05 — Concreteness | Abstract claims or examples guide decisions. | Useful example inputs, specific outputs, failure symptoms or measurable bounds supported by evidence. | Do not invent measurements; conceptual explanation can legitimately be abstract. |
| IBM-06 — Style | Comparing reader-facing conventions. | Consistent, appropriate editorial patterns and documented exceptions. | Style differences with no reader cost are low priority; generated/vendor text may have another owner. |
| IBM-07 — Organization | Assessing order and relationships of information. | Coherent sequence, grouping, cross-links and manageable transitions. | Page length, feature grouping, or lack of four folders is not a defect by itself. |
| IBM-08 — Retrievability | Following a lookup or navigation journey. | Useful titles, terms, entry links, anchors, index/search behavior if actually inspected. | A page absent from top-level navigation can still be readily discoverable. Do not claim search tested from configuration alone. |
| IBM-09 — Visual effectiveness | A visual contributes to meaning. | Legibility at intended size, useful hierarchy/labels, text alternatives, appropriate contrast and information conveyed. | If only source is read, limit claims to its content; mark visual rendering not assessed. Decorative art need not teach architecture. |

## Optional DITA-inspired reuse and maintenance

| Local rule | Applies when | Observable evidence to inspect | Common false positives / boundary |
| --- | --- | --- | --- |
| REUSE-01 — Coherent topics | Readers enter through direct links or search. | Informative title, purpose and enough context to understand the topic independently; useful links to prerequisites. | Self-contained does not mean repeating an entire prerequisite guide. |
| REUSE-02 — Canonical facts | Several maintained locations repeat changing facts or instructions. | Conflicting versions/defaults/steps; an identified owner or reusable source; smallest workable way to prevent drift. | Beneficial repetition, especially critical warnings, should remain. Reuse machinery may cost more than a small duplication. |
| REUSE-03 — Content and navigation | Topics are reused or navigation is being changed. | Stable destinations, includes with coherent context, organization separable from content where useful. | No requirement for XML, DITA maps, micro-pages, or a publishing migration. |

## Additional engineering recommendations

These checks complement the frameworks; they are not official Diátaxis, Google,
C4, IBM, or DITA requirements.

| Local rule | Applies when | Observable evidence to inspect | Common false positives / boundary |
| --- | --- | --- | --- |
| ENG-01 — Technical traceability | Commands, schemas, defaults, APIs, examples or security behavior matter to the journey. | Exact doc claim versus same-version source, fixtures/tests, and any executed check; permissions, destructive effects and runtime boundaries. | Plausible syntax is insufficient. A test not run is only inspected evidence. Undocumented does not mean unimplemented. |
| ENG-02 — Version and generation | Docs reference releases, compatibility or generated content. | Declared version/source commit, generation input, release metadata, publication process; intentional source/site differences. | A release tag existing alone does not prove that a site was built from it. Generated pages can be valid; recommend fixes at their maintained source. |
| ENG-03 — Link integrity | Following links in the reviewed journey. | Local targets/anchors, include and redirect resolution, or permitted remote status and actual response content. | 403, timeout or authentication means unverified. A plain Markdown checker may not resolve site-specific routes; a redirect is not necessarily broken. |
| ENG-04 — Rendered usability | Site appearance, accessibility, diagrams, screenshots or navigation behavior is in scope. | Actual rendered inspection and viewport; readable tables/code, usable navigation, diagram size, media alternatives, relevant keyboard behavior. | Static HTML/CSS inspection is not a visual or accessibility test. No screenshot or browser available means not assessed, not failed. |
| ENG-05 — Documentation checks | Evaluating correctness assurance or maintenance. | Existing build/lint/link/example checks, what they validate, event/version coverage, generated drift risks, known unchecked consequential claims. | A green build does not prove accuracy. No dedicated docs CI is not automatically a defect; recommend a proportional check for an observed risk. |

## Finding quality gate

Before finalizing each finding, confirm an affected audience and goal, a precise
location, concrete evidence, reader impact, an applicable local rule, the smallest
useful change, and observable acceptance criteria. Separate assumptions and
unexecuted checks. For missing content, list search locations and describe the
gap only within that coverage. Do not inflate style concerns to fill priorities.
