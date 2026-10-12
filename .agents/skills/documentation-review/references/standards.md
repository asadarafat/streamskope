# Standards, provenance, and boundaries

Read before applying the rubric. These are original working summaries, not
reproductions of the guides or official certification criteria. Source checks
below were performed on **2026-10-11**. Future reviews must report their own access
and version limitations rather than inheriting that date as fresh verification.

## Division of responsibility

| Framework                                  | Role in this skill                                             | Boundary                                                                              |
| ------------------------------------------ | -------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Diátaxis                                   | Identify the reader need and suitable documentation treatment. | Does not require four folders, balanced page counts, or a site-wide reorganization.   |
| Google developer documentation style guide | Evaluate editorial conventions.                                | Respect documented terminology and justified house style; preserve technical meaning. |
| Google Technical Writing courses           | Support audience analysis and clear technical prose.           | Teaching material, not a mandatory style checklist or a claimed completed course.     |
| C4                                         | Assess architecture views that help a real audience.           | Does not govern every visual or require every abstraction level.                      |
| IBM-informed content quality               | Assess usefulness across nine complementary characteristics.   | No certification, numeric score, or mandate to make all content procedural.           |
| DITA-inspired principles, optional         | Improve coherent topics, reuse, and maintenance.               | Supplement Diátaxis; do not require XML, DITA maps, or platform migration.            |

Technical accuracy and user safety outrank stylistic uniformity. The skill's
rule IDs, severity/confidence scales, sampling strategy, evidence labels,
read-only behavior, and report format are **local review conventions**. Link and
CI checks are explicitly labeled engineering recommendations. Do not attribute
these conventions to an upstream framework.

## Diátaxis

- [Overview](https://diataxis.fr/): documentation responds to distinct needs for
  learning, accomplishing work, looking up facts, and building understanding.
- [Using Diátaxis](https://diataxis.fr/how-to-use-diataxis/): improve useful pieces
  iteratively; classification guides judgment rather than prescribing a template.
- [Tutorials](https://diataxis.fr/tutorials/): guide a learner through an achievable
  experience, giving concrete actions and visible progress with limited choices.
- [How-to guides](https://diataxis.fr/how-to-guides/): help a capable reader solve a
  recognizable problem, accommodating the decisions that real work requires.
- [Reference](https://diataxis.fr/reference/): make precise facts predictable to
  locate and consult; short examples can clarify behavior without becoming lessons.
- [Explanation](https://diataxis.fr/explanation/): connect concepts and illuminate
  rationale, constraints, alternatives, and consequences within a bounded subject.

**Verification:** all seven pages accessible; relevant principles inspected.
This skill uses a risk-based sample for audits; that is its own review method,
not a claim that Diátaxis prescribes audit sampling or severity rankings.

## Google writing guidance

- [Style guide](https://developers.google.com/style): prioritize the reader and
  consistency while allowing reasoned departures that improve communication.
- [Highlights](https://developers.google.com/style/highlights): use clear action
  language, useful links, accessible descriptions, and recognizable formatting
  for code and interface elements.
- [Procedures](https://developers.google.com/style/procedures): order instructions
  logically, establish context before actions, identify optional steps, and make
  command inputs and relevant results understandable.
- [Technical Writing overview](https://developers.google.com/tech-writing/overview):
  identifies courses and their intended learners; it is not evidence that every
  lesson has been reviewed.
- [Audience lesson](https://developers.google.com/tech-writing/one/audience):
  consider the reader's role and existing knowledge; explain what bridges the gap
  to the goal without assuming insider familiarity.
- [Clear sentences lesson](https://developers.google.com/tech-writing/one/clear-sentences):
  use precise subjects and verbs and concrete claims; changes should clarify
  meaning rather than mechanically eliminate grammatical forms.

**Verification:** the six linked pages were accessible and relevant material
inspected. Only the named lessons were sampled, not the entire curriculum.
An attempted `one/basics` URL was unavailable through the browsing tool; no rule
depends on it. Do not impose English-specific grammar or US spelling on a
different language or a documented regional house style.

## C4

- [Model overview](https://c4model.com/): describes nested architecture abstractions
  without prescribing a drawing tool or notation.
- [Diagram types](https://c4model.com/diagrams): select views for the audience;
  context and container views can be sufficient. Dynamic and deployment views
  answer different questions from static structure.
- [Review checklist](https://c4model.com/diagrams/checklist): readers should be able
  to identify scope, elements, responsibilities, relationship meaning and direction,
  and the visual notation. Relevant technology details remove ambiguity.
- [Container abstraction](https://c4model.com/abstractions/container): a C4 container
  describes an application or data store boundary, not specifically a Docker
  container, package, or directory.

**Verification:** all four pages accessible; definitions and checklist inspected.
Sequence diagrams, screenshots, and other non-C4 visuals retain their own purpose.
The rubric's implementation cross-check is an engineering accuracy safeguard.

## IBM-informed quality characteristics

- [IBM content-quality page](https://www.ibm.com/docs/en/technical-content?topic=standards-content-quality):
  requested primary reference, but the access attempt returned **403 Forbidden**.
  Its current body was not read or verified; do not label the URL broken.
- [Publisher excerpt of Developing Quality Technical Information](https://ptgmedia.pearsoncmg.com/images/9780133118971/samplepages/9780133118971.pdf):
  the accessible 53-page sample includes the contents and preface. The contents
  identify the nine characteristics; the preface describes the broad quality
  approach. Inspection covered those portions, **not the complete book or all
  nine chapters**. Printed contents pages xi–xiv are PDF pages 12–15 (one-based).

Use the nine characteristics as complementary questions: does content serve its
reader's purpose, reflect the system correctly, cover needed information, convey
meaning clearly, ground claims in specifics, use suitable conventions, arrange
ideas coherently, make answers findable, and use visuals effectively?

Their names are **task orientation, accuracy, completeness, clarity,
concreteness, style, organization, retrievability, and visual effectiveness**.
The detailed repository checks in the rubric are this skill's operational
interpretation, not quotations from inaccessible chapters or the IBM web page.

## Optional DITA-inspired maintenance

[OASIS DITA FAQ](https://www.oasis-open.org/committees/dita/faq.php) describes
topic-oriented content, reuse across deliverables, and navigation assembled
separately from topics. Apply the useful ideas to the repository's existing
formats: direct-linked topics should make sense, recurring facts need clear
ownership, and reusable text should earn its maintenance cost.

**Verification:** accessible; topic, reuse, and map explanations inspected.
The FAQ contains historical material. It is not used here to assert the latest
DITA specification version. Stable-link practices and drift checks are local
engineering recommendations. Preserve useful repetition, especially warnings.

## Codex packaging provenance

[Official skills documentation](https://developers.openai.com/codex/skills/)
redirected to [Build skills](https://learn.chatgpt.com/docs/build-skills), which
was accessible. The creation, discovery, and optional metadata sections were
inspected. A Markdown-format fetch was unsupported by the tool; the HTML page
provided the evidence.

Creation used the installed `skill-creator`, its initializer, and its
`references/openai_yaml.md`, with **Codex CLI 0.162.0** present. The only optional
metadata used is `interface.display_name`, `interface.short_description`, and
`interface.default_prompt`. Implicit invocation retains its default behavior.
No account, tool dependency, script, paid service, or documentation platform is
required by this skill.

## Offline and exception handling

Use the bundled rubric when online verification is unavailable. State that the
review applies the bundled interpretation and which links or current upstream
claims remain unverified. Do not imply fresh checks. If a recommendation depends
on guidance not summarized here, verify an appropriate primary source when
permitted or mark it as a reasoned recommendation with uncertainty.

Record project-specific exceptions with their source, rationale, and effect on
the assessment. A justified exception is not automatically a defect. Accuracy,
permissions, and consequences must remain explicit regardless of house style.
