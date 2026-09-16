# StreamSkope Graph Context Token Benchmark

This note records an initial, reproducible comparison of Graphify and
codebase-memory-mcp for reducing agent code-reading tokens on StreamSkope.

Token estimates use `ceil(bytes / 4)`. That matches Graphify's documented
benchmark approximation and gives a stable local metric without binding this
repo to a tokenizer dependency.

## Repo Baseline

- Snapshot: `evaluate-graph-context-tools` after the latest `main` pull.
- Naive code corpus: `~365,200` tokens from Graphify's built-in benchmark.
- Fixed benchmark budget: approximately `2,000` output tokens per query.

Reproduce the benchmark with:

```bash
python3 -m venv .cache/graph-benchmark/graphify-venv
. .cache/graph-benchmark/graphify-venv/bin/activate
pip install graphifyy
node tools/measure-graph-context-tokens.mjs
```

The runner writes generated graphs and raw results under `.cache/graph-benchmark/`.

## Index Results

| Tool | Command shape | Indexed surface | Nodes | Edges | Elapsed |
| --- | --- | ---: | ---: | ---: | ---: |
| Graphify 0.9.62 | `graphify extract . --code-only --no-cluster --no-viz --out .cache/graph-benchmark/graphify-output` | 591 code files | 5,370 at extract; 5,478 in benchmark | 19,087 | 13s |
| codebase-memory-mcp 0.11.0 | `CBM_CACHE_DIR=.cache/graph-benchmark/cbm-cache npx -y codebase-memory-mcp@latest cli index_repository --repo-path "$PWD"` | Repo minus `.git`, `.cache`, `node_modules`, fixture ownership/runtime | 8,082 | 32,190 | 24s |

## Graphify Built-In Benchmark

| Metric | Value |
| --- | ---: |
| Naive corpus tokens | `~365,200` |
| Average query cost | `~70,536` |
| Reduction | `5.2x` fewer tokens/query |

Per built-in question reductions: `3.6x`, `46.0x`, `1.6x`, `51.9x`, `24.9x`.

## Fixed StreamSkope Questions

1. How do Kafka connection profiles flow from UI to facade to application service?
2. What owns local AIO Kafka dev fixture startup and readiness?
3. How do rule definitions get validated, evaluated, and surfaced in the UI?
4. What is the blast radius of changing profile trust acquisition?
5. How does Schema Registry support connect from profile configuration to engine HTTP calls?

| Tool/query mode | Q1 | Q2 | Q3 | Q4 | Q5 | Avg tokens | Reduction vs naive corpus |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Graphify `query --budget 2000` | 1,653 | 1,676 | 1,632 | 1,629 | 2,689 | 1,856 | `196.8x` |
| CBM `search_graph semantic_query max_output_tokens=2000` | 1,987 | 1,862 | 1,928 | 1,825 | 1,941 | 1,909 | `191.3x` |
| CBM `search_graph --query max-output-tokens=2000` | 1,999 | 1,692 | 1,772 | 1,991 | 1,883 | 1,867 | `195.6x` |

## Relevance Notes

- Graphify returned better broad subsystem context for profile flow, fixture
  startup, trust acquisition, and Schema Registry. Relevant hits included
  `KafkaProfileService`, `KafkaBackendFacade`, `KafkaApplicationSession`,
  `NodeFixtureRuntime`, `start-web-development.ts`,
  `KafkaTrustAcquisitionService`, `SchemaRegistryHttpAdapter`, and
  `schema-registry-validation.ts`.
- Graphify can still be broad. Several results warned that hundreds or
  thousands of nodes matched and only the first budgeted slice was shown.
- codebase-memory-mcp semantic search respected the output cap but was noisy
  for broad natural-language questions, especially Q1 and Q2.
- codebase-memory-mcp lexical search was better for exact subsystem terms and
  especially strong for Q5, but it also returned synthetic route nodes and
  unrelated helpers.
- Under a fixed output budget, size alone does not distinguish the tools. The
  more useful decision metric is relevant subsystem hits per output token.

## Current Decision

Graphify is the better first candidate for StreamSkope agent exploration because
it produced more useful cross-subsystem context with lower index time in this
initial benchmark. codebase-memory-mcp remains worth evaluating if the priority
shifts to persistent MCP integration, exact symbol commands, or long-lived
multi-agent cache behavior.
