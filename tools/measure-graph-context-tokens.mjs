import { Buffer } from "node:buffer";
import { spawn } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import process from "node:process";

const root = process.cwd();
const cacheRoot = join(root, ".cache", "graph-benchmark");
const resultsRoot = join(cacheRoot, "results");
const graphifyOut = join(cacheRoot, "graphify-output");
const cbmCache = join(cacheRoot, "cbm-cache");
const graphifyBin =
  process.env.GRAPHIFY_BIN ?? join(cacheRoot, "graphify-venv", "bin", "graphify");
const cbmBin = process.env.CBM_BIN ?? "npx";
const corpusTokens = Number(process.env.GRAPH_BENCHMARK_CORPUS_TOKENS ?? 365_200);
const queryBudget = Number(process.env.GRAPH_BENCHMARK_QUERY_TOKENS ?? 2_000);
const cbmProject = process.env.CBM_PROJECT ?? "Users-aarafat-_project-streamskope";

const questions = [
  "How do Kafka connection profiles flow from UI to facade to application service?",
  "What owns local AIO Kafka dev fixture startup and readiness?",
  "How do rule definitions get validated, evaluated, and surfaced in the UI?",
  "What is the blast radius of changing profile trust acquisition?",
  "How does Schema Registry support connect from profile configuration to engine HTTP calls?",
];

function estimateTokens(text) {
  return Math.ceil(Buffer.byteLength(text, "utf8") / 4);
}

function commandName(command, args) {
  return [command, ...args].join(" ");
}

async function run(command, args, options = {}) {
  const started = Date.now();
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: root,
      env: { ...process.env, ...(options.env ?? {}) },
      shell: options.shell ?? false,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("close", (code) => {
      const elapsedSeconds = (Date.now() - started) / 1000;
      if (code === 0) {
        resolve({ elapsedSeconds, stderr, stdout });
        return;
      }
      reject(
        new Error(
          `${commandName(command, args)} failed with exit ${code}\n${stderr || stdout}`.trim(),
        ),
      );
    });
  });
}

async function writeResult(name, content) {
  await writeFile(join(resultsRoot, name), content);
}

async function measureGraphify() {
  const index = await run(graphifyBin, [
    "extract",
    ".",
    "--code-only",
    "--no-cluster",
    "--no-viz",
    "--out",
    graphifyOut,
  ]);
  const graph = join(graphifyOut, "graphify-out", "graph.json");
  const benchmark = await run(graphifyBin, ["benchmark", graph]);
  await writeResult("graphify-benchmark.txt", benchmark.stdout);

  const queryRows = [];
  for (const [index, question] of questions.entries()) {
    const result = await run(graphifyBin, [
      "query",
      question,
      "--graph",
      graph,
      "--budget",
      String(queryBudget),
    ]);
    await writeResult(`graphify-query-${index + 1}.txt`, result.stdout);
    queryRows.push({
      bytes: Buffer.byteLength(result.stdout, "utf8"),
      question,
      tokens: estimateTokens(result.stdout),
    });
  }

  return {
    elapsedSeconds: index.elapsedSeconds,
    queries: queryRows,
    stderr: index.stderr,
    stdout: index.stdout,
  };
}

async function measureCbm(mode) {
  const commonEnv = { CBM_CACHE_DIR: cbmCache };
  const index = await run(
    cbmBin,
    ["-y", "codebase-memory-mcp@latest", "cli", "--quiet", "index_repository", "--repo-path", root],
    { env: commonEnv },
  );
  await writeResult("cbm-index.json", index.stdout);

  const queryRows = [];
  for (const [index, question] of questions.entries()) {
    const args =
      mode === "semantic"
        ? [
            "-y",
            "codebase-memory-mcp@latest",
            "cli",
            "--quiet",
            "search_graph",
            JSON.stringify({
              format: "tree",
              max_output_tokens: queryBudget,
              project: cbmProject,
              semantic_limit: 50,
              semantic_query: [question],
            }),
          ]
        : [
            "-y",
            "codebase-memory-mcp@latest",
            "cli",
            "--quiet",
            "search_graph",
            "--project",
            cbmProject,
            "--query",
            question,
            "--limit",
            "50",
            "--max-output-tokens",
            String(queryBudget),
            "--format",
            "tree",
          ];
    const result = await run(cbmBin, args, { env: commonEnv });
    await writeResult(`cbm-${mode}-${index + 1}.txt`, result.stdout);
    queryRows.push({
      bytes: Buffer.byteLength(result.stdout, "utf8"),
      question,
      tokens: estimateTokens(result.stdout),
    });
  }

  return {
    elapsedSeconds: index.elapsedSeconds,
    queries: queryRows,
  };
}

function summaryRow(name, result) {
  const averageTokens =
    result.queries.reduce((total, query) => total + query.tokens, 0) / result.queries.length;
  return {
    averageTokens,
    name,
    reductionVsCorpus: corpusTokens / averageTokens,
    tokens: result.queries.map((query) => query.tokens),
  };
}

async function main() {
  await mkdir(resultsRoot, { recursive: true });
  await mkdir(cbmCache, { recursive: true });
  await writeResult("questions.txt", `${questions.join("\n")}\n`);

  const graphify = await measureGraphify();
  const cbmSemantic = await measureCbm("semantic");
  const cbmLexical = await measureCbm("lexical");
  const summary = {
    cbmLexical: summaryRow("CBM lexical search", cbmLexical),
    cbmSemantic: summaryRow("CBM semantic search", cbmSemantic),
    corpusTokens,
    graphify: summaryRow("Graphify query", graphify),
    indexSeconds: {
      cbmLexical: cbmLexical.elapsedSeconds,
      cbmSemantic: cbmSemantic.elapsedSeconds,
      graphify: graphify.elapsedSeconds,
    },
    queryBudget,
  };

  await writeResult("summary.json", `${JSON.stringify(summary, undefined, 2)}\n`);
  process.stdout.write(`${JSON.stringify(summary, undefined, 2)}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
