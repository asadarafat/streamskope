/* global process, console */
import { execFileSync } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  writeFileSync,
} from "node:fs";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";

const source = realpathSync(process.cwd());
const destinationArgument = process.argv[2];
if (!destinationArgument || process.argv.length !== 3)
  throw new Error("Usage: node tools/maintenance/export-source.mjs /absolute/new/directory");
if (!isAbsolute(destinationArgument)) throw new Error("The export destination must be absolute.");
const requestedDestination = resolve(destinationArgument);
let parent = dirname(requestedDestination);
while (!existsSync(parent)) parent = dirname(parent);
const destination = resolve(realpathSync(parent), relative(parent, requestedDestination));
const relation = relative(source, destination);
if ((relation !== ".." && !relation.startsWith(`..${sep}`)) || destination === source)
  throw new Error("Export outside the development repository.");
if (existsSync(destination))
  throw new Error("The destination must not exist; exports never overwrite files.");
const manifest = JSON.parse(readFileSync("package.json", "utf8"));
if (manifest.version !== "0.1.0")
  throw new Error("The first public snapshot must be version 0.1.0.");
const files = execFileSync(
  "git",
  ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
  { encoding: "utf8" },
)
  .split("\0")
  .filter(Boolean);
const ignoredTracked = execFileSync("git", ["ls-files", "-ci", "--exclude-standard"], {
  encoding: "utf8",
}).trim();
if (ignoredTracked)
  throw new Error("Private or generated files are tracked; resolve them before exporting.");
// Inspect every source before creating the destination; reject navigation links.
const exportFiles = [...new Set(files)].filter(
  (file) => !file.startsWith("apps/") && existsSync(resolve(source, file)),
);
for (const file of exportFiles) {
  if (file.startsWith(".git/") || !lstatSync(resolve(source, file)).isFile())
    throw new Error(`Unsupported export file: ${file}`);
}
mkdirSync(destination, { recursive: true });
for (const file of exportFiles) {
  const target = resolve(destination, file);
  mkdirSync(dirname(target), { recursive: true });
  copyFileSync(resolve(source, file), target);
}
execFileSync("git", ["init", "--initial-branch=main", destination], { stdio: "inherit" });
writeFileSync(
  resolve(destination, ".git/streamskope-export.json"),
  JSON.stringify(
    {
      sourceHead: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
      files: exportFiles.length,
      desktopVersion: "0.1.0",
    },
    null,
    2,
  ),
);
console.log(
  `Exported ${exportFiles.length} public source files to ${destination}. Review and commit the snapshot there; it has no commits, tags, remotes, or inherited development history.`,
);
