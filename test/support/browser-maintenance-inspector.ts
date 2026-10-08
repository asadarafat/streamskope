import { inspectBrowserData } from "../../src/platform/node/browser-data-preflight";

// The command substitute runs the production read-only inspector on real fixture
// files. Image/process isolation belongs to the separate native Docker rehearsal.
async function main(): Promise<void> {
  const [directory, version] = process.argv.slice(2);
  if (!directory || !version) throw new Error("Missing owned fixture inspection arguments.");
  process.stdout.write(
    `${JSON.stringify(await inspectBrowserData(directory, { hostRelease: `v${version}` }))}\n`,
  );
}
void main();
