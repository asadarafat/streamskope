import { inspectBrowserData, unavailableBrowserDataInspection } from "./browser-data-preflight";

void inspectBrowserData(process.argv.length === 3 ? process.argv[2]! : "")
  .catch(() => unavailableBrowserDataInspection())
  .then((report) => {
    process.stdout.write(`${JSON.stringify(report)}\n`);
    process.exitCode = report.outcome === "eligible" ? 0 : 2;
  });
