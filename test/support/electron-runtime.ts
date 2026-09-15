import type { ElectronApplication, Page } from "@playwright/test";

import type { ElectronProcessEvidence } from "../../tools/electron-runtime-efficiency-policy";
import {
  electronProcessEvidenceFromMetrics,
  type ElectronRuntimeMetric,
} from "../../tools/electron-runtime-evidence";

export async function sampleElectronProcesses(
  application: ElectronApplication,
  page: Page,
): Promise<readonly (readonly ElectronProcessEvidence[])[]> {
  await application.evaluate(({ app }) => app.getAppMetrics());
  await page.waitForTimeout(500);
  const samples: ElectronProcessEvidence[][] = [];
  for (let index = 0; index < 3; index += 1) {
    const metrics = await application.evaluate(({ app }) =>
      app.getAppMetrics().map((metric) => ({
        cpu: { percentCPUUsage: metric.cpu.percentCPUUsage },
        memory: { workingSetSize: metric.memory.workingSetSize },
        type: metric.type,
      })),
    );
    samples.push(
      electronProcessEvidenceFromMetrics(metrics satisfies readonly ElectronRuntimeMetric[]),
    );
    if (index < 2) {
      await page.waitForTimeout(500);
    }
  }
  return samples;
}
