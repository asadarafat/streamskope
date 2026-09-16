import { access, readFile, readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const repositoryRoot = fileURLToPath(new URL("../..", import.meta.url));
const rendererUiRoot = fileURLToPath(new URL("../../src/features/kafka/ui/", import.meta.url));
const applicationRoot = rendererUiRoot;

const sharedUiOwners = [
  "src/platform/ui/colorContract.ts",
  "src/platform/ui/controls.tsx",
  "src/platform/ui/createStreamSkopeTheme.ts",
  "src/platform/ui/spacingContract.ts",
  "src/platform/ui/StudioCodeBlock.tsx",
  "src/platform/ui/StudioInventoryGrid.tsx",
  "src/platform/ui/StudioPanel.tsx",
  "src/platform/ui/StudioPropertyRow.tsx",
  "src/platform/ui/studioTokens.ts",
  "src/platform/ui/typographyContract.ts",
] as const;

const interactiveMaterialOwners = new Set([
  "Accordion",
  "AccordionDetails",
  "AccordionSummary",
  "Alert",
  "Button",
  "Checkbox",
  "Dialog",
  "DialogActions",
  "DialogContent",
  "DialogTitle",
  "FormControl",
  "FormControlLabel",
  "FormLabel",
  "IconButton",
  "InputLabel",
  "ListItemButton",
  "MenuItem",
  "Radio",
  "Select",
  "Switch",
  "Tab",
  "Tabs",
  "TextField",
  "ToggleButton",
  "ToggleButtonGroup",
  "Tooltip",
]);

const monospaceFeatureOwners = new Set([
  "ActivityLogDrawer.tsx",
  "ClusterDetailsDialog.tsx",
  "ConsumerGroupWorkspace.tsx",
  "LatencyWorkspace.tsx",
  "MessageDataGrid.tsx",
  "MessageInspector.tsx",
  "ProfilePanel.tsx",
  "ProfileWorkspace.tsx",
  "TopicConfigurationWorkspace.tsx",
  "WorkbenchContextBar.tsx",
  "WorkbenchCommandPalette.tsx",
]);

function directInteractiveMaterialImports(source: string): readonly string[] {
  const imports: string[] = [];
  for (const match of source.matchAll(/import\s+\{([^}]*)\}\s+from\s+["']@mui\/material["'];?/gu)) {
    for (const candidate of (match[1] ?? "").split(",")) {
      const name = candidate.trim().split(/\s+/u)[0];
      if (name !== undefined && interactiveMaterialOwners.has(name)) {
        imports.push(name);
      }
    }
  }
  for (const match of source.matchAll(
    /import\s+([A-Za-z][A-Za-z0-9]*)\s+from\s+["']@mui\/material\/([A-Za-z][A-Za-z0-9]*)["'];?/gu,
  )) {
    const owner = match[2];
    if (owner !== undefined && interactiveMaterialOwners.has(owner)) {
      imports.push(owner);
    }
  }
  return imports;
}

describe("TopoViewer Studio frontend ownership", () => {
  it("keeps application composition inside the Kafka feature and shared UI in platform", async () => {
    const applicationSource = await readFile(`${applicationRoot}/StreamSkopeApp.tsx`, "utf8");
    const workbenchSource = await readFile(`${rendererUiRoot}/StreamSkopeWorkbench.tsx`, "utf8");
    const rendererEntrySource = await readFile(
      `${repositoryRoot}/src/platform/electron/renderer/main.tsx`,
      "utf8",
    );

    expect(applicationSource).toContain("./StreamSkopeWorkbench");
    expect(applicationSource).toContain("../../../platform/ui/StreamSkopeThemeProvider");
    expect(workbenchSource).not.toContain("StreamSkopeThemeProvider");
    expect(rendererEntrySource).toContain("../../../features/kafka/ui/StreamSkopeApp");
    expect(rendererEntrySource).not.toContain("../../../features/kafka/ui/StreamSkopeWorkbench");
  });

  it("keeps shared presentation knowledge in src/platform/ui instead of the Kafka feature", async () => {
    await expect(
      Promise.all(sharedUiOwners.map((path) => access(`${repositoryRoot}/${path}`))),
    ).resolves.toHaveLength(sharedUiOwners.length);

    await expect(access(`${rendererUiRoot}/design-contracts.ts`)).rejects.toThrow();
    await expect(access(`${rendererUiRoot}/theme.ts`)).rejects.toThrow();
  });

  it("keeps type metrics and font-family ownership out of Kafka feature presenters", async () => {
    const fileNames = (await readdir(rendererUiRoot)).filter((fileName) =>
      fileName.endsWith(".tsx"),
    );
    const violations: string[] = [];

    for (const fileName of fileNames) {
      const source = await readFile(`${rendererUiRoot}/${fileName}`, "utf8");
      for (const [index, line] of source.split("\n").entries()) {
        if (/\b(?:fontFamily|fontSize|lineHeight)\s*:/u.test(line)) {
          violations.push(`${fileName}:${String(index + 1)} ${line.trim()}`);
        }
      }
      if (source.includes("technicalFontFamily")) {
        violations.push(`${fileName}: imports the technical family instead of a shared style`);
      }
    }

    expect(violations).toEqual([]);
  });

  it("limits the one monospace style to approved Kafka evidence owners", async () => {
    const fileNames = (await readdir(rendererUiRoot)).filter((fileName) =>
      fileName.endsWith(".tsx"),
    );
    const violations: string[] = [];

    for (const fileName of fileNames) {
      const source = await readFile(`${rendererUiRoot}/${fileName}`, "utf8");
      if (
        source.includes("streamSkopeMuiTechnicalTypography") ||
        source.includes("streamSkopeMuiCodeTypography")
      ) {
        violations.push(`${fileName}: imports a superseded multi-role monospace style`);
      }
      if (
        source.includes("streamSkopeMuiMonospaceTypography") &&
        !monospaceFeatureOwners.has(fileName)
      ) {
        violations.push(`${fileName}: uses monospace outside an approved semantic owner`);
      }
    }

    expect(violations).toEqual([]);
  });

  it("routes interactive Material controls through the shared Studio UI owner", async () => {
    const fileNames = (await readdir(rendererUiRoot)).filter((fileName) =>
      fileName.endsWith(".tsx"),
    );
    const violations: string[] = [];

    for (const fileName of fileNames) {
      const source = await readFile(`${rendererUiRoot}/${fileName}`, "utf8");
      for (const owner of directInteractiveMaterialImports(source)) {
        violations.push(`${fileName}: imports ${owner} directly from Material UI`);
      }
    }

    expect(violations).toEqual([]);
  });

  it("uses Studio's composed select contract instead of native option controls", async () => {
    const fileNames = (await readdir(rendererUiRoot)).filter((fileName) =>
      fileName.endsWith(".tsx"),
    );
    const violations: string[] = [];

    for (const fileName of fileNames) {
      const source = await readFile(`${rendererUiRoot}/${fileName}`, "utf8");
      if (/<option\b/u.test(source)) {
        violations.push(`${fileName}: renders native option controls`);
      }
      if (/<Select\b[^>]*\bnative\b/su.test(source)) {
        violations.push(`${fileName}: enables native select mode`);
      }
    }

    expect(violations).toEqual([]);
  });
});
