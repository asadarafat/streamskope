import { describe, expect, it } from "vitest";
import { createTheme, getContrastRatio } from "@mui/material/styles";

import {
  streamSkopeColorSchemes,
  streamSkopeLayout,
  streamSkopeMaterialSchemes,
  streamSkopeMuiMonospaceTypography,
  streamSkopeTheme,
} from "../../src/platform/ui/createStreamSkopeTheme";
import { streamSkopeGeometry } from "../../src/platform/ui/studioTokens";

describe("StreamSkope semantic theme", () => {
  it.each(["light", "dark"] as const)(
    "keeps primary and warning action text legible in %s mode",
    (mode) => {
      const { palette } = createTheme({
        palette: { ...streamSkopeColorSchemes[mode].palette, mode },
      });
      for (const color of [palette.primary, palette.warning]) {
        expect(getContrastRatio(color.main, color.contrastText)).toBeGreaterThanOrEqual(4.5);
        expect(getContrastRatio(color.dark, color.contrastText)).toBeGreaterThanOrEqual(4.5);
      }
    },
  );
  it("anchors compact controls to the natural button without fixing multiline height", () => {
    expect(streamSkopeGeometry.controlHeight).toBe(26);
    const button = streamSkopeTheme.components?.MuiButton?.styleOverrides?.root;
    expect(button).not.toHaveProperty("minHeight");
    expect(button).not.toHaveProperty("height");
    const input = streamSkopeTheme.components?.MuiOutlinedInput?.styleOverrides?.root;
    expect(input).not.toHaveProperty("height");
    expect(input).toMatchObject({ "&:not(.MuiInputBase-multiline)": { height: 26 } });
  });

  it("owns the neutral light material hierarchy and restrained accent", () => {
    const light = streamSkopeColorSchemes.light.palette;
    const materials = streamSkopeMaterialSchemes.light;

    expect(materials).toEqual({
      canvas: "#fafafa",
      panel: "#ffffff",
      raised: "#ffffff",
      recessed: "#fafafa",
      strongDivider: "rgba(0, 0, 0, 0.08)",
    });
    expect(light.background.default).toBe(materials.canvas);
    expect(light.background.paper).toBe(materials.panel);
    expect(light.text.primary).toBe("#1c1c21");
    expect(light.primary.main).toBe("#365fe8");
    expect(light.primary.light).toBe("#dce4ff");
    expect(light.primary.main).not.toBe(light.error.main);
    expect(light.primary.main).not.toBe(light.warning.main);
    expect(light.primary.main).not.toBe(light.success.main);
    expect(getContrastRatio(light.primary.main, materials.canvas)).toBeGreaterThanOrEqual(4.5);
    expect(getContrastRatio(light.primary.main, light.primary.contrastText)).toBeGreaterThanOrEqual(
      4.5,
    );
    expect(getContrastRatio(light.warning.main, light.background.default)).toBeGreaterThanOrEqual(
      4.5,
    );
    expect(getContrastRatio(light.text.secondary, materials.recessed)).toBeGreaterThanOrEqual(4.5);
    expect(getContrastRatio(light.text.disabled, materials.panel)).toBeGreaterThanOrEqual(4.5);
    expect(streamSkopeTheme.shape.borderRadius).toBe(8);
  });

  it("owns equivalent graphite materials without collapsing dark surfaces to black", () => {
    const dark = streamSkopeColorSchemes.dark.palette;
    const materials = streamSkopeMaterialSchemes.dark;

    expect(materials).toEqual({
      canvas: "#1b1b1f",
      panel: "#232328",
      raised: "#232328",
      recessed: "#1b1b1f",
      strongDivider: "rgba(255, 255, 255, 0.08)",
    });
    expect(dark.background.default).toBe(materials.canvas);
    expect(dark.background.paper).toBe(materials.panel);
    expect(dark.primary.main).toBe("#6e8bfb");
    expect(
      new Set([dark.primary.main, dark.error.main, dark.warning.main, dark.success.main]).size,
    ).toBe(4);
    expect(materials.canvas).not.toBe(materials.panel);
    expect(materials.strongDivider).not.toBe(materials.canvas);
    expect(materials.strongDivider).not.toBe(materials.panel);
    expect(getContrastRatio(dark.primary.dark, dark.primary.contrastText)).toBeGreaterThanOrEqual(
      4.5,
    );
    expect(getContrastRatio(dark.error.main, materials.raised)).toBeGreaterThanOrEqual(4.5);
    expect(getContrastRatio(dark.primary.main, materials.raised)).toBeGreaterThanOrEqual(4.5);
    expect(getContrastRatio(dark.primary.main, dark.primary.contrastText)).toBeGreaterThanOrEqual(
      4.5,
    );
    expect(getContrastRatio(dark.text.disabled, materials.raised)).toBeGreaterThanOrEqual(4.5);
  });

  it("keeps compatibility aliases derived from the canonical Studio contract", () => {
    const dialogPaper = streamSkopeTheme.components?.MuiDialog?.styleOverrides?.paper as {
      readonly maxHeight?: unknown;
    };

    expect(streamSkopeTheme.typography.fontFamily).toContain("Segoe UI");
    expect(streamSkopeMuiMonospaceTypography).toMatchObject({
      fontSize: "0.75rem",
      lineHeight: 1.5,
    });
    expect(streamSkopeLayout.compactControlHeight).toBe(streamSkopeGeometry.controlHeight);
    expect(streamSkopeLayout.contextBarHeight).toBe(streamSkopeGeometry.contextBarHeight);
    expect(streamSkopeLayout.headerHeight).toBe(streamSkopeGeometry.commandBarHeight);
    expect(streamSkopeLayout.resourceDefaultWidth).toBe(streamSkopeGeometry.navigatorWidth);
    expect(streamSkopeLayout.inspectorDefaultWidth).toBe(streamSkopeGeometry.inspectorDefaultWidth);
    expect(streamSkopeLayout.statusBarHeight).toBe(streamSkopeGeometry.statusBarHeight);
    expect(streamSkopeLayout.tableHeaderHeight).toBe(streamSkopeGeometry.tableHeaderHeight);
    expect(streamSkopeLayout.tableRowHeight).toBe(streamSkopeGeometry.tableRowHeight);
    expect(dialogPaper.maxHeight).toBe("calc(100% - 32px)");
  });
});
