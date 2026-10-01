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
import { streamSkopeColors } from "../../src/platform/ui/colorContract";
import { streamSkopeTypography } from "../../src/platform/ui/typographyContract";

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
      for (const text of [palette.text.primary, palette.text.secondary]) {
        expect(getContrastRatio(text, palette.background.paper)).toBeGreaterThanOrEqual(4.5);
      }
      expect(
        new Set([
          palette.primary.main,
          palette.error.main,
          palette.warning.main,
          palette.success.main,
        ]).size,
      ).toBe(4);
    },
  );
  it("anchors compact controls to the natural button without fixing multiline height", () => {
    const button = streamSkopeTheme.components?.MuiButton?.styleOverrides?.root;
    expect(button).not.toHaveProperty("minHeight");
    expect(button).not.toHaveProperty("height");
    const input = streamSkopeTheme.components?.MuiOutlinedInput?.styleOverrides?.root;
    expect(input).not.toHaveProperty("height");
    expect(input).toMatchObject({
      "&:not(.MuiInputBase-multiline)": { height: streamSkopeGeometry.controlHeight },
    });
  });

  it("owns the neutral light material hierarchy and restrained accent", () => {
    const light = streamSkopeColorSchemes.light.palette;
    const materials = streamSkopeMaterialSchemes.light;

    expect(materials).toMatchObject({
      canvas: streamSkopeColors.light.background.default,
      panel: streamSkopeColors.light.background.paper,
      strongDivider: streamSkopeColors.light.divider,
    });
    expect(light.background.default).toBe(materials.canvas);
    expect(light.background.paper).toBe(materials.panel);
    expect(light.primary.main).toBe(streamSkopeColors.light.primary.main);
    expect(getContrastRatio(light.primary.main, materials.canvas)).toBeGreaterThanOrEqual(4.5);
    expect(getContrastRatio(light.primary.main, light.primary.contrastText)).toBeGreaterThanOrEqual(
      4.5,
    );
    expect(getContrastRatio(light.warning.main, light.background.default)).toBeGreaterThanOrEqual(
      4.5,
    );
    expect(getContrastRatio(light.text.secondary, materials.recessed)).toBeGreaterThanOrEqual(4.5);
    expect(getContrastRatio(light.text.disabled, materials.panel)).toBeGreaterThanOrEqual(4.5);
  });

  it("owns equivalent graphite materials without collapsing dark surfaces to black", () => {
    const dark = streamSkopeColorSchemes.dark.palette;
    const materials = streamSkopeMaterialSchemes.dark;

    expect(materials).toMatchObject({
      canvas: streamSkopeColors.dark.background.default,
      panel: streamSkopeColors.dark.background.paper,
      strongDivider: streamSkopeColors.dark.divider,
    });
    expect(dark.background.default).toBe(materials.canvas);
    expect(dark.background.paper).toBe(materials.panel);
    expect(dark.primary.main).toBe(streamSkopeColors.dark.primary.main);
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

    expect(streamSkopeTheme.typography.fontFamily).toBe(streamSkopeTypography.family.interface);
    expect(streamSkopeMuiMonospaceTypography).toMatchObject({
      fontFamily: streamSkopeTypography.family.monospace,
      fontSize: `${streamSkopeTypography.monospace.size / streamSkopeTypography.rootSize}rem`,
      lineHeight: streamSkopeTypography.monospace.lineHeight / streamSkopeTypography.monospace.size,
    });
    for (const [variant, role] of [
      ["h5", streamSkopeTypography.roles.pageTitle],
      ["h6", streamSkopeTypography.roles.appTitle],
      ["body1", streamSkopeTypography.roles.body],
      ["body2", streamSkopeTypography.roles.compact],
    ] as const) {
      expect(streamSkopeTheme.typography[variant]).toMatchObject({
        fontSize: `${role.size / streamSkopeTypography.rootSize}rem`,
        lineHeight: role.lineHeight / role.size,
      });
    }
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
