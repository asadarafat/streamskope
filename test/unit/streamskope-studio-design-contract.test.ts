import { getContrastRatio } from "@mui/material/styles";
import { describe, expect, it } from "vitest";

import { streamSkopeColors } from "../../src/ui/colorContract";
import {
  interfaceFontFamily,
  monospaceFontFamily,
  streamSkopeMuiMonospaceTypography,
  streamSkopeMuiResourceTypography,
  streamSkopeTheme,
} from "../../src/ui/createStreamSkopeTheme";
import { streamSkopeSpacing } from "../../src/ui/spacingContract";
import { streamSkopeGeometry, streamSkopeRadius } from "../../src/ui/studioTokens";
import { streamSkopeTypography } from "../../src/ui/typographyContract";

describe("StreamSkope desktop design contract", () => {
  it("owns the bounded interface and technical-evidence roles without a feature-local scale", () => {
    expect(Object.isFrozen(streamSkopeTypography)).toBe(true);
    expect(streamSkopeTypography).toEqual({
      buttonTextTransform: "none",
      family: {
        interface: 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
        monospace:
          'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
      },
      iconSize: { compact: 14, inline: 16 },
      letterSpacing: 0,
      roles: {
        appTitle: { lineHeight: 24, size: 18, weight: 600 },
        body: { lineHeight: 20, size: 13, weight: 400 },
        compact: { lineHeight: 18, size: 12, weight: 400 },
        control: { lineHeight: 18, size: 13, weight: 400 },
        label: { lineHeight: 18, size: 13, weight: 600 },
        metadata: { lineHeight: 17, size: 12, weight: 400 },
        pageTitle: { lineHeight: 32, size: 24, weight: 600 },
        panelTitle: { lineHeight: 22, size: 15, weight: 600 },
        sectionLabel: { lineHeight: 18, size: 12, weight: 600 },
        sectionTitle: { lineHeight: 20, size: 14, weight: 600 },
      },
      rootSize: 16,
      sectionLabelTracking: 0,
      sectionLabelTransform: "none",
      monospace: { lineHeight: 18, size: 12, weight: 400 },
    });

    expect(new Set(Object.values(streamSkopeTypography.roles).map((role) => role.size))).toEqual(
      new Set([24, 18, 15, 14, 13, 12]),
    );

    expect(streamSkopeTheme.typography.fontFamily).toBe(streamSkopeTypography.family.interface);
    expect(streamSkopeTheme.typography.fontSize).toBe(13);
    expect(streamSkopeTheme.typography.h6).toMatchObject({
      fontSize: "1.125rem",
      fontWeight: 600,
      lineHeight: 24 / 18,
    });
    expect(streamSkopeTheme.typography.h5).toMatchObject({
      fontSize: "1.5rem",
      fontWeight: 600,
      lineHeight: 32 / 24,
    });
    expect(streamSkopeTheme.typography.body1).toMatchObject({
      fontSize: "0.8125rem",
      lineHeight: 20 / 13,
    });
    expect(streamSkopeTheme.typography.body2).toMatchObject({
      fontSize: "0.75rem",
      lineHeight: 1.5,
    });
    expect(streamSkopeTheme.typography.caption).toMatchObject({
      fontSize: "0.75rem",
      lineHeight: 17 / 12,
    });
  });

  it("owns one monospace size for technical evidence", () => {
    expect(streamSkopeMuiMonospaceTypography).toEqual({
      fontFamily: monospaceFontFamily,
      fontSize: "0.75rem",
      fontVariantNumeric: "tabular-nums",
      lineHeight: 1.5,
    });
    expect(streamSkopeMuiResourceTypography).toMatchObject({
      fontFamily: interfaceFontFamily,
      fontSize: "0.8125rem",
      lineHeight: 18 / 13,
    });
    expect(streamSkopeTypography.monospace).toEqual({ lineHeight: 18, size: 12, weight: 400 });

    const tableHeader = streamSkopeTheme.components?.MuiTableCell?.styleOverrides?.head;
    expect(tableHeader).toMatchObject({ fontSize: "0.75rem" });
  });

  it("owns coherent Kubus-informed desktop geometry as immutable exact values", () => {
    expect(Object.isFrozen(streamSkopeSpacing)).toBe(true);
    expect(Object.values(streamSkopeSpacing.scale)).toEqual([0, 2, 4, 6, 8, 10, 12, 16, 24]);
    expect(streamSkopeSpacing.baseUnit).toBe(8);
    expect(streamSkopeGeometry).toMatchObject({
      activityCollapsedHeight: 36,
      activityHeight: 240,
      commandBarHeight: 52,
      contextBarHeight: 52,
      controlHeight: 26,
      localToolbarHeight: 44,
      navigatorWidth: 228,
      profileRowHeight: 44,
      resourceDisclosureHeight: 36,
      resourceHeaderHeight: 52,
      resourceIconBoxSize: 32,
      resourceIconSize: 24,
      resourcePanelInline: 12,
      resourceRowHeight: 34,
      resourceSearchHeight: 52,
      resourceTabDotSize: 6,
      resourceTabGap: 8,
      resourceTabHeight: 34,
      resourceTabInline: 12,
      resourceTabsHeight: 40,
      topicRowHeight: 40,
      topicListInset: 12,
      topicListMargin: 4,
      statusBarHeight: 26,
      workspaceTabWidth: 190,
      workspaceTabsHeight: 40,
    });
    expect(streamSkopeRadius).toEqual({ control: 8, round: 999, surface: 10 });
    expect(streamSkopeTheme.shape.borderRadius).toBe(8);
  });

  it("uses neutral materials and functional blue without confusing status", () => {
    expect(streamSkopeColors.light).toMatchObject({
      background: { default: "#fafafa", paper: "#ffffff" },
      divider: "rgba(0, 0, 0, 0.08)",
      primary: { main: "#365fe8" },
      text: { primary: "#1c1c21", secondary: "#6e6e78" },
    });
    expect(streamSkopeColors.dark).toMatchObject({
      background: { default: "#1b1b1f", paper: "#232328" },
      divider: "rgba(255, 255, 255, 0.08)",
      primary: { main: "#6e8bfb" },
      text: { primary: "#e6e6ea", secondary: "#9d9da7" },
    });
    for (const scheme of [streamSkopeColors.light, streamSkopeColors.dark]) {
      expect(
        new Set([scheme.primary.main, scheme.error.main, scheme.warning.main, scheme.success.main])
          .size,
      ).toBe(4);
      expect(getContrastRatio(scheme.text.primary, scheme.background.paper)).toBeGreaterThanOrEqual(
        4.5,
      );
      expect(
        getContrastRatio(scheme.text.secondary, scheme.background.paper),
      ).toBeGreaterThanOrEqual(4.5);
    }
  });
});
