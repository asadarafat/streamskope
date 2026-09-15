import { createTheme, type TypographyStyle } from "@mui/material/styles";

import { streamSkopeColors, type StreamSkopeColorScheme } from "./colorContract";
import { streamSkopeSpacing } from "./spacingContract";
import { streamSkopeCssVariables } from "./studioCssVariables";
import { streamSkopeGeometry, streamSkopeRadius } from "./studioTokens";
import { streamSkopeTypography, type StreamSkopeTypographyRole } from "./typographyContract";

export const interfaceFontFamily = streamSkopeTypography.family.interface;
export const monospaceFontFamily = streamSkopeTypography.family.monospace;

/** Compatibility facade for existing feature presenters; values come from the canonical contracts. */
export const streamSkopeLayout = Object.freeze({
  activityCollapsedHeight: streamSkopeGeometry.activityCollapsedHeight,
  activityHeight: streamSkopeGeometry.activityHeight,
  activityMaximumHeight: streamSkopeGeometry.activityMaximumHeight,
  activityMinimumHeight: streamSkopeGeometry.activityMinimumHeight,
  activitySeparatorHeight: streamSkopeGeometry.activitySeparatorHeight,
  breadcrumbBarHeight: streamSkopeGeometry.breadcrumbBarHeight,
  compactControlHeight: streamSkopeGeometry.controlHeight,
  contextBarHeight: streamSkopeGeometry.contextBarHeight,
  controlHeight: streamSkopeGeometry.controlHeight,
  fullDesktopMinimumWidth: streamSkopeGeometry.fullDesktopMinimumWidth,
  headerHeight: streamSkopeGeometry.commandBarHeight,
  inspectorDefaultWidth: streamSkopeGeometry.inspectorDefaultWidth,
  inspectorMaximumWidth: streamSkopeGeometry.inspectorMaximumWidth,
  inspectorMinimumWidth: streamSkopeGeometry.inspectorMinimumWidth,
  messageInspectorFullColumnsMinimumWidth:
    streamSkopeGeometry.messageInspectorFullColumnsMinimumWidth,
  localToolbarHeight: streamSkopeGeometry.localToolbarHeight,
  profileRowHeight: streamSkopeGeometry.profileRowHeight,
  resourceDefaultWidth: streamSkopeGeometry.navigatorWidth,
  resourceHeaderHeight: streamSkopeGeometry.resourceHeaderHeight,
  resourceMaximumWidth: streamSkopeGeometry.navigatorMaximumWidth,
  resourceMinimumWidth: streamSkopeGeometry.navigatorMinimumWidth,
  resourceRowHeight: streamSkopeGeometry.resourceRowHeight,
  resourceSearchHeight: streamSkopeGeometry.resourceSearchHeight,
  resizerWidth: streamSkopeGeometry.resizerWidth,
  statusBarHeight: streamSkopeGeometry.statusBarHeight,
  tableHeaderHeight: streamSkopeGeometry.tableHeaderHeight,
  tableRowHeight: streamSkopeGeometry.tableRowHeight,
  topicRowHeight: streamSkopeGeometry.topicRowHeight,
  workspaceBarHeight: streamSkopeGeometry.localToolbarHeight,
  workspaceTabWidth: streamSkopeGeometry.workspaceTabWidth,
  workspaceTabsHeight: streamSkopeGeometry.workspaceTabsHeight,
});

export const streamSkopeMaterialSchemes = Object.freeze({
  dark: {
    canvas: streamSkopeColors.dark.background.default,
    panel: streamSkopeColors.dark.background.paper,
    raised: streamSkopeColors.dark.background.paper,
    recessed: streamSkopeColors.dark.background.default,
    strongDivider: streamSkopeColors.dark.divider,
  },
  light: {
    canvas: streamSkopeColors.light.background.default,
    panel: streamSkopeColors.light.background.paper,
    raised: streamSkopeColors.light.background.paper,
    recessed: streamSkopeColors.light.background.default,
    strongDivider: streamSkopeColors.light.divider,
  },
});

function toPalette(scheme: StreamSkopeColorScheme): {
  palette: Omit<StreamSkopeColorScheme, "navigation">;
} {
  return {
    palette: {
      action: scheme.action,
      background: scheme.background,
      divider: scheme.divider,
      error: scheme.error,
      info: scheme.info,
      primary: scheme.primary,
      success: scheme.success,
      text: scheme.text,
      warning: scheme.warning,
    },
  };
}

export const streamSkopeColorSchemes = Object.freeze({
  dark: toPalette(streamSkopeColors.dark),
  light: toPalette(streamSkopeColors.light),
});

function toRem(size: number): string {
  return `${String(size / streamSkopeTypography.rootSize)}rem`;
}

function typography(role: StreamSkopeTypographyRole): TypographyStyle {
  return {
    fontSize: toRem(role.size),
    fontWeight: role.weight,
    letterSpacing: streamSkopeTypography.letterSpacing,
    lineHeight: role.lineHeight / role.size,
  };
}

function monospaceTypography(role: StreamSkopeTypographyRole): TypographyStyle {
  return {
    fontFamily: streamSkopeTypography.family.monospace,
    fontSize: toRem(role.size),
    fontVariantNumeric: "tabular-nums",
    lineHeight: role.lineHeight / role.size,
  };
}

/** The sole monospace role, reserved for topics, addresses, logs, and message evidence. */
export const streamSkopeMuiMonospaceTypography: TypographyStyle = Object.freeze({
  ...monospaceTypography(streamSkopeTypography.monospace),
});

export const streamSkopeMuiResourceTypography: TypographyStyle = Object.freeze({
  ...typography(streamSkopeTypography.roles.control),
  fontFamily: streamSkopeTypography.family.interface,
});

export const streamSkopeMuiIconSize = streamSkopeTypography.iconSize;

const bodyTypography = typography(streamSkopeTypography.roles.body);
const compactTypography = typography(streamSkopeTypography.roles.compact);
const controlTypography = typography(streamSkopeTypography.roles.control);
const labelTypography = typography(streamSkopeTypography.roles.label);
const metadataTypography = typography(streamSkopeTypography.roles.metadata);
const lightVariables = {
  ...streamSkopeCssVariables,
  "--streamskope-divider-strong": streamSkopeColors.light.divider,
  "--streamskope-plot-grid": streamSkopeColors.light.divider,
  "--streamskope-plot-primary": streamSkopeColors.light.primary.main,
  "--streamskope-plot-secondary": streamSkopeColors.light.info.main,
  "--streamskope-plot-tertiary": streamSkopeColors.light.success.main,
  "--streamskope-scroll-thumb": "rgba(0, 0, 0, 0.18)",
  "--streamskope-scroll-track": "transparent",
  "--streamskope-selection-soft": streamSkopeColors.light.action.selected,
  "--streamskope-signal": streamSkopeColors.light.primary.main,
  "--streamskope-nav-background": streamSkopeColors.light.navigation.background,
  "--streamskope-nav-border": streamSkopeColors.light.navigation.border,
  "--streamskope-nav-muted": streamSkopeColors.light.navigation.muted,
  "--streamskope-nav-selected": streamSkopeColors.light.navigation.selected,
  "--streamskope-nav-text": streamSkopeColors.light.navigation.text,
  "--streamskope-surface-raised": streamSkopeColors.light.background.paper,
  "--streamskope-surface-recessed": streamSkopeColors.light.background.default,
};

const darkVariables = {
  ...streamSkopeCssVariables,
  "--streamskope-divider-strong": streamSkopeColors.dark.divider,
  "--streamskope-plot-grid": streamSkopeColors.dark.divider,
  "--streamskope-plot-primary": streamSkopeColors.dark.primary.main,
  "--streamskope-plot-secondary": streamSkopeColors.dark.info.main,
  "--streamskope-plot-tertiary": streamSkopeColors.dark.success.main,
  "--streamskope-scroll-thumb": "rgba(255, 255, 255, 0.16)",
  "--streamskope-scroll-track": "transparent",
  "--streamskope-selection-soft": streamSkopeColors.dark.action.selected,
  "--streamskope-signal": streamSkopeColors.dark.primary.main,
  "--streamskope-nav-background": streamSkopeColors.dark.navigation.background,
  "--streamskope-nav-border": streamSkopeColors.dark.navigation.border,
  "--streamskope-nav-muted": streamSkopeColors.dark.navigation.muted,
  "--streamskope-nav-selected": streamSkopeColors.dark.navigation.selected,
  "--streamskope-nav-text": streamSkopeColors.dark.navigation.text,
  "--streamskope-surface-raised": streamSkopeColors.dark.background.paper,
  "--streamskope-surface-recessed": streamSkopeColors.dark.background.default,
};

export const streamSkopeTheme = createTheme({
  colorSchemes: streamSkopeColorSchemes,
  components: {
    MuiAccordion: {
      defaultProps: { disableGutters: true, elevation: 0 },
      styleOverrides: {
        root: {
          backgroundColor: "transparent",
          borderColor: "var(--mui-palette-divider)",
          borderRadius: streamSkopeRadius.control,
          "&::before": { display: "none" },
        },
      },
    },
    MuiAccordionDetails: { styleOverrides: { root: { padding: "8px 12px 12px" } } },
    MuiAccordionSummary: {
      styleOverrides: {
        content: { marginBlock: 6, "&.Mui-expanded": { marginBlock: 6 } },
        root: {
          minHeight: streamSkopeGeometry.controlHeight,
          paddingInline: streamSkopeSpacing.scale.space12,
          "&.Mui-expanded": { minHeight: streamSkopeGeometry.controlHeight },
        },
      },
    },
    MuiAlert: {
      styleOverrides: {
        action: { alignItems: "center", marginRight: 0, paddingBlock: 0 },
        icon: {
          fontSize: toRem(streamSkopeTypography.iconSize.inline),
          marginRight: 8,
          paddingBlock: 1,
        },
        root: {
          ...compactTypography,
          alignItems: "flex-start",
          backgroundColor: "var(--mui-palette-background-paper)",
          border: "1px solid var(--mui-palette-divider)",
          borderLeftWidth: 3,
          borderRadius: streamSkopeRadius.control,
          padding: "6px 10px",
          "&.MuiAlert-colorError": { borderLeftColor: "var(--mui-palette-error-main)" },
          "&.MuiAlert-colorInfo": { borderLeftColor: "var(--mui-palette-info-main)" },
          "&.MuiAlert-colorSuccess": { borderLeftColor: "var(--mui-palette-success-main)" },
          "&.MuiAlert-colorWarning": { borderLeftColor: "var(--mui-palette-warning-main)" },
        },
      },
    },
    MuiAlertTitle: { styleOverrides: { root: { ...labelTypography, marginBottom: 2 } } },
    MuiAppBar: {
      defaultProps: { elevation: 0 },
      styleOverrides: {
        root: {
          backgroundColor: "var(--mui-palette-background-paper)",
          backgroundImage: "none",
          color: "var(--mui-palette-text-primary)",
        },
      },
    },
    MuiButton: {
      defaultProps: { disableElevation: true, size: "small" },
      styleOverrides: {
        contained: { boxShadow: "none", "&:hover": { boxShadow: "none" } },
        root: {
          ...labelTypography,
          borderRadius: streamSkopeRadius.control,
          paddingInline: streamSkopeSpacing.scale.space8,
          textTransform: streamSkopeTypography.buttonTextTransform,
        },
      },
    },
    MuiButtonBase: {
      defaultProps: { disableRipple: true },
      styleOverrides: {
        root: {
          "&.Mui-focusVisible": {
            outline: "2px solid var(--mui-palette-primary-main)",
            outlineOffset: 2,
          },
        },
      },
    },
    MuiCheckbox: { defaultProps: { size: "small" } },
    MuiChip: {
      defaultProps: { size: "small" },
      styleOverrides: {
        root: {
          ...metadataTypography,
          borderRadius: streamSkopeRadius.control,
          height: 22,
        },
      },
    },
    MuiCssBaseline: {
      styleOverrides: {
        ":root": lightVariables,
        '[data-mui-color-scheme="dark"]': darkVariables,
        '[data-mui-color-scheme="light"]': lightVariables,
        "*": {
          boxSizing: "border-box",
          scrollbarColor: "var(--streamskope-scroll-thumb) var(--streamskope-scroll-track)",
          scrollbarWidth: "thin",
        },
        "*::-webkit-scrollbar": { height: 8, width: 8 },
        "*::-webkit-scrollbar-thumb": {
          backgroundClip: "padding-box",
          backgroundColor: "var(--streamskope-scroll-thumb)",
          border: "2px solid transparent",
          borderRadius: 8,
        },
        "::selection": {
          backgroundColor: "var(--streamskope-selection-soft)",
          color: "var(--mui-palette-text-primary)",
        },
        html: { fontSize: `${String(streamSkopeTypography.rootSize)}px` },
        body: { fontSynthesis: "none", textRendering: "optimizeLegibility" },
        ".MuiDataGrid-root": {
          "--DataGrid-containerBackground": "var(--mui-palette-background-default)",
          backgroundColor: "var(--mui-palette-background-paper)",
          border: "none",
        },
        ".MuiDataGrid-columnHeader": {
          ...metadataTypography,
          backgroundColor: "var(--mui-palette-background-default)",
          color: "var(--mui-palette-text-secondary)",
        },
        ".MuiDataGrid-columnSeparator": { color: "transparent" },
        ".MuiDataGrid-cell:focus, .MuiDataGrid-columnHeader:focus": { outline: "none" },
        ".MuiDataGrid-cell:focus-visible, .MuiDataGrid-columnHeader:focus-visible": {
          outline: "2px solid var(--mui-palette-primary-main)",
          outlineOffset: "-2px",
        },
        ".MuiDataGrid-cell": {
          ...compactTypography,
          borderColor: "var(--mui-palette-divider)",
        },
        ".MuiDataGrid-row.Mui-selected": {
          backgroundColor: "var(--mui-palette-action-selected)",
          boxShadow: "inset 2px 0 0 var(--mui-palette-primary-main)",
        },
        "@media (prefers-reduced-motion: reduce)": {
          "*, *::before, *::after": {
            animationDuration: "0.01ms !important",
            animationIterationCount: "1 !important",
            scrollBehavior: "auto !important",
            transitionDuration: "0.01ms !important",
          },
        },
      },
    },
    MuiDialog: {
      defaultProps: { fullWidth: true },
      styleOverrides: {
        paper: {
          border: "1px solid var(--mui-palette-divider)",
          borderRadius: streamSkopeRadius.surface,
          maxHeight: "calc(100% - 32px)",
        },
      },
    },
    MuiDialogActions: {
      styleOverrides: {
        root: {
          borderTop: "1px solid var(--mui-palette-divider)",
          gap: streamSkopeSpacing.scale.space4,
          padding: "8px 12px",
        },
      },
    },
    MuiDialogContent: { styleOverrides: { root: { padding: streamSkopeSpacing.scale.space12 } } },
    MuiDialogTitle: {
      styleOverrides: {
        root: {
          ...typography(streamSkopeTypography.roles.panelTitle),
          padding: "10px 12px 8px",
        },
      },
    },
    MuiDrawer: {
      styleOverrides: {
        paper: {
          backgroundColor: "var(--mui-palette-background-paper)",
          backgroundImage: "none",
          borderColor: "var(--mui-palette-divider)",
        },
      },
    },
    MuiFormControl: { defaultProps: { size: "small" } },
    MuiFormHelperText: { styleOverrides: { root: metadataTypography } },
    MuiFormLabel: { styleOverrides: { root: compactTypography } },
    MuiIconButton: {
      defaultProps: { size: "small" },
      styleOverrides: {
        root: {
          ...controlTypography,
          borderRadius: streamSkopeRadius.control,
          height: streamSkopeGeometry.controlHeight,
          width: streamSkopeGeometry.controlHeight,
        },
      },
    },
    MuiInputBase: {
      styleOverrides: {
        input: {
          ...controlTypography,
          "&:not(textarea)": {
            boxSizing: "border-box",
            height: "100%",
            paddingBlock: 0,
            paddingInline: streamSkopeGeometry.resourceTabInline,
          },
        },
        root: controlTypography,
      },
    },
    MuiInputLabel: {
      styleOverrides: {
        root: {
          ...compactTypography,
          "&.MuiInputLabel-outlined:not(.MuiInputLabel-shrink)": {
            transform: `translate(14px, ${(streamSkopeGeometry.controlHeight - streamSkopeTypography.roles.compact.lineHeight) / 2}px) scale(1)`,
          },
        },
      },
    },
    MuiListItemButton: {
      defaultProps: { dense: true },
      styleOverrides: {
        root: {
          ...controlTypography,
          borderRadius: streamSkopeRadius.control,
          margin: "1px 6px",
          minHeight: streamSkopeGeometry.resourceRowHeight,
          paddingBlock: 3,
        },
      },
    },
    MuiMenu: {
      styleOverrides: {
        paper: {
          border: "1px solid var(--mui-palette-divider)",
          borderRadius: streamSkopeRadius.control,
        },
      },
    },
    MuiMenuItem: { styleOverrides: { root: controlTypography } },
    MuiOutlinedInput: {
      styleOverrides: {
        root: {
          backgroundColor: "var(--mui-palette-background-paper)",
          borderRadius: streamSkopeRadius.control,
          minHeight: streamSkopeGeometry.controlHeight,
          "&:not(.MuiInputBase-multiline)": { height: streamSkopeGeometry.controlHeight },
        },
      },
    },
    MuiPaper: {
      defaultProps: { elevation: 0 },
      styleOverrides: { root: { backgroundImage: "none" } },
    },
    MuiSelect: {
      defaultProps: { size: "small" },
      styleOverrides: { select: { alignItems: "center", display: "flex" } },
    },
    MuiSwitch: { defaultProps: { size: "small" } },
    MuiSvgIcon: {
      styleOverrides: {
        root: {
          fontSize: toRem(streamSkopeTypography.iconSize.inline),
        },
      },
    },
    MuiTab: {
      styleOverrides: {
        root: {
          ...labelTypography,
          color: "var(--mui-palette-text-secondary)",
          minHeight: streamSkopeGeometry.workspaceTabsHeight,
          padding: "6px 12px",
          textTransform: streamSkopeTypography.buttonTextTransform,
          "&.Mui-selected": { color: "var(--mui-palette-text-primary)" },
        },
      },
    },
    MuiTable: { defaultProps: { size: "small" } },
    MuiTableCell: {
      styleOverrides: {
        body: compactTypography,
        head: {
          ...metadataTypography,
          backgroundColor: "var(--mui-palette-background-default)",
          color: "var(--mui-palette-text-secondary)",
        },
        root: { borderColor: "var(--mui-palette-divider)", padding: "6px 8px" },
      },
    },
    MuiTabs: {
      styleOverrides: {
        indicator: {
          backgroundColor: "var(--mui-palette-text-primary)",
          borderRadius: "2px 2px 0 0",
          height: 2,
        },
        root: { minHeight: streamSkopeGeometry.workspaceTabsHeight },
      },
    },
    MuiTextField: { defaultProps: { size: "small" } },
    MuiToggleButton: {
      defaultProps: { size: "small" },
      styleOverrides: {
        root: {
          ...labelTypography,
          color: "var(--mui-palette-text-primary)",
          minHeight: streamSkopeGeometry.controlHeight,
          paddingInline: streamSkopeSpacing.scale.space8,
          paddingBlock:
            (streamSkopeGeometry.controlHeight - streamSkopeTypography.roles.label.lineHeight - 2) /
            2,
          textTransform: streamSkopeTypography.buttonTextTransform,
          "&.Mui-selected": {
            backgroundColor: "var(--mui-palette-primary-dark)",
            color: "var(--mui-palette-primary-contrastText)",
          },
          "&.Mui-selected:hover": {
            backgroundColor: "var(--mui-palette-primary-main)",
          },
        },
      },
    },
    MuiToolbar: { styleOverrides: { dense: { minHeight: streamSkopeGeometry.commandBarHeight } } },
    MuiTooltip: {
      defaultProps: { arrow: true, enterDelay: 450 },
      styleOverrides: { tooltip: metadataTypography },
    },
  },
  cssVariables: { colorSchemeSelector: "data-mui-color-scheme" },
  shape: { borderRadius: streamSkopeRadius.control },
  spacing: streamSkopeSpacing.baseUnit,
  typography: {
    body1: bodyTypography,
    body2: compactTypography,
    button: { ...labelTypography, textTransform: streamSkopeTypography.buttonTextTransform },
    caption: metadataTypography,
    fontFamily: interfaceFontFamily,
    fontSize: streamSkopeTypography.roles.body.size,
    fontWeightBold: 600,
    fontWeightMedium: 500,
    fontWeightRegular: 400,
    h1: typography(streamSkopeTypography.roles.appTitle),
    h2: typography(streamSkopeTypography.roles.appTitle),
    h3: typography(streamSkopeTypography.roles.appTitle),
    h4: typography(streamSkopeTypography.roles.appTitle),
    h5: typography(streamSkopeTypography.roles.pageTitle),
    h6: typography(streamSkopeTypography.roles.appTitle),
    htmlFontSize: streamSkopeTypography.rootSize,
    overline: {
      ...typography(streamSkopeTypography.roles.sectionLabel),
      letterSpacing: streamSkopeTypography.sectionLabelTracking,
      textTransform: streamSkopeTypography.sectionLabelTransform,
    },
    subtitle1: typography(streamSkopeTypography.roles.panelTitle),
    subtitle2: typography(streamSkopeTypography.roles.sectionTitle),
  },
});

export {
  streamSkopeColors,
  streamSkopeGeometry,
  streamSkopeRadius,
  streamSkopeSpacing,
  streamSkopeTypography,
};
