import { streamSkopeSpacing } from "./spacingContract";
import { streamSkopeTypography } from "./typographyContract";

/** Canonical geometry for the StreamSkope desktop shell and feature surfaces. */
export const streamSkopeGeometry = Object.freeze({
  activityCollapsedHeight: 36,
  activityHeight: 240,
  activityMaximumHeight: 420,
  activityMinimumHeight: 160,
  activitySeparatorHeight: 9,
  brandIconSize: 19,
  brandMarkSize: 28,
  breadcrumbBarHeight: 40,
  canvasMinimumWidth: 420,
  commandBarHeight: 52,
  contextBarHeight: 52,
  // Natural small button: label line height plus 4 px padding above and below.
  controlHeight: streamSkopeTypography.roles.label.lineHeight + streamSkopeSpacing.scale.space8,
  fullDesktopMinimumWidth: 900,
  inspectorDefaultWidth: 320,
  inspectorMaximumWidth: 560,
  inspectorMinimumWidth: 292,
  messageInspectorFullColumnsMinimumWidth: 1200,
  localToolbarHeight: 44,
  navigatorMaximumWidth: 360,
  navigatorMinimumWidth: 200,
  navigatorWidth: 228,
  profileRowHeight: 44,
  resourceHeaderHeight: 52,
  resourceDisclosureHeight: 36,
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
  resizerWidth: 5,
  statusBarHeight: 26,
  tableHeaderHeight: 40,
  tableRowHeight: 40,
  topicRowHeight: 40,
  topicListInset: 12,
  topicListMargin: 4,
  workspaceTabWidth: 190,
  workspaceTabsHeight: 40,
});

export const streamSkopeRadius = Object.freeze({
  control: 8,
  round: 999,
  surface: 10,
});

export const streamSkopeLayer = Object.freeze({
  content: 1,
  inlineAction: 2,
  panel: 20,
  panelResizer: 24,
  drawer: 80,
  scrim: 100,
});

export const streamSkopeCssGeometry = Object.freeze({
  "--streamskope-activity-collapsed-height": `${streamSkopeGeometry.activityCollapsedHeight}px`,
  "--streamskope-activity-height": `${streamSkopeGeometry.activityHeight}px`,
  "--streamskope-activity-maximum-height": `${streamSkopeGeometry.activityMaximumHeight}px`,
  "--streamskope-activity-minimum-height": `${streamSkopeGeometry.activityMinimumHeight}px`,
  "--streamskope-activity-separator-height": `${streamSkopeGeometry.activitySeparatorHeight}px`,
  "--streamskope-breadcrumb-bar-height": `${streamSkopeGeometry.breadcrumbBarHeight}px`,
  "--streamskope-command-bar-height": `${streamSkopeGeometry.commandBarHeight}px`,
  "--streamskope-context-bar-height": `${streamSkopeGeometry.contextBarHeight}px`,
  "--streamskope-control-height": `${streamSkopeGeometry.controlHeight}px`,
  "--streamskope-local-toolbar-height": `${streamSkopeGeometry.localToolbarHeight}px`,
  "--streamskope-navigator-width": `${streamSkopeGeometry.navigatorWidth}px`,
  "--streamskope-profile-row-height": `${streamSkopeGeometry.profileRowHeight}px`,
  "--streamskope-resource-header-height": `${streamSkopeGeometry.resourceHeaderHeight}px`,
  "--streamskope-resource-disclosure-height": `${streamSkopeGeometry.resourceDisclosureHeight}px`,
  "--streamskope-resource-icon-box-size": `${streamSkopeGeometry.resourceIconBoxSize}px`,
  "--streamskope-resource-icon-size": `${streamSkopeGeometry.resourceIconSize}px`,
  "--streamskope-resource-panel-inline": `${streamSkopeGeometry.resourcePanelInline}px`,
  "--streamskope-resource-row-height": `${streamSkopeGeometry.resourceRowHeight}px`,
  "--streamskope-resource-search-height": `${streamSkopeGeometry.resourceSearchHeight}px`,
  "--streamskope-resource-tab-dot-size": `${streamSkopeGeometry.resourceTabDotSize}px`,
  "--streamskope-resource-tab-gap": `${streamSkopeGeometry.resourceTabGap}px`,
  "--streamskope-resource-tab-height": `${streamSkopeGeometry.resourceTabHeight}px`,
  "--streamskope-resource-tab-inline": `${streamSkopeGeometry.resourceTabInline}px`,
  "--streamskope-resource-tabs-height": `${streamSkopeGeometry.resourceTabsHeight}px`,
  "--streamskope-status-bar-height": `${streamSkopeGeometry.statusBarHeight}px`,
  "--streamskope-topic-row-height": `${streamSkopeGeometry.topicRowHeight}px`,
  "--streamskope-topic-list-inset": `${streamSkopeGeometry.topicListInset}px`,
  "--streamskope-topic-list-margin": `${streamSkopeGeometry.topicListMargin}px`,
  "--streamskope-workspace-tab-width": `${streamSkopeGeometry.workspaceTabWidth}px`,
  "--streamskope-workspace-tabs-height": `${streamSkopeGeometry.workspaceTabsHeight}px`,
});

/** Legacy aliases are retained while existing feature components migrate. */
export const studioGeometry = streamSkopeGeometry;
export const studioLayer = streamSkopeLayer;
export const studioRadius = streamSkopeRadius;
