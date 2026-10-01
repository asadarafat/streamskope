import { streamSkopeSpacing } from "./spacingContract";

export const streamSkopeCssSpacing = Object.freeze({
  "--streamskope-space-2": `${streamSkopeSpacing.scale.space2}px`,
  "--streamskope-space-4": `${streamSkopeSpacing.scale.space4}px`,
  "--streamskope-space-6": `${streamSkopeSpacing.scale.space6}px`,
  "--streamskope-space-8": `${streamSkopeSpacing.scale.space8}px`,
  "--streamskope-space-10": `${streamSkopeSpacing.scale.space10}px`,
  "--streamskope-space-12": `${streamSkopeSpacing.scale.space12}px`,
  "--streamskope-space-16": `${streamSkopeSpacing.scale.space16}px`,
  "--streamskope-space-24": `${streamSkopeSpacing.scale.space24}px`,
  "--streamskope-space-compact-gap": `${streamSkopeSpacing.roles.compactGap}px`,
  "--streamskope-space-content-gap": `${streamSkopeSpacing.roles.contentGap}px`,
  "--streamskope-space-control-gap": `${streamSkopeSpacing.roles.controlGap}px`,
  "--streamskope-space-editor-inset": `${streamSkopeSpacing.roles.editorInset}px`,
  "--streamskope-space-panel-inline": `${streamSkopeSpacing.roles.panelInline}px`,
  "--streamskope-space-section-gap": `${streamSkopeSpacing.roles.sectionGap}px`,
});
