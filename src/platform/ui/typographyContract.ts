export interface StreamSkopeTypographyRole {
  readonly lineHeight: number;
  readonly size: number;
  readonly weight: 400 | 500 | 600;
}

function role(
  size: number,
  lineHeight: number,
  weight: StreamSkopeTypographyRole["weight"],
): Readonly<StreamSkopeTypographyRole> {
  return Object.freeze({ lineHeight, size, weight });
}

const interfaceRoles = Object.freeze({
  appTitle: role(18, 24, 600),
  body: role(13, 20, 400),
  compact: role(12, 18, 400),
  control: role(13, 18, 400),
  label: role(13, 18, 600),
  metadata: role(12, 17, 400),
  pageTitle: role(24, 32, 600),
  panelTitle: role(15, 22, 600),
  sectionLabel: role(12, 18, 600),
  sectionTitle: role(14, 20, 600),
});

const monospace = role(12, interfaceRoles.compact.lineHeight, 400);

/**
 * One compact desktop type scale for the complete renderer. The system stack
 * owns interface language; the single monospace role is reserved for technical
 * evidence. A 16 px root keeps browser and accessibility scaling predictable.
 */
export const streamSkopeTypography = Object.freeze({
  buttonTextTransform: "none" as const,
  family: Object.freeze({
    interface: 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
    monospace:
      'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
  }),
  iconSize: Object.freeze({
    compact: 14,
    inline: 16,
  }),
  letterSpacing: 0,
  roles: interfaceRoles,
  rootSize: 16,
  sectionLabelTracking: 0,
  sectionLabelTransform: "none" as const,
  monospace,
});
