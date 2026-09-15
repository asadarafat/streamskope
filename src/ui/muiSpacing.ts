import { streamSkopeSpacing } from "./spacingContract";

function toMuiFactor(pixels: number): number {
  return pixels / streamSkopeSpacing.baseUnit;
}

function mapValues<T extends Record<string, number>>(
  values: T,
): { readonly [Key in keyof T]: number } {
  return Object.freeze(
    Object.fromEntries(Object.entries(values).map(([name, pixels]) => [name, toMuiFactor(pixels)])),
  ) as { readonly [Key in keyof T]: number };
}

export const streamSkopeMuiSpacingBase = streamSkopeSpacing.baseUnit;
export const streamSkopeLayoutSpacing = mapValues(streamSkopeSpacing.roles);
export const streamSkopeSpace = mapValues(streamSkopeSpacing.scale);

/** Studio component names are retained for source-parity ports. */
export const studioMuiSpacingBase = streamSkopeMuiSpacingBase;
export const studioLayoutSpacing = streamSkopeLayoutSpacing;
export const studioSpace = streamSkopeSpace;
