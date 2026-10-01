import { streamSkopeCssSpacing } from "./cssSpacing";
import { streamSkopeCssGeometry } from "./studioTokens";

export const streamSkopeCssVariables = Object.freeze({
  ...streamSkopeCssGeometry,
  ...streamSkopeCssSpacing,
});
