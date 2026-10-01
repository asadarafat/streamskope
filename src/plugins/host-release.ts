import packageMetadata from "../../package.json";

import { parseReleaseVersion } from "./compatibility";

/** The package version is the sole desktop version; Git tags add a display prefix. */
export const STREAMSKOPE_RELEASE = `v${parseReleaseVersion(packageMetadata.version)}`;
