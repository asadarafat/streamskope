import packageMetadata from "../../package.json";

import { parseReleaseVersion } from "./compatibility";

/** Source uses a development sentinel; release CI stamps the build's package version. */
export const STREAMSKOPE_RELEASE = `v${parseReleaseVersion(packageMetadata.version)}`;
