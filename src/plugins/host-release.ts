import packageMetadata from "../../package.json";

import { compareDesktopReleases } from "./compatibility";

const release = packageMetadata.streamskopeRelease;
compareDesktopReleases(release, release);
if (release.split("+")[0] !== `v${packageMetadata.version}`) {
  throw new Error("StreamSkope release metadata does not match the desktop package version.");
}

/** Exact desktop build used when filtering, installing and loading plugin packages. */
export const STREAMSKOPE_RELEASE = release;
