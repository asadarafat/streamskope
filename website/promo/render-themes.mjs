import { cp, mkdir, readFile, writeFile, copyFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import process from "node:process";
import { URL, fileURLToPath } from "node:url";
import path from "node:path";

const root = fileURLToPath(new URL("../../", import.meta.url));
const staging = path.join(root, ".artifacts/website/energetic-intro-render");
const composition = await readFile(path.join(root, "website/promo/intro/index.html"), "utf8");
const inputs = {
  light:
    process.env.STREAMSKOPE_INTRO_LIGHT_ASSETS ||
    path.join(root, ".artifacts/website/header-refresh/light/assets"),
  dark:
    process.env.STREAMSKOPE_INTRO_DARK_ASSETS ||
    path.join(root, ".artifacts/website/header-refresh/dark/assets"),
};
function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, stdio: "inherit", env: process.env });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} failed (${result.status})`);
}
// The same original score is generated once and shared by both themes.
run("python3", ["website/promo/create-score.py"]);
const outputs = [];
for (const theme of ["light", "dark"]) {
  const directory = path.join(staging, theme);
  await mkdir(path.join(directory, "assets"), { recursive: true });
  // Require the approved real recordings, including the soundtrack and reading holds.
  for (const asset of new Set(
    [...composition.matchAll(/src="assets\/([^"]+)"/g)].map((match) => match[1]),
  )) {
    try {
      const source =
        asset === "energetic-score.m4a"
          ? path.join(root, ".artifacts/website/energetic-score", asset)
          : path.join(inputs[theme], asset);
      await copyFile(source, path.join(directory, "assets", asset));
    } catch (error) {
      throw new Error(
        `Missing approved ${theme} capture asset: ${asset}. Set STREAMSKOPE_INTRO_${theme.toUpperCase()}_ASSETS to the retained capture directory.`,
        { cause: error },
      );
    }
  }
  const html = composition.replace("<html>", `<html data-theme="${theme}">`);
  await writeFile(path.join(directory, "index.html"), html);
  if (process.argv.includes("--prepare-only")) continue;
  const output = path.join(directory, `streamskope-intro-${theme}.mp4`);
  run(path.join(root, "website/promo/node_modules/.bin/hyperframes"), [
    "render",
    directory,
    "--output",
    output,
    "--fps",
    "60",
    "--workers",
    "2",
    "--no-browser-gpu",
    "--video-frame-format",
    "png",
    "--quality",
    "draft",
    "--crf",
    "18",
    "--no-best-effort",
    "--frames-cache-dir",
    "off",
  ]);
  const probe = spawnSync(
    "ffprobe",
    ["-v", "error", "-show_streams", "-show_format", "-of", "json", output],
    { encoding: "utf8" },
  );
  if (probe.status !== 0) throw new Error(`Cannot verify ${theme} export`);
  const metadata = JSON.parse(probe.stdout);
  const video = metadata.streams.find((stream) => stream.codec_type === "video");
  if (
    video.width !== 3840 ||
    video.height !== 2160 ||
    video.avg_frame_rate !== "60/1" ||
    Number(video.nb_frames) !== 2160 ||
    Math.abs(Number(metadata.format.duration) - 36) > 0.05
  )
    throw new Error(`${theme} export does not match the approved 36-second 4K/60 intro`);
  if (!metadata.streams.some((stream) => stream.codec_type === "audio"))
    throw new Error(`${theme} export is missing its score`);
  run("ffmpeg", ["-v", "error", "-i", output, "-f", "null", "-"]);
  const poster = path.join(directory, `streamskope-intro-${theme}.png`);
  run("ffmpeg", ["-v", "error", "-y", "-ss", "1", "-i", output, "-frames:v", "1", poster]);
  outputs.push([poster, path.join(root, `website/docs/assets/streamskope-intro-${theme}.png`)]);
  outputs.push([output, path.join(root, `website/docs/assets/streamskope-intro-${theme}.mp4`)]);
}
// Replace published assets only once both matching exports have passed verification.
for (const [source, destination] of outputs) await cp(source, destination);
