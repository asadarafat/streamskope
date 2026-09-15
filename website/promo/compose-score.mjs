// Original synthesized score: restrained tension, a tonal reveal, then resolution.
// No samples or third-party recordings. Licensed with StreamSkope (Apache-2.0).
import { spawnSync } from "node:child_process";
import { fileURLToPath, URL } from "node:url";
import { Buffer } from "node:buffer";
import { readFileSync } from "node:fs";

const rate = 48000;
const seconds = 60;
const samples = new Float32Array(rate * seconds * 2);
const tau = Math.PI * 2;
const hz = (note) => 440 * 2 ** ((note - 69) / 12);
let seed = 1842;
let lowNoise = 0;
const composition = readFileSync(new URL("../docs/launch/index.html", import.meta.url), "utf8");
const cuts = [
  ...new Set(
    [...composition.matchAll(/data-start="(\d+)"/g)]
      .map((match) => Number(match[1]))
      .filter((value) => value > 0),
  ),
];
for (let frame = 0; frame < rate * seconds; frame++) {
  const t = frame / rate;
  seed = (1664525 * seed + 1013904223) >>> 0;
  const noise = seed / 2147483648 - 1;
  lowNoise += 0.025 * (noise - lowNoise);
  const resolved = t >= 21;
  const beat = resolved ? 0.625 : 1.25;
  const phase = (resolved ? t - 21 : t) % beat;
  const beatNumber = Math.floor((resolved ? t - 21 : t) / beat);
  const melody = resolved ? [64, 71, 68, 76, 71, 68, 66, 71] : [64, 71, 67, 74];
  const note = hz(melody[beatNumber % melody.length]);
  const attack = Math.min(1, phase / 0.012);
  const envelope = attack * Math.exp(-phase * (resolved ? 3.1 : 4.4));
  const pluck =
    envelope *
    (Math.sin(tau * note * phase) + 0.2 * Math.sin(tau * note * 2 * phase)) *
    (resolved ? 0.095 : 0.055);
  const kickPhase = resolved ? (t - 21) % 1.25 : t % 2.5;
  const kick =
    Math.sin(tau * (48 * kickPhase + 8 * (1 - Math.exp(-kickPhase * 30)))) *
    Math.exp(-kickPhase * 14) *
    0.14;
  const chordIndex = resolved ? Math.min(3, Math.floor((t - 21) / 8)) : 0;
  const chord = resolved
    ? [
        [40, 47, 56],
        [36, 43, 52],
        [43, 50, 59],
        [40, 47, 56],
      ][chordIndex]
    : [40, 47, 55];
  const padEnvelope = Math.min(1, t / 3) * (0.75 + 0.25 * Math.sin(tau * 0.08 * t));
  let left = 0;
  let right = 0;
  for (const pitch of chord) {
    const frequency = hz(pitch);
    left += Math.sin(tau * frequency * t) * 0.026 * padEnvelope;
    right += Math.sin(tau * frequency * 1.0007 * t) * 0.026 * padEnvelope;
  }
  let transition = 0;
  for (const cut of cuts) {
    const before = cut - t;
    if (before > 0 && before < 1.1) transition += lowNoise * (1 - before / 1.1) ** 2 * 0.32;
    const after = t - cut;
    if (after >= 0 && after < 2)
      transition +=
        Math.sin(tau * 41.2 * after) * Math.exp(-after * 3) * Math.min(1, after / 0.008) * 0.17;
  }
  const tick = noise * Math.exp(-phase * 130) * 0.02;
  const fade = Math.min(1, t / 0.15, Math.max(0, (seconds - t) / 3));
  samples[frame * 2] = Math.tanh(left + pluck + kick + transition + tick) * fade;
  samples[frame * 2 + 1] = Math.tanh(right + pluck * 0.88 + kick + transition + tick) * fade;
}
const output = fileURLToPath(new URL("../docs/assets/launch-score.mp3", import.meta.url));
const result = spawnSync(
  "ffmpeg",
  [
    "-v",
    "error",
    "-y",
    "-f",
    "f32le",
    "-ar",
    String(rate),
    "-ac",
    "2",
    "-i",
    "pipe:0",
    "-af",
    "loudnorm=I=-20:TP=-2:LRA=8",
    "-ar",
    String(rate),
    "-c:a",
    "libmp3lame",
    "-b:a",
    "128k",
    "-t",
    String(seconds),
    output,
  ],
  { input: Buffer.from(samples.buffer), encoding: "utf8", maxBuffer: 1024 * 1024 },
);
if (result.error || result.status !== 0) throw new Error(result.error?.message ?? result.stderr);
