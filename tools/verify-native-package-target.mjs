import assert from "node:assert/strict";
import process from "node:process";

const args = process.argv.slice(2);
assert.ok(
  args.length === 0 || args.length === 2,
  "Usage: verify-native-package-target.mjs [platform architecture]",
);
const [platform, arch] =
  args.length === 2
    ? args
    : [
        process.env.EXPECTED_PLATFORM ?? process.platform,
        process.env.EXPECTED_ARCH ?? process.arch,
      ];
assert.equal(
  process.platform,
  platform,
  `Native package platform mismatch: run this target on ${platform}`,
);
assert.equal(process.arch, arch, `Native package architecture mismatch: use ${arch} Node.js`);
process.stdout.write(`Verified native target: ${process.platform}-${process.arch}\n`);
