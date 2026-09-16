import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { expect, it } from "vitest";

const cwd = process.env.GIT_WORK_TREE ?? fileURLToPath(new URL("../../", import.meta.url));

it("does not track ignored private or generated content", () => {
  const tracked = execFileSync("git", ["ls-files", "-ci", "--exclude-standard"], {
    cwd,
    encoding: "utf8",
  });
  expect(tracked.trim()).toBe("");
});

it("ignores local credentials and recordings without hiding required public source", () => {
  const ignored = [
    ".codex/private.md",
    "openspec/config.yaml",
    "docs/private.md",
    "docs/development/README.md",
    "website/docs/launch/assets/messages.png",
    "website/.site/index.html",
    "website/promo/node_modules/hyperframes/package.json",
    ".artifacts/run.json",
    ".env",
    ".env.local",
    "local.key",
    "local.pem",
    "local.p12",
    "local.pfx",
    "local.jks",
    "id_rsa",
    "id_ed25519",
    "recording.mp4",
    "recording.webm",
    "capture.mov",
    "aio-kafka/ownership/records/local.json",
    "aio-kafka/config/kafka-broker/certs/ca.key",
  ];
  const publicFiles = [
    ".env.example",
    ".env.test.example",
    "LICENSE",
    "README.md",
    "website/docs/index.md",
    "website/docs/guide/connections.md",
    "website/docs/assets/messages.png",
    "website/docs/assets/streamskope-intro-light.mp4",
    "website/docs/assets/streamskope-intro-dark.mp4",
    "website/docs/launch/index.html",
    "website/promo/package-lock.json",
    "index.html",
    "package-lock.json",
    ".github/workflows/ci.yml",
    "aio-kafka/fixture.config.json",
    "aio-kafka/make-certs.sh",
    "src/platform/ui/assets/streamskope.svg",
    "assets/icons/streamskope.icns",
    "test/unit/public-repository-policy.test.ts",
  ];
  const result = execFileSync("git", ["check-ignore", "--no-index", "--stdin"], {
    cwd,
    encoding: "utf8",
    input: [...ignored, ...publicFiles].join("\n") + "\n",
  });
  expect(result.trim().split("\n").sort()).toEqual([...ignored].sort());
});
