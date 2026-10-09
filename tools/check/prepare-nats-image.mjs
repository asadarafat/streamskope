import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { NATS_SERVER_IMAGES } from "../dev/nats-fixture/definition.ts";

// Keep public dependency setup visible without reading registry credentials or helpers.
const configuration = await mkdtemp(join(tmpdir(), "streamskope-public-image-"));
try {
  await writeFile(join(configuration, "config.json"), "{}\n", { mode: 0o600 });
  execFileSync(
    "docker",
    ["--config", configuration, "pull", "--platform", "linux/amd64", NATS_SERVER_IMAGES.x64],
    { stdio: "inherit", timeout: 120_000 },
  );
} finally {
  await rm(configuration, { recursive: true, force: true });
}
