import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

// Fixture commands run from the repository root. Avoid import.meta because
// Playwright also loads these helpers through its CommonJS transformer.
const topology = resolve("aio-kafka/topology.clab.yml");

/** The interactive lab and every isolated broker use the same image pin. */
export async function kafkaPin(): Promise<{ image: string; version: string }> {
  const image = /image:\s*(apache\/kafka:([\d.]+)@sha256:[a-f0-9]{64})\s/u.exec(
    await readFile(topology, "utf8"),
  );
  if (!image?.[1] || !image[2]) throw new Error("AIO Kafka image must have a version and digest.");
  return { image: image[1], version: image[2] };
}

// Reference-capable schema tests need this Registry variant. The interactive
// TLS/OAuth Registry retains its separately qualified fixture.config.json pin.
export const REFERENCE_REGISTRY_IMAGE =
  "ghcr.io/aiven-open/karapace:6.1.0@sha256:d5d2cb9dfe259ccd4df83553c272c531c2b5309d87340d5b226ab05b607cfd27";
export const PROMETHEUS_IMAGE =
  "prom/prometheus:v3.15.0@sha256:efd719c99d83b060d9daefdcf00360461adf279f45ef5391f8d111892118753e";
export const JMX_AGENT_VERSION = "1.7.0";
export const JMX_AGENT_SHA256 = "0ad843261c567d1924a017d542e14c40f2afff523d940c25ee000742fde4b3a9";
