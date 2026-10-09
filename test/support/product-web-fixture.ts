import {
  launchWebDevelopment,
  type RunningWebDevelopment,
  type WebDevelopmentLaunchOptions,
} from "../../src/platform/dev-host";
import { createKafkaProviderEndpoint } from "../../src/platform/node/kafka-provider";
import { createNatsBackend } from "../../src/platform/node/nats-backend";
import { createNatsProviderEndpoint } from "../../src/platform/node/nats-provider";
import { ProviderHostRegistry } from "../../src/platform/node/provider-host";

export type { DevelopmentBackend, RunningWebDevelopment } from "../../src/platform/dev-host";

/** Product browser fixtures expose both built-in providers, including inactive profile inventories. */
export function launchProductWebFixture(
  options: WebDevelopmentLaunchOptions,
): Promise<RunningWebDevelopment> {
  if (options.providers !== undefined) return launchWebDevelopment(options);
  const { backend, ...launchOptions } = options;
  const pluginAsset = options.pluginAsset ?? backend.pluginAsset?.bind(backend);
  return launchWebDevelopment({
    ...launchOptions,
    providers: new ProviderHostRegistry([
      createKafkaProviderEndpoint(backend),
      createNatsProviderEndpoint(createNatsBackend()),
    ]),
    ...(pluginAsset === undefined ? {} : { pluginAsset }),
    ...(backend.exportFiles === undefined ? {} : { exportFiles: backend.exportFiles }),
  });
}
