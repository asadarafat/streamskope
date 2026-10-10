import { expect, it } from "vitest";

import { KafkaApplicationSession } from "../../src/features/kafka/application";
import { ExternalMetricsService } from "../../src/features/kafka/application/external-metrics-service";
import { PrometheusMetricsAdapter } from "../../src/features/kafka/engine/prometheus-metrics";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import { startPrometheusFixture } from "../support/prometheus-fixture";

it("reads actual JMX partition gauges with actual Prometheus scrape times, observes ISR loss and recovery, and keeps missing exporters unknown", async () => {
  const fixture = await startPrometheusFixture();
  const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
  const service = new ExternalMetricsService(
    () => session.metricsScope(),
    new PrometheusMetricsAdapter(new NodeBoundedJsonHttp()),
  );
  const selection = {
    endpoint: fixture.endpoint,
    job: fixture.job,
    instance: fixture.broker.metricsInstances[0]!,
  };
  try {
    await fixture.broker.admin.createTopics({
      topics: ["metrics-replicated"],
      replicas: -1,
      partitions: -1,
      assignments: [{ partition: 0, brokers: [1, 2] }],
    });
    await session.connect({
      ...fixture.broker.connection,
      services: { metrics: { baseUrl: fixture.endpoint, authentication: "none" } },
    });
    await expect
      .poll(async () => (await service.capture(selection)).metrics[0]?.value, {
        timeout: 30_000,
        interval: 1000,
      })
      .toBe(0);
    const zero = await service.capture(selection);
    expect(zero).toMatchObject({
      source: "prometheus",
      identity: "not-established",
      endpoint: fixture.endpoint,
    });
    expect(zero.metrics[0]).toMatchObject({ state: "ready", value: 0 });
    expect(zero.metrics[1]).toMatchObject({ state: "ready", value: 0 });
    expect(zero.metrics[0]!.scrapedAt).toBeLessThanOrEqual(zero.collectedAt);
    expect(zero.collectedAt - zero.metrics[0]!.scrapedAt!).toBeLessThan(45_000);
    await fixture.broker.stopBroker(2);
    await expect
      .poll(async () => (await service.capture(selection)).metrics[0]?.value, {
        timeout: 30_000,
        interval: 1000,
      })
      .toBe(1);
    await fixture.broker.startBroker(2);
    await expect
      .poll(async () => (await service.capture(selection)).metrics[0]?.value, {
        timeout: 30_000,
        interval: 1000,
      })
      .toBe(0);
    const missing = await service.capture({ ...selection, instance: "unconfigured-exporter:9404" });
    expect(
      missing.metrics.every(
        (m) => m.state === "missing" && m.value === null && m.scrapedAt === null,
      ),
    ).toBe(true);
    await session.disconnect();
    await expect(service.capture(selection)).rejects.toMatchObject({
      code: "UNSUPPORTED_OPERATION",
    });
  } finally {
    service.cancel();
    await service.idle();
    await session.shutdown();
    await fixture.dispose();
  }
}, 180_000);
