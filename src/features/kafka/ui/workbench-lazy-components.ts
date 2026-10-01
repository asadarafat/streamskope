import { lazy } from "react";

export const LazyTopicConfigurationWorkspace = lazy(async () => {
  const module = await import("./TopicConfigurationWorkspace");
  return { default: module.TopicConfigurationWorkspace };
});

export const LazyLatencyWorkspace = lazy(async () => {
  const module = await import("./LatencyWorkspace");
  return { default: module.LatencyWorkspace };
});

export const LazyStreamMonitorPanel = lazy(async () => {
  const module = await import("./StreamMonitorPanel");
  return { default: module.StreamMonitorPanel };
});

export const LazyOperationalPreferencesDialog = lazy(async () => {
  const module = await import("./OperationalPreferencesDialog");
  return { default: module.OperationalPreferencesDialog };
});
