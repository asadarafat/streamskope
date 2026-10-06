export interface PluginNetworkTransportConfiguration {
  readonly mode: "system" | "custom";
  readonly proxyUrl?: string;
  readonly credentials?: { readonly username: string; readonly password: string };
}

/** Host-only, dedicated transport. It never changes Kafka, NATS or provider API networking. */
export interface PluginNetworkTransport {
  readonly nativeAvailable: boolean;
  readonly supportedProxyProtocols: readonly ("http" | "https")[];
  readonly fetch: typeof fetch;
  configure(configuration: PluginNetworkTransportConfiguration): Promise<void>;
  close(): Promise<void>;
}
