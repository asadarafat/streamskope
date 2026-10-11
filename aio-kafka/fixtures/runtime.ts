export interface BrokerRuntime {
  readonly metadata: {
    readonly backend: "container" | "native";
    readonly kafkaVersion: string;
    readonly image?: string;
    readonly archiveSha512?: string;
  };
  tool(
    command: string,
    arguments_: readonly string[],
    options: { cwd: string; timeout: number },
  ): Promise<unknown>;
  start(options: {
    directory: string;
    oauthRoot: string;
    clusterId: string;
    storageArguments: readonly string[];
  }): Promise<void>;
  exited(): Promise<boolean>;
  logs(): Promise<string>;
  dispose(): Promise<void>;
}
