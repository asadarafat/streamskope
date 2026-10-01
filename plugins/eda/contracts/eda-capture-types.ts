export const EDA_CAPTURE_DEFAULTS = {
  localPort: 19_092,
  namespace: "eda-system",
  registryPort: 5_443,
  brokerImage: "docker.redpanda.com/redpandadata/redpanda:v24.3.5",
} as const;

export const EDA_CAPTURE_LIMITS = {
  contexts: 64,
  certificateCharacters: 1_500_000,
  credentialCharacters: 4_096,
  hostCharacters: 253,
  imageCharacters: 2_048,
  privateKeyCharacters: 131_072,
  nameCharacters: 253,
  namespaces: 1_000,
  sources: 1_000,
  topics: 1_000,
  urlCharacters: 2_048,
} as const;

export const EDA_CAPTURE_PROGRESS_PHASES = [
  "authenticating",
  "preparing-image",
  "opening-tunnel",
  "deploying-broker",
  "waiting-broker",
  "configuring-exporter",
  "waiting-topics",
  "ready",
] as const;

export type EdaCaptureProgressPhase = (typeof EDA_CAPTURE_PROGRESS_PHASES)[number];

export type EdaCaptureProducerKind = "ClusterProducer" | "Producer";

export interface EdaCaptureSourceIdentity {
  readonly apiVersion: "kafka.eda.nokia.com/v1" | "kafka.eda.nokia.com/v1alpha1";
  readonly kind: EdaCaptureProducerKind;
  readonly name: string;
  readonly namespace: string;
}

export interface EdaCaptureSource extends EdaCaptureSourceIdentity {
  readonly topics: readonly string[];
  readonly brokers?: readonly string[];
}

export interface EdaApiCredentialsInput {
  readonly baseUrl: string;
  readonly password: string;
  readonly username: string;
  readonly verifyTls?: boolean;
}

// App releases use the full target EDA version, without independent app patches.
export const EDA_TARGET_VERSION = "v26.8.2" as const;

export const EDA_CAPTURE_APPLICATION = {
  appId: "capture.streamskope.io",
  publisher: "StreamSkope",
  version: EDA_TARGET_VERSION,
} as const;

export interface EdaAdministratorCredentialsInput {
  readonly password: string;
  readonly username: string;
}

export interface EdaCaptureApplicationInput {
  readonly authorization?: EdaAdministratorCredentialsInput;
  readonly edaApi: EdaApiCredentialsInput;
}

export interface EdaCaptureApplicationStatus {
  readonly appId: typeof EDA_CAPTURE_APPLICATION.appId;
  readonly publisher: typeof EDA_CAPTURE_APPLICATION.publisher;
  readonly state: "installed" | "missing";
  readonly version: typeof EDA_CAPTURE_APPLICATION.version;
}

export interface EdaCaptureInspectInput {
  readonly edaApi: EdaApiCredentialsInput;
}

export interface EdaCaptureInspection {
  readonly context?: string;
  readonly contexts: readonly string[];
  readonly edaApiUrl?: string;
  readonly imageSetup:
    | { readonly state: "unconfigured" }
    | {
        readonly image: string;
        readonly imageDelivery: "cluster" | "embedded";
        readonly registryHost?: string;
        readonly registryPort?: number;
        readonly state: "configured";
      };
  readonly namespace: string;
  readonly sources: readonly EdaCaptureSource[];
}

interface EdaCaptureDeployInputBase {
  readonly context: string;
  readonly edaApi: EdaApiCredentialsInput;
  readonly localPort: number;
  readonly sessionId?: string;
  readonly source: EdaCaptureSourceIdentity;
}

export interface EdaCaptureDirectDeployInput extends EdaCaptureDeployInputBase {
  readonly image: string;
  readonly imageDelivery: "cluster";
  readonly imagePullSecret?: string;
  readonly registry?: never;
}

export interface EdaCaptureEmbeddedRegistryInput {
  readonly advertisedHost: string;
  readonly certificatePem: string;
  readonly port: number;
  readonly privateKeyPem: string;
}

export interface EdaCaptureEmbeddedDeployInput extends EdaCaptureDeployInputBase {
  readonly image: string;
  readonly imageDelivery: "embedded";
  readonly imagePullSecret?: never;
  readonly registry: EdaCaptureEmbeddedRegistryInput;
}

export interface EdaCaptureConfiguredDeployInput extends EdaCaptureDeployInputBase {
  readonly image?: never;
  readonly imageDelivery: "configured";
  readonly imagePullSecret?: never;
  readonly registry?: never;
}

export type EdaCaptureDeployInput =
  EdaCaptureConfiguredDeployInput | EdaCaptureDirectDeployInput | EdaCaptureEmbeddedDeployInput;

export interface EdaCaptureDeployment {
  readonly sessionId?: string;
  readonly broker: string;
  readonly clusterBroker: string;
  readonly context: string;
  readonly exporterName: string;
  readonly namespace: string;
  readonly profileName: string;
  readonly topics: readonly string[];
  readonly workloadName: string;
}

export interface EdaCaptureProgress {
  readonly detail: string;
  readonly phase: EdaCaptureProgressPhase;
  readonly requestId: string;
}

export interface EdaCaptureCommandResults {
  readonly "edaCapture.application.status": {
    readonly correlationId: string;
    readonly application: EdaCaptureApplicationStatus;
  };
  readonly "edaCapture.application.install": EdaCaptureCommandResults["edaCapture.application.status"];
  readonly "edaCapture.preflight": {
    readonly correlationId: string;
    readonly captureHost: import("./eda-capture-lifecycle").EdaCaptureHostStatus;
  };
  readonly "edaCapture.status": {
    readonly correlationId: string;
    readonly captureSession: import("./eda-capture-lifecycle").EdaCaptureSessionStatus;
  };
  readonly "edaCapture.stop": EdaCaptureCommandResults["edaCapture.status"];
  readonly "edaCapture.remove": EdaCaptureCommandResults["edaCapture.status"];
  readonly "edaCapture.inspect": {
    readonly correlationId: string;
    readonly inspection: EdaCaptureInspection;
  };
  readonly "edaCapture.deploy": {
    readonly correlationId: string;
    readonly deployment: EdaCaptureDeployment;
  };
}
