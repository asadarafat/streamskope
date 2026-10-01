import type { EdaCaptureSourceIdentity } from "./eda-capture-types";

export interface ProfileEdaCaptureSource {
  readonly edaApiUrl?: string;
  readonly context?: string;
  readonly sessionId?: string;
  readonly broker: string;
  readonly clusterBroker: string;
  readonly exporterName: string;
  readonly kind: "eda-capture";
  readonly source: EdaCaptureSourceIdentity;
  readonly state: "ready";
  readonly topics: readonly string[];
  readonly workloadName: string;
}
