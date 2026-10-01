import type {
  EdaCaptureDeployInput,
  EdaCaptureDeployment,
  EdaCaptureInspectInput,
  EdaCaptureInspection,
  EdaCaptureProgressPhase,
  EdaCaptureHostStatus,
  EdaCaptureSessionStatus,
  EdaCaptureApplicationInput,
  EdaCaptureApplicationStatus,
  ProfileEdaCaptureSource,
} from "../contracts";

export type EdaCaptureProgressObserver = (phase: EdaCaptureProgressPhase, detail: string) => void;

export interface EdaCapturePort {
  applicationStatus?(input: EdaCaptureApplicationInput): Promise<EdaCaptureApplicationStatus>;
  installApplication?(input: EdaCaptureApplicationInput): Promise<EdaCaptureApplicationStatus>;
  preflight(): Promise<EdaCaptureHostStatus>;
  status(): EdaCaptureSessionStatus;
  stop(source: ProfileEdaCaptureSource, remove?: boolean): Promise<void>;
  close(): Promise<void>;
  deploy(
    input: EdaCaptureDeployInput,
    onProgress?: EdaCaptureProgressObserver,
    signal?: AbortSignal,
  ): Promise<EdaCaptureDeployment>;
  inspect(input: EdaCaptureInspectInput): Promise<EdaCaptureInspection>;
}
