import {
  parseArtifactReference,
  type ArtifactReference,
  type StreamSkopeDesktop,
} from "../../../platform/desktop";

export type ArtifactTransferOutcome = "cancelled" | "saved" | "started";
export interface ArtifactTransferPort {
  download(reference: ArtifactReference): Promise<ArtifactTransferOutcome>;
}

/** Transfer an opaque host-owned artifact without buffering its contents in the renderer. */
export function createArtifactTransfer(desktop?: StreamSkopeDesktop): ArtifactTransferPort {
  return {
    async download(value): Promise<ArtifactTransferOutcome> {
      const reference = parseArtifactReference(value);
      if (desktop !== undefined) return (await desktop.saveArtifact(reference)).state;
      const url = `/__streamskope_host/exports/${encodeURIComponent(reference.artifactId)}/${reference.part}`;
      const response = await fetch(url, {
        method: "HEAD",
        credentials: "same-origin",
        mode: "same-origin",
        redirect: "error",
        cache: "no-store",
      });
      if (!response.ok) {
        throw new Error(
          response.status === 401 || response.status === 403
            ? "Unlock the browser workbench before downloading."
            : "This download is no longer available. Refresh the export status or start a new export.",
        );
      }
      const anchor = document.createElement("a");
      try {
        anchor.href = url;
        anchor.download = "";
        anchor.hidden = true;
        document.body.append(anchor);
        anchor.click();
      } finally {
        anchor.remove();
      }
      return "started";
    },
  };
}
