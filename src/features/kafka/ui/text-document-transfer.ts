import type { HostTextDocument } from "../contracts";
import type { StreamSkopeDesktop } from "../../../platform/desktop";

export interface TextDocumentTransferPort {
  copy(content: string): Promise<void>;
  download(document: HostTextDocument): Promise<TextDocumentTransferOutcome | void>;
}

export type TextDocumentTransferOutcome = "cancelled" | "saved" | "started";

export const browserTextDocumentTransfer: TextDocumentTransferPort = {
  async copy(content) {
    if (globalThis.navigator.clipboard === undefined) {
      throw new Error("Clipboard access is unavailable.");
    }
    await globalThis.navigator.clipboard.writeText(content);
  },
  download(document) {
    return Promise.resolve().then(() => {
      const objectUrl = globalThis.URL.createObjectURL(
        new Blob([document.content], { type: document.mediaType }),
      );
      let anchor: HTMLAnchorElement | undefined;
      try {
        anchor = globalThis.document.createElement("a");
        anchor.download = document.fileName;
        anchor.href = objectUrl;
        anchor.hidden = true;
        globalThis.document.body.append(anchor);
        anchor.click();
      } finally {
        anchor?.remove();
        globalThis.URL.revokeObjectURL(objectUrl);
      }
      return "started" as const;
    });
  },
};

export function createTextDocumentTransfer(
  desktop: StreamSkopeDesktop | undefined,
): TextDocumentTransferPort {
  return desktop === undefined
    ? browserTextDocumentTransfer
    : {
        copy: (content) => browserTextDocumentTransfer.copy(content),
        async download(document): Promise<TextDocumentTransferOutcome> {
          const result = await desktop.saveTextDocument(document);
          return result.state;
        },
      };
}
