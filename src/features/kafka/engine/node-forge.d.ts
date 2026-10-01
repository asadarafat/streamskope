declare module "node-forge" {
  interface SafeBag {
    readonly type: string;
    readonly cert?: object | null;
    readonly asn1?: object;
  }
  const forge: {
    readonly asn1: {
      fromDer(bytes: string): object;
      toDer(value: object): { getBytes(): string };
    };
    readonly pkcs12: {
      pkcs12FromAsn1(
        value: object,
        password: string,
      ): {
        readonly safeContents: readonly { readonly safeBags: readonly SafeBag[] }[];
      };
    };
    readonly pki: {
      readonly oids: { readonly certBag: string };
      certificateToPem(certificate: object): string;
    };
    readonly pem: {
      encode(value: { readonly type: string; readonly body: string }): string;
    };
  };
  export default forge;
}
