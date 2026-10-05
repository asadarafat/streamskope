/** Host-only protection supplied by the operating-system credential service. */
export interface ProfileProtector {
  protect(plaintext: string): Promise<Buffer>;
  unprotect(
    protectedValue: Buffer,
  ): Promise<{ readonly plaintext: string; readonly shouldReEncrypt: boolean }>;
}

export interface ProfileProtectionCapability {
  readonly durability: "durable";
  readonly protection: "os-protected" | "unavailable";
  readonly recovery?: string;
  readonly state: "ready" | "unavailable";
}
