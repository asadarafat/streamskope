/** One local Connect mutation at a time; original request ownership still governs cleanup. */
export class ConnectWriteAdmission {
  private owner: object | undefined;
  acquire(): (() => void) | null {
    if (this.owner) return null;
    const owner = {};
    this.owner = owner;
    return (): void => {
      if (this.owner === owner) this.owner = undefined;
    };
  }
}
