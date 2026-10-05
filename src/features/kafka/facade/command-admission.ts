/** Owns admitted commands until their complete facade response has settled. */
export class KafkaCommandAdmission {
  private acceptingExternal = true;
  private readonly operations = new Set<Promise<unknown>>();

  accepts(internal: boolean): boolean {
    return internal || this.acceptingExternal;
  }

  close(): void {
    this.acceptingExternal = false;
  }

  track<Value>(run: () => Promise<Value>): Promise<Value> {
    let resolve!: (value: Value) => void;
    let reject!: (reason: unknown) => void;
    const operation = new Promise<Value>((complete, fail) => {
      resolve = complete;
      reject = fail;
    });
    // Publish ownership before dispatch, which can synchronously notify subscribers.
    this.operations.add(operation);
    operation.then(
      () => this.operations.delete(operation),
      () => this.operations.delete(operation),
    );
    try {
      run().then(resolve, reject);
    } catch (error) {
      reject(error);
    }
    return operation;
  }

  async idle(): Promise<void> {
    while (this.operations.size > 0) await Promise.allSettled([...this.operations]);
  }
}
