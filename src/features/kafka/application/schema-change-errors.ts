/** Only fixed application-owned guidance may cross the schema-review boundary. */
export class SchemaChangeReviewError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SchemaChangeReviewError";
  }
}
