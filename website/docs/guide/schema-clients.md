---
title: Generate a schema client
description: Generate a bounded, provenance-pinned JavaScript client from a supported Registry JSON Schema.
---

# Generate a schema client

Connect a [Schema Registry](schema-registry.md), select a subject and a specific version, then choose **Generate JavaScript client**. Generation reads the schema and its references; it does not register a schema, create a topic or publish a record.

The initial target is **JavaScript CommonJS on Node.js 24** using **Ajv 8.20.0 standalone generation**. Supported inputs are self-contained JSON Schema draft-07 definitions using primitive/object/array types, properties, required fields, additional properties, item/string/numeric bounds, `enum` and `const`. References, regular expressions, custom keywords, composition keywords, Avro and Protobuf client generation are rejected. Existing Avro/Protobuf record decoding and sample generation remain available separately.

## Save and run

1. Review the selected subject, version, Registry ID and schema SHA-256 shown beside the output.
2. Select **Copy client source**, then save it as `client.cjs` in your own project.
3. Install the pinned validator runtime there:

    ```sh
    npm install --save-exact ajv@8.20.0
    node --check client.cjs
    ```

4. Use the generated encoder and decoder:

    ```js
    const client = require("./client.cjs");
    const record = { id: 42, label: "example" }; // Must match your selected schema.
    const bytes = client.encode(record);
    const roundTrip = client.decode(bytes);
    console.log(roundTrip, client.provenance);
    ```

`encode` validates and adds the Registry magic-byte/schema-ID prefix to UTF-8 JSON. `decode` requires that exact ID and validates the decoded value. Records are limited to 64 KiB before framing. Registry IDs are local to a Registry: register or select the corresponding schema and regenerate for another environment. The generated file never contains Registry authentication or Kafka credentials.

The optional `await client.send(producer, topic, record, key)` helper accepts a configured `@platformatic/kafka` producer, requires broker acknowledgement, disables topic auto-creation and does not retry stale metadata automatically. Configure that producer with `retries: 0` if a single attempt is required. A lost acknowledgement can still mean the record was written; do not blindly retry. The helper does not manage credentials, create topics or close your producer. Use the [sandbox](developer-sandbox.md) for a small consumer/transform exercise.

## Provenance and limits

The file embeds the subject, exact version, Registry ID, source-schema SHA-256, generator/version and licensing notice. The wrapper is Apache-2.0; Ajv and its generated validator are MIT. Preserve those notices with redistributed code and retain Ajv's license with the runtime dependency.

Generation uses the existing bounded codec worker with time and memory limits. Schema input is limited to 64 KiB, depth 16 and supported keywords; output is bounded to 256 KiB. Unsupported or excessive input fails without evaluating user-provided JavaScript. This is a validating record codec and producer helper, not generated domain classes, a consumer framework or a compatibility guarantee for future schema versions.
