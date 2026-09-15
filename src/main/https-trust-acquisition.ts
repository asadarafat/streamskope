import { HTTPS_TRUST_LIMITS as limits } from "../kafka/contracts/https-trust-types";
import { parseTrustRecipeHttps, resolveHttpsGet } from "../kafka/contracts/https-trust-validation";
import {
  HttpsTrustTransportError,
  type HttpsTrustAcquisitionPort,
  type HttpsTrustAcquisitionRequest,
} from "../kafka/application/https-trust-port";

import { extractHttpsTrustMaterial, extractHttpsTrustPassword } from "./https-trust-extraction";
import { NodeHttpsTrustTransport } from "./https-trust-transport";

/** Transport stages only: the application retains ownership of candidates and deadlines. */
export class NodeHttpsTrustAcquisition implements HttpsTrustAcquisitionPort {
  private readonly transport = new NodeHttpsTrustTransport();

  async fetch(
    input: HttpsTrustAcquisitionRequest,
  ): Promise<{ bytes: Uint8Array; password?: string }> {
    const definition = parseTrustRecipeHttps(input.definition, input.parameters, "recipe.https");
    const material = resolveHttpsGet(definition.material, input.parameters, input.values);
    const origin = new URL(material.url).origin;
    if (definition.authentication !== input.authentication.mode)
      throw new HttpsTrustTransportError("configuration", origin);
    const passwordRequest =
      definition.password.source === "https"
        ? resolveHttpsGet(definition.password.request, input.parameters, input.values)
        : undefined;
    if (passwordRequest !== undefined && new URL(passwordRequest.url).origin !== origin)
      throw new HttpsTrustTransportError("configuration", origin);
    let password: string | undefined;
    if (definition.password.source === "ask") {
      if (
        input.password === undefined ||
        input.password.length === 0 ||
        input.password.length > limits.passwordCharacters ||
        input.password.includes("\u0000")
      )
        throw new HttpsTrustTransportError("configuration", origin);
      password = input.password;
    } else if (input.password !== undefined)
      throw new HttpsTrustTransportError("configuration", origin);
    const common = { authentication: input.authentication, tls: input.tls, signal: input.signal };
    if (definition.password.source === "https" && passwordRequest !== undefined) {
      let response: Uint8Array | undefined;
      try {
        response = await this.transport.get({
          ...common,
          ...passwordRequest,
          maximumBytes: limits.passwordWireBytes,
        });
        password = extractHttpsTrustPassword(response, definition.password.request.extraction);
      } catch (error) {
        throw new HttpsTrustTransportError(
          error instanceof HttpsTrustTransportError ? error.category : "extraction",
          origin,
          "password",
        );
      } finally {
        response?.fill(0);
      }
    }
    const response = await this.transport.get({
      ...common,
      ...material,
      maximumBytes:
        definition.material.extraction.mode === "raw" ? limits.materialBytes : limits.jsonWireBytes,
    });
    try {
      const bytes = extractHttpsTrustMaterial(response, definition.material.extraction);
      return { bytes, ...(password === undefined ? {} : { password }) };
    } catch {
      throw new HttpsTrustTransportError("extraction", origin);
    } finally {
      if (definition.material.extraction.mode !== "raw") response.fill(0);
    }
  }
}
