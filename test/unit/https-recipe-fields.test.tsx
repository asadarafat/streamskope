// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { useState } from "react";

import { trustRecipeInput } from "../support/trust-recipe";
import { TrustRecipeFields } from "../../src/kafka/ui/TrustRecipeFields";
import type { TrustAcquisitionRecipeInput } from "../../src/kafka/contracts";

afterEach(cleanup);
it("explains stdout and warns about legacy output-file redirection without rewriting commands", () => {
  const command = "cat /certificates/truststore.jks > {truststorePath}";
  const value = {
    ...trustRecipeInput(),
    kind: "jks" as const,
    parameters: [],
    ssh: { source: "stdout" as const, value: command, password: { source: "ask" as const } },
  };
  const onChange = vi.fn();
  render(<TrustRecipeFields value={value} onChange={onChange} />);
  expect(screen.getByLabelText("Material command")).toHaveAccessibleDescription(
    expect.stringContaining("raw certificate or truststore bytes"),
  );
  expect(screen.getByRole("alert")).toHaveTextContent("{truststorePath}");
  expect(screen.getByRole("alert")).toHaveTextContent("redirection");
  expect(screen.getByLabelText("Material command")).toHaveValue(command);
  expect(onChange).not.toHaveBeenCalled();
});
it("keeps the active password settings compatible after changing format on another method", () => {
  function Editor(): React.JSX.Element {
    const [value, setValue] = useState<TrustAcquisitionRecipeInput>({
      ...trustRecipeInput(),
      https: {
        authentication: "none",
        material: {
          url: "https://localhost/ca",
          headers: [],
          query: [],
          extraction: { mode: "json-pem", pointer: "/ca" },
        },
        password: { source: "none" },
      },
    });
    return <TrustRecipeFields value={value} onChange={setValue} />;
  }
  render(<Editor />);
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Trust format" }));
  fireEvent.click(screen.getByRole("option", { name: "PKCS12" }));
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Retrieval method" }));
  fireEvent.click(screen.getByRole("option", { name: "HTTPS API" }));
  expect(screen.getByRole("combobox", { name: "Truststore password source" })).toHaveTextContent(
    "Ask during acquisition",
  );
  expect(screen.getByRole("combobox", { name: "Material response" })).toHaveTextContent(
    "Base64 string in JSON",
  );
});
it("switches retrieval methods without losing inactive safe settings", () => {
  function Editor(): React.JSX.Element {
    const [value, setValue] = useState<TrustAcquisitionRecipeInput>(trustRecipeInput());
    return <TrustRecipeFields value={value} onChange={setValue} />;
  }
  render(<Editor />);
  const original = trustRecipeInput().ssh.value;
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Retrieval method" }));
  fireEvent.click(screen.getByRole("option", { name: "HTTPS API" }));
  fireEvent.change(screen.getByLabelText("Material URL"), {
    target: { value: "https://localhost/certificates" },
  });
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Retrieval method" }));
  fireEvent.click(screen.getByRole("option", { name: "Remote SSH" }));
  expect(screen.getByLabelText("Remote file")).toHaveValue(original);
  fireEvent.mouseDown(screen.getByRole("combobox", { name: "Retrieval method" }));
  fireEvent.click(screen.getByRole("option", { name: "HTTPS API" }));
  expect(screen.getByLabelText("Material URL")).toHaveValue("https://localhost/certificates");
});
it("edits an imported HTTPS recipe through the existing common fields", () => {
  const value: TrustAcquisitionRecipeInput = {
    name: "Certificate API",
    method: "https",
    syntax: "named-v1",
    kind: "pem",
    timeoutSeconds: 30,
    parameters: [],
    https: {
      authentication: "none",
      material: {
        url: "https://localhost/ca",
        headers: [],
        query: [],
        extraction: { mode: "raw" },
      },
      password: { source: "none" },
    },
  };
  const onChange = vi.fn();
  render(<TrustRecipeFields value={value} onChange={onChange} />);
  expect(screen.getByLabelText("Retrieval profile name")).toHaveValue("Certificate API");
  fireEvent.change(screen.getByLabelText("Material URL"), {
    target: { value: "https://localhost/certificate" },
  });
  expect(onChange).toHaveBeenLastCalledWith({
    ...value,
    https: {
      ...value.https,
      material: { ...value.https.material, url: "https://localhost/certificate" },
    },
  });
  expect(screen.queryByLabelText("Remote file")).not.toBeInTheDocument();
  expect(screen.queryByLabelText("Truststore password source")).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Add material header" }));
  expect(onChange).toHaveBeenLastCalledWith({
    ...value,
    https: {
      ...value.https,
      material: { ...value.https.material, headers: [{ name: "", value: "" }] },
    },
  });
});

it("focuses an invalid HTTPS endpoint in the shared editor", () => {
  const value: TrustAcquisitionRecipeInput = {
    ...trustRecipeInput(),
    method: "https",
    https: {
      authentication: "none",
      material: {
        url: "http://insecure.invalid/ca",
        headers: [],
        query: [],
        extraction: { mode: "raw" },
      },
      password: { source: "none" },
    },
  };
  render(
    <TrustRecipeFields
      value={value}
      onChange={vi.fn()}
      issue={{ path: "recipe.https.material.url", message: "Use a verified HTTPS endpoint." }}
    />,
  );
  expect(screen.getByLabelText("Material URL")).toHaveFocus();
  expect(screen.getByText("Use a verified HTTPS endpoint.")).toBeVisible();
});
