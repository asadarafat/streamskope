import { fileURLToPath } from "node:url";

import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const developmentCsp = {
  apply: "serve" as const,
  name: "streamskope-development-csp",
  transformIndexHtml(html: string): string {
    return html.replace(
      "connect-src http://127.0.0.1:*;",
      "connect-src http://127.0.0.1:* ws://127.0.0.1:*;",
    );
  },
};

export default defineConfig({
  root: fileURLToPath(new URL("../", import.meta.url)),
  base: "./",
  build: {
    outDir: "dist/renderer",
    // Let the bundler preserve dependency initialization order across lazy workspaces.
    // Forced partial React/MUI groups can place a styled component on both sides of a chunk cycle.
  },
  optimizeDeps: {
    include: ["@mui/x-data-grid"],
  },
  plugins: [developmentCsp, react()],
  server: {
    host: "127.0.0.1",
    strictPort: true,
  },
});
