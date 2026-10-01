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
    rolldownOptions: {
      output: {
        codeSplitting: {
          groups: [
            {
              includeDependenciesRecursively: false,
              name: "react-dom",
              priority: 2,
              test: /node_modules[\\/]react-dom/u,
            },
            {
              includeDependenciesRecursively: false,
              name: "mui-data-grid",
              priority: 1,
              test: /node_modules[\\/]@mui[\\/]x-data-grid/u,
            },
          ],
        },
      },
    },
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
