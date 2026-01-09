import { reactRouter } from "@react-router/dev/vite";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import tsconfigPaths from "vite-tsconfig-paths";
import devtoolsJson from "vite-plugin-devtools-json";
import { nodePolyfills } from "vite-plugin-node-polyfills";

export default defineConfig({
  plugins: [
    tailwindcss(),
    reactRouter(),
    tsconfigPaths(),
    devtoolsJson(),
    nodePolyfills({
      include: ["process"],
      globals: { global: true, process: true },
    }),
  ],
  ssr: {
    external: ["bun"],
  },
  build: {
    rollupOptions: {
      external: ["bun"],
    },
  },
  optimizeDeps: {
    exclude: ["bun"],
  },
});
