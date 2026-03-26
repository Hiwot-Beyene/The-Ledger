import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

const repoRoot = path.resolve(__dirname, "..");

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
      "@repo-data": path.join(repoRoot, "data"),
    },
  },
  server: {
    port: 5174,
    fs: { allow: [repoRoot] },
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8080",
        changeOrigin: true,
      },
    },
  },
  build: { chunkSizeWarningLimit: 1200 },
});
