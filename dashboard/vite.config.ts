/// <reference types="vitest" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev: the API runs at SS_DASH_API (default http://localhost:8200). Production: the API serves the built files itself, same origin.
export default defineConfig({
  plugins: [react()],
  server: { port: 5173, proxy: { "/api": process.env.SS_DASH_API ?? "http://localhost:8200" } },
  test: { environment: "jsdom", globals: true, setupFiles: ["./src/setupTests.ts"], include: ["src/__tests__/**/*.test.tsx"] },
});
