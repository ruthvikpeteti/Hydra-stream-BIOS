import { defineConfig } from "@playwright/test";

// Browser automation against the LIVE stack (scripts/up.sh --dr). Tokens are read from infrastructure/.env by e2e/env.ts.
export default defineConfig({
  testDir: "./e2e",
  timeout: 180_000,
  expect: { timeout: 20_000 },
  workers: 1,
  fullyParallel: false,
  reporter: [["list"]],
  use: { baseURL: process.env.SS_DASH_URL ?? "http://localhost:8200", headless: true, trace: "retain-on-failure" },
  projects: [{ name: "chromium", use: { browserName: "chromium" } }],
});
