// The browser smoke test of the built site (#1362 (browser smoke test and
// link check)): tests/smoke.spec.mjs. Chromium only, on dist/ served at
// /paros/ by scripts/serve.mjs. Run `npm run build` first, then
// `npm test` in `nix develop .#site` (the flake gives the browser).
import { defineConfig, devices } from "@playwright/test";

const PORT = 4322;

export default defineConfig({
  testDir: "tests",
  // A test that fails is a finding: no retry (AGENTS.md).
  retries: 0,
  forbidOnly: true,
  reporter: process.env.CI ? [["list"], ["github"]] : "list",
  timeout: 120_000,
  use: {
    baseURL: `http://127.0.0.1:${PORT}/paros/`,
    // A story screen then appears complete, with no animation to wait for.
    reducedMotion: "reduce",
    trace: "off",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: `node scripts/serve.mjs ${PORT}`,
    url: `http://127.0.0.1:${PORT}/paros/`,
    reuseExistingServer: false,
  },
});
