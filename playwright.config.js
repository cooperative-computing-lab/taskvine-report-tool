const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests/e2e',
  timeout: 45_000,
  expect: { timeout: 12_000 },
  fullyParallel: false,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [['line'], ['html', { open: 'never' }]] : 'line',
  use: {
    baseURL: 'http://127.0.0.1:9137',
    viewport: { width: 1440, height: 1000 },
    ignoreHTTPSErrors: true,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  webServer: {
    command: 'python tests/e2e_server.py --port 9137',
    url: 'http://127.0.0.1:9137/api/v1/health',
    timeout: 60_000,
    reuseExistingServer: false,
  },
});
