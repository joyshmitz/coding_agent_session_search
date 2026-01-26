import { test as base, expect, Page } from '@playwright/test';
import { readFileSync, existsSync } from 'fs';
import path from 'path';

// Load environment variables from .env.test
const envPath = path.resolve(__dirname, '../.env.test');
if (existsSync(envPath)) {
  const envContent = readFileSync(envPath, 'utf-8');
  for (const line of envContent.split('\n')) {
    const [key, ...valueParts] = line.split('=');
    if (key && valueParts.length > 0) {
      process.env[key] = valueParts.join('=');
    }
  }
}

/**
 * Test fixtures for HTML export tests.
 */
export interface TestFixtures {
  exportPath: string;
  encryptedExportPath: string;
  toolCallsExportPath: string;
  largeExportPath: string;
  unicodeExportPath: string;
  noCdnExportPath: string;
  password: string;
}

/**
 * Extended test with HTML export fixtures.
 */
export const test = base.extend<TestFixtures>({
  exportPath: async ({}, use) => {
    const exportPath = process.env.TEST_EXPORT_TEST_BASIC || '';
    await use(exportPath);
  },

  encryptedExportPath: async ({}, use) => {
    const exportPath = process.env.TEST_EXPORT_TEST_ENCRYPTED || '';
    await use(exportPath);
  },

  toolCallsExportPath: async ({}, use) => {
    const exportPath = process.env.TEST_EXPORT_TEST_TOOL_CALLS || '';
    await use(exportPath);
  },

  largeExportPath: async ({}, use) => {
    const exportPath = process.env.TEST_EXPORT_TEST_LARGE || '';
    await use(exportPath);
  },

  unicodeExportPath: async ({}, use) => {
    const exportPath = process.env.TEST_EXPORT_TEST_UNICODE || '';
    await use(exportPath);
  },

  noCdnExportPath: async ({}, use) => {
    const exportPath = process.env.TEST_EXPORT_TEST_NO_CDN || '';
    await use(exportPath);
  },

  password: async ({}, use) => {
    await use(process.env.TEST_EXPORT_PASSWORD || 'test-password-123');
  },
});

export { expect };

/**
 * Utility to collect console errors during test.
 */
export async function collectConsoleErrors(page: Page): Promise<string[]> {
  const errors: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error') {
      errors.push(msg.text());
    }
  });
  return errors;
}

/**
 * Utility to wait for page to be fully loaded including lazy resources.
 */
export async function waitForPageReady(page: Page): Promise<void> {
  await page.waitForLoadState('networkidle');
  // Additional wait for any animations or deferred scripts
  await page.waitForTimeout(500);
}

/**
 * Count messages in the rendered HTML.
 */
export async function countMessages(page: Page): Promise<number> {
  return page.locator('.message').count();
}

/**
 * Get the current theme from the page.
 */
export async function getCurrentTheme(page: Page): Promise<string> {
  return page.locator('html').getAttribute('data-theme') || 'unknown';
}
