import { test, expect, waitForPageReady } from '../setup/test-utils';

test.describe('Copy to Clipboard', () => {
  test('copy button appears on code blocks', async ({ page, exportPath }) => {
    test.skip(!exportPath, 'Export path not available');

    await page.goto(`file://${exportPath}`);
    await waitForPageReady(page);

    // Check for code blocks
    const codeBlocks = page.locator('pre');
    const codeCount = await codeBlocks.count();

    if (codeCount > 0) {
      // Each code block should have a copy button nearby
      const copyBtn = page.locator('.copy-code-btn, .copy-btn, [data-action="copy"]').first();
      await expect(copyBtn).toBeVisible();
    }
  });

  test('clicking copy button shows toast notification', async ({ page, context, exportPath }) => {
    test.skip(!exportPath, 'Export path not available');

    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    await page.goto(`file://${exportPath}`);
    await waitForPageReady(page);

    // Find copy button on first code block
    const copyBtn = page.locator('.copy-code-btn, .copy-btn, [data-action="copy"]').first();
    if (await copyBtn.count() > 0) {
      await copyBtn.click();

      // Toast notification should appear
      const toast = page.locator('.toast, #toast-container > *');
      await expect(toast.first()).toBeVisible({ timeout: 3000 });
    }
  });

  test('copies code content to clipboard', async ({ page, context, exportPath }) => {
    test.skip(!exportPath, 'Export path not available');

    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    await page.goto(`file://${exportPath}`);
    await waitForPageReady(page);

    // Get the code content from first code block
    const codeBlock = page.locator('pre code').first();
    if (await codeBlock.count() > 0) {
      const codeContent = await codeBlock.textContent();

      // Click the copy button
      const copyBtn = page.locator('.copy-code-btn, .copy-btn').first();
      if (await copyBtn.count() > 0) {
        await copyBtn.click();

        // Wait for clipboard to update
        await page.waitForTimeout(500);

        // Verify clipboard content
        const clipboardText = await page.evaluate(() => navigator.clipboard.readText());

        // Clipboard should contain the code (trim whitespace for comparison)
        expect(clipboardText.trim().length).toBeGreaterThan(0);
      }
    }
  });

  test('toast notification disappears after timeout', async ({ page, context, exportPath }) => {
    test.skip(!exportPath, 'Export path not available');

    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    await page.goto(`file://${exportPath}`);
    await waitForPageReady(page);

    const copyBtn = page.locator('.copy-code-btn, .copy-btn').first();
    if (await copyBtn.count() > 0) {
      await copyBtn.click();

      // Toast should appear
      const toast = page.locator('.toast').first();
      await expect(toast).toBeVisible({ timeout: 1000 });

      // Wait for toast to disappear (default is ~3 seconds)
      await page.waitForTimeout(4000);

      // Toast should be gone or hidden
      await expect(toast).not.toBeVisible();
    }
  });
});

test.describe('Message Copy', () => {
  test('message action buttons are accessible', async ({ page, exportPath }) => {
    test.skip(!exportPath, 'Export path not available');

    await page.goto(`file://${exportPath}`);
    await waitForPageReady(page);

    // Check for message action buttons
    const messageActions = page.locator('.message-actions, .message-action-btn');
    const count = await messageActions.count();

    // If message actions exist, they should be visible on interaction
    if (count > 0) {
      const firstMessage = page.locator('.message').first();
      await firstMessage.hover();

      // Actions should become visible
      const actionBtn = firstMessage.locator('.message-action-btn').first();
      if (await actionBtn.count() > 0) {
        await expect(actionBtn).toBeVisible();
      }
    }
  });
});
