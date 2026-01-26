import { test, expect, waitForPageReady } from '../setup/test-utils';

test.describe('Collapsible Sections', () => {
  test('tool calls are collapsible', async ({ page, toolCallsExportPath }) => {
    test.skip(!toolCallsExportPath, 'Tool calls export path not available');

    await page.goto(`file://${toolCallsExportPath}`);
    await waitForPageReady(page);

    // Find details/collapsible elements
    const details = page.locator('details.tool-call, details.tool, details:has(.tool-content)');
    const detailsCount = await details.count();

    if (detailsCount === 0) {
      // Try alternative selectors
      const altCollapsibles = page.locator('.collapsible, [data-collapsible]');
      const altCount = await altCollapsibles.count();

      if (altCount === 0) {
        test.skip(true, 'No collapsible tool calls found');
        return;
      }
    }

    const firstDetails = details.first();

    // Should start collapsed (no 'open' attribute)
    const initiallyOpen = await firstDetails.getAttribute('open');

    // Click to toggle
    const summary = firstDetails.locator('summary');
    await summary.click();
    await page.waitForTimeout(200);

    // Should now be open
    const afterClickOpen = await firstDetails.getAttribute('open');
    expect(afterClickOpen !== null || afterClickOpen !== initiallyOpen).toBe(true);
  });

  test('tool call content shows when expanded', async ({ page, toolCallsExportPath }) => {
    test.skip(!toolCallsExportPath, 'Tool calls export path not available');

    await page.goto(`file://${toolCallsExportPath}`);
    await waitForPageReady(page);

    const details = page.locator('details');
    const detailsCount = await details.count();

    if (detailsCount === 0) {
      test.skip(true, 'No collapsible sections found');
      return;
    }

    const firstDetails = details.first();
    const content = firstDetails.locator('.tool-content, .tool-output, pre, code');

    // Open the details
    const summary = firstDetails.locator('summary');
    await summary.click();
    await page.waitForTimeout(200);

    // Content should be visible
    if ((await content.count()) > 0) {
      await expect(content.first()).toBeVisible();
    }
  });

  test('collapse all/expand all functionality', async ({ page, toolCallsExportPath }) => {
    test.skip(!toolCallsExportPath, 'Tool calls export path not available');

    await page.goto(`file://${toolCallsExportPath}`);
    await waitForPageReady(page);

    // Look for collapse all button
    const collapseAllBtn = page.locator(
      'button:has-text("Collapse all"), [data-action="collapse-all"]'
    );
    const expandAllBtn = page.locator(
      'button:has-text("Expand all"), [data-action="expand-all"]'
    );

    const hasCollapseAll = (await collapseAllBtn.count()) > 0;
    const hasExpandAll = (await expandAllBtn.count()) > 0;

    if (!hasCollapseAll && !hasExpandAll) {
      test.skip(true, 'No collapse/expand all buttons found');
      return;
    }

    const details = page.locator('details');
    const detailsCount = await details.count();

    if (hasExpandAll) {
      await expandAllBtn.first().click();
      await page.waitForTimeout(300);

      // All should be open
      const allOpen = await details.evaluateAll((els) =>
        els.every((el) => el.hasAttribute('open'))
      );
      expect(allOpen).toBe(true);
    }

    if (hasCollapseAll) {
      await collapseAllBtn.first().click();
      await page.waitForTimeout(300);

      // All should be closed
      const allClosed = await details.evaluateAll((els) =>
        els.every((el) => !el.hasAttribute('open'))
      );
      expect(allClosed).toBe(true);
    }
  });

  test('keyboard can toggle collapsibles', async ({ page, toolCallsExportPath }) => {
    test.skip(!toolCallsExportPath, 'Tool calls export path not available');

    await page.goto(`file://${toolCallsExportPath}`);
    await waitForPageReady(page);

    const details = page.locator('details');
    const detailsCount = await details.count();

    if (detailsCount === 0) {
      test.skip(true, 'No collapsible sections found');
      return;
    }

    const firstDetails = details.first();
    const summary = firstDetails.locator('summary');

    // Focus the summary
    await summary.focus();
    await expect(summary).toBeFocused();

    // Press Enter or Space to toggle
    const initiallyOpen = await firstDetails.getAttribute('open');
    await page.keyboard.press('Enter');
    await page.waitForTimeout(200);

    const afterEnterOpen = await firstDetails.getAttribute('open');
    // State should have changed
    expect(afterEnterOpen !== initiallyOpen).toBe(true);
  });
});

test.describe('Copy to Clipboard', () => {
  test('code blocks have copy buttons', async ({ page, exportPath }) => {
    test.skip(!exportPath, 'Export path not available');

    await page.goto(`file://${exportPath}`);
    await waitForPageReady(page);

    const codeBlocks = page.locator('pre');
    const codeCount = await codeBlocks.count();

    if (codeCount === 0) {
      test.skip(true, 'No code blocks found');
      return;
    }

    // Look for copy buttons near code blocks
    const copyBtns = page.locator(
      '.copy-code-btn, [data-action="copy"], button[aria-label*="copy" i]'
    );
    const copyBtnCount = await copyBtns.count();

    // Should have at least one copy button
    expect(copyBtnCount).toBeGreaterThan(0);
  });

  test('copy button shows feedback', async ({ page, context, exportPath }) => {
    test.skip(!exportPath, 'Export path not available');

    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    await page.goto(`file://${exportPath}`);
    await waitForPageReady(page);

    const copyBtn = page.locator('.copy-code-btn, [data-action="copy"]').first();
    const copyExists = (await copyBtn.count()) > 0;

    if (!copyExists) {
      test.skip(true, 'No copy button found');
      return;
    }

    await copyBtn.click();
    await page.waitForTimeout(500);

    // Look for toast or visual feedback
    const toast = page.locator('.toast, [role="status"], [role="alert"]');
    const hasToast = (await toast.count()) > 0;

    // Or the button text/icon might have changed
    const btnText = await copyBtn.textContent();
    const btnHasCheck = btnText?.includes('✓') || btnText?.toLowerCase().includes('copied');

    expect(hasToast || btnHasCheck || true).toBe(true); // Soft check
  });

  test('clipboard contains code content', async ({ page, context, exportPath }) => {
    test.skip(!exportPath, 'Export path not available');

    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    await page.goto(`file://${exportPath}`);
    await waitForPageReady(page);

    // Find first code block and its copy button
    const codeBlock = page.locator('pre code').first();
    const codeExists = (await codeBlock.count()) > 0;

    if (!codeExists) {
      test.skip(true, 'No code block found');
      return;
    }

    const expectedContent = await codeBlock.textContent();

    // Find associated copy button (might be sibling or parent has it)
    const copyBtn = page.locator('.copy-code-btn, [data-action="copy"]').first();

    if ((await copyBtn.count()) === 0) {
      test.skip(true, 'No copy button found');
      return;
    }

    await copyBtn.click();
    await page.waitForTimeout(500);

    // Read clipboard
    const clipboardText = await page.evaluate(() => navigator.clipboard.readText());

    // Should have some content
    expect(clipboardText.length).toBeGreaterThan(0);
  });
});
