const { test, expect } = require('@playwright/test');

async function expectRun(page, run) {
  await expect(page.locator('#overview-title')).toHaveText(run);
  await expect(page.locator('#log-selector')).toHaveValue(run);
  await expect(page.locator('#overview-dashboard')).toBeVisible();
}

test('independent pages never share their selected run', async ({ browser }) => {
  const contextA = await browser.newContext();
  const contextB = await browser.newContext();
  const pageA = await contextA.newPage();
  const pageB = await contextB.newPage();
  await Promise.all([
    pageA.goto('/?run=run-a'),
    pageB.goto('/?run=run-b'),
  ]);
  await Promise.all([expectRun(pageA, 'run-a'), expectRun(pageB, 'run-b')]);
  await pageA.locator('#log-selector').selectOption('run-b');
  await expectRun(pageA, 'run-b');
  await expectRun(pageB, 'run-b');
  await pageB.locator('#log-selector').selectOption('run-a');
  await expectRun(pageA, 'run-b');
  await expectRun(pageB, 'run-a');
  await contextA.close();
  await contextB.close();
});

test('late run responses are discarded and reload state is stable', async ({ page }) => {
  const browserErrors = [];
  page.on('pageerror', error => browserErrors.push(error.message));
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');

  await page.route('**/api/v1/runs/run-b', async route => {
    await new Promise(resolve => setTimeout(resolve, 500));
    await route.continue().catch(() => {});
  });
  await page.locator('#log-selector').selectOption('run-b');
  await page.waitForTimeout(50);
  await page.locator('#log-selector').selectOption('run-a');
  await expectRun(page, 'run-a');
  await page.waitForTimeout(650);
  await expectRun(page, 'run-a');
  await expect(page.locator('#app-banner')).toBeHidden();

  await page.route('**/api/v1/runs/run-a/refresh', async route => {
    await new Promise(resolve => setTimeout(resolve, 300));
    await route.continue();
  });
  await page.locator('#reload-button').click();
  await expect(page.locator('#reload-button')).toHaveClass(/is-reloading/);
  await expect(page.locator('#reload-button')).not.toHaveClass(/is-reloading/);
  await expectRun(page, 'run-a');
  expect(browserErrors).toEqual([]);
});

test('chart geometry, orange hover, and vertical resizing remain coherent', async ({ page }) => {
  const browserErrors = [];
  const serverErrors = [];
  page.on('pageerror', error => browserErrors.push(error.message));
  page.on('response', response => {
    if (response.url().includes('/api/') && response.status() >= 500) {
      serverErrors.push(`${response.status()} ${response.url()}`);
    }
  });
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  await page.locator('.nav-item[data-view="files"]').click();

  const card = page.locator('[data-module="file-sizes"]');
  const point = card.locator('circle.file-size-point').first();
  await expect(point).toBeVisible();
  const widths = await card.evaluate(element => {
    const top = element.querySelector('.section-row.top').getBoundingClientRect();
    const bottom = element.querySelector('.section-row.bottom').getBoundingClientRect();
    return { top: top.width, bottom: bottom.width };
  });
  expect(Math.abs(widths.top - widths.bottom)).toBeLessThanOrEqual(1);

  await point.hover();
  await expect.poll(() => point.getAttribute('fill')).toBe('#d96f24');

  const initialHeight = await card.evaluate(element => element.getBoundingClientRect().height);
  const handle = card.locator('.module-resize-handle');
  const box = await handle.boundingBox();
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await page.mouse.down();
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2 + 140, { steps: 8 });
  await page.mouse.up();
  await expect(card).toHaveClass(/user-resized/);
  const resizedHeight = await card.evaluate(element => element.getBoundingClientRect().height);
  expect(resizedHeight).toBeGreaterThan(initialHeight + 80);
  expect(browserErrors).toEqual([]);
  expect(serverErrors).toEqual([]);
});

test('source-backed metric renders only validated evidence', async ({ page }) => {
  const browserErrors = [];
  page.on('pageerror', error => browserErrors.push(error.message));
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  await page.locator('.nav-item[data-view="tasks"]').click();

  const card = page.locator('[data-module="task-throughput"]');
  await expect(card).toBeVisible();
  await expect(card.locator('path.task-throughput-metric-line').first()).toBeVisible();
  await expect(card.locator('.section-title')).toHaveAttribute('title', /Source: transactions; coverage 100\.0%/);
  expect(browserErrors).toEqual([]);
});
