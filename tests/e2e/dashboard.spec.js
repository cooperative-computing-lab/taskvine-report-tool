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
  const point = card.locator('circle.file-sizes-series-0-point').first();
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

test('line grids inspect every visible series at the cursor x position', async ({ page }) => {
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  await page.locator('.nav-item[data-view="workers"]').click();

  const card = page.locator('[data-module="worker-capacity"]');
  await card.scrollIntoViewIfNeeded();
  const hitbox = card.locator('.line-cursor-hitbox');
  await expect(hitbox).toBeVisible();
  const box = await hitbox.boundingBox();
  await page.mouse.move(box.x + box.width * 0.5, box.y + box.height * 0.45);

  const tooltip = page.locator('#vine-tooltip');
  await expect(tooltip).toHaveClass(/line-cursor-tooltip/);
  await expect(tooltip).toBeVisible();
  await expect(tooltip.locator('.line-cursor-x')).toContainText('Runtime');
  await expect(tooltip.locator('.line-cursor-status')).toContainText('active · Click to pin');
  await expect(tooltip.locator('.line-cursor-row')).toHaveCount(3);
  await expect(card.locator('.line-cursor-marker')).toHaveCount(3);
  await expect(card.locator('.line-cursor-layer')).toBeVisible();
  await expect(card.locator('.line-cursor-guide')).toHaveAttribute('x1', /\d/);

  await card.locator('input#worker-capacity-series-1').uncheck();
  await page.mouse.move(box.x + box.width * 0.6, box.y + box.height * 0.45);
  await expect(tooltip.locator('.line-cursor-row')).toHaveCount(2);

  await page.mouse.click(box.x + box.width * 0.6, box.y + box.height * 0.45);
  await expect(tooltip).toHaveClass(/cursor-pinned/);
  await expect(tooltip.locator('.line-cursor-status')).toContainText('Pinned');
  const pinnedCoordinate = await tooltip.locator('.line-cursor-x').textContent();
  const competingHitbox = page.locator('[data-module="worker-executing-tasks"] .line-cursor-hitbox');
  await expect(competingHitbox).toBeVisible();
  await competingHitbox.hover({ position: { x: 30, y: 30 } });
  await expect(tooltip).toBeVisible();
  await expect(tooltip).toHaveClass(/cursor-pinned/);
  await expect(tooltip.locator('.line-cursor-x')).toHaveText(pinnedCoordinate);
  await expect(hitbox).toHaveAttribute('aria-pressed', 'true');
  await hitbox.press('Escape');
  await expect(tooltip).toBeHidden();

  await page.mouse.move(box.x - 10, box.y - 10);
  await expect(tooltip).toBeHidden();
});

test('line cursor prioritizes active series and stays fast at scale', async ({ page }) => {
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  const result = await page.evaluate(async () => {
    const { LineCursor, sortCursorValues } = await import('/static/js/modules/line_cursor.js?v=scale-test');
    const sorted = sortCursorValues([
      { id: 'zero-a', index: 0, y: 0 },
      { id: 'small', index: 1, y: 2 },
      { id: 'large', index: 2, y: -7 },
      { id: 'zero-b', index: 3, y: 0 },
    ]).map(item => item.id);
    const series = Array.from({ length: 120 }, (_, seriesIndex) => ({
      id: `series-${seriesIndex}`,
      points: Array.from({ length: 2000 }, (_, pointIndex) => [pointIndex, (seriesIndex + pointIndex) % 9]),
    }));
    const cursor = new LineCursor({
      svg: d3.select(document.createElementNS('http://www.w3.org/2000/svg', 'svg')),
      xScale: d3.scaleLinear().domain([0, 1999]).range([0, 1000]),
      yScale: d3.scaleLinear().domain([0, 8]).range([500, 0]),
      series,
      tooltip: document.createElement('div'),
    });
    const start = performance.now();
    let sampled = 0;
    for (let index = 0; index < 500; index += 1) {
      sampled += cursor._valuesAt((index * 37) % 1999).length;
    }
    return { sorted, sampled, durationMs: performance.now() - start };
  });
  expect(result.sorted).toEqual(['large', 'small', 'zero-a', 'zero-b']);
  expect(result.sampled).toBe(60_000);
  expect(result.durationMs).toBeLessThan(1500);
});

test('Display CDF renders and restores a generic scatter metric', async ({ page }) => {
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  await page.locator('.nav-item[data-view="files"]').click();

  const card = page.locator('[data-module="file-sizes"]');
  await expect(card.locator('circle.file-sizes-series-0-point').first()).toBeVisible();
  await card.locator('.toolbox-toggle-button').click();
  await card.getByRole('button', { name: 'Display CDF', exact: true }).click();

  const cdf = card.locator('path.cdf-path');
  await expect(cdf).toBeVisible();
  await expect(cdf).toHaveAttribute('d', /L/);
  await expect(card.locator('circle.cdf-point')).toHaveCount(100);
  await expect(card.locator('.line-cursor-hitbox')).toBeVisible();

  await card.locator('.toolbox-toggle-button').click();
  await card.getByRole('button', { name: 'Undisplay CDF', exact: true }).click();
  await expect(card.locator('circle.file-sizes-series-0-point').first()).toBeVisible();
  await expect(card.locator('path.cdf-path')).toHaveCount(0);
});

test('every visible chart section has a readable explanation', async ({ page }) => {
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  for (const view of ['tasks', 'workers', 'transfers', 'files']) {
    await page.locator(`.nav-item[data-view="${view}"]`).click();
    const cards = page.locator(`#modules-${view} .module-card`);
    await expect(cards.first()).toBeVisible();
    const descriptions = cards.locator('.section-description');
    await expect(descriptions).toHaveCount(await cards.count());
    const checks = await descriptions.evaluateAll(nodes => nodes.map(node => ({
      text: node.textContent.trim(),
      fontSize: Number.parseFloat(getComputedStyle(node).fontSize),
    })));
    expect(checks.every(item => item.text.length >= 45 && item.fontSize >= 15)).toBe(true);
  }
});

test('new frontend blocks an older backend with one actionable message', async ({ page }) => {
  await page.route('**/api/v1/health', async route => {
    const response = await route.fetch();
    const payload = await response.json();
    delete payload.data.capabilities;
    await route.fulfill({ response, json: payload });
  });
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'TaskVine Report server restart required' })).toBeVisible();
  await expect(page.locator('body')).toContainText('newer JavaScript bundle than the running backend');
  await expect(page.locator('.module-state.error')).toHaveCount(0);
});
