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

test('a group load is single-flight while run activation is still finishing', async ({ page }) => {
  let timelineRequests = 0;
  page.on('request', request => {
    if (request.url().includes('/charts/task-execution-details')) timelineRequests += 1;
  });
  await page.route('**/tasks?*', async route => {
    await new Promise(resolve => setTimeout(resolve, 350));
    await route.continue();
  });
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  await page.locator('.nav-item[data-view="tasks"]').click();
  await page.locator('[data-module="task-execution-details"] rect[role="button"]').first().waitFor({ state: 'attached' });
  await page.waitForTimeout(500);
  expect(timelineRequests).toBe(1);
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

test('partial metrics show the actionable status reason, not the semantic caveat', async ({ page }) => {
  await page.goto('/?run=run-b');
  await expectRun(page, 'run-b');
  await page.locator('.nav-item[data-view="tasks"]').click();

  const card = page.locator('[data-module="failure-timeline"]');
  await expect(card).toContainText('PARTIAL: Manager END is missing');
  await expect(card).not.toContainText('PARTIAL: Statuses are TaskVine terminal outcomes');
  await expect(card.locator('.section-title')).toHaveAttribute('title', /Statuses are TaskVine terminal outcomes/);
});

test('operational charts expose pressure, retries, occupancy, and worker loss reasons', async ({ page }) => {
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  await page.locator('.nav-item[data-view="tasks"]').click();
  await expect(page.locator('[data-module="workflow-pressure"] path.workflow-pressure-metric-line').first()).toBeVisible();
  await expect(page.locator('[data-module="retry-pressure"] path.retry-pressure-metric-line').first()).toBeVisible();

  await page.locator('.nav-item[data-view="workers"]').click();
  const occupancy = page.locator('[data-module="core-occupancy"]');
  await expect(occupancy.locator('path.core-occupancy-metric-line')).toBeVisible();
  await expect(occupancy.locator('.section-description')).toContainText('not CPU utilization');
  const sessions = page.locator('[data-module="worker-churn"]');
  await expect(sessions.locator('path.worker-churn-metric-line').first()).toBeVisible();
  await expect(sessions.locator('.legend-container')).toContainText('EXIT');
});

test('live updates are revision-aware and can be paused', async ({ page }) => {
  let refreshes = 0;
  page.on('request', request => {
    if (request.method() === 'POST' && request.url().endsWith('/refresh')) refreshes += 1;
  });
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  const live = page.locator('#auto-refresh-button');
  await expect(live).toHaveAttribute('aria-pressed', 'true');
  await live.click();
  await expect(live).toHaveAttribute('aria-pressed', 'false');

  await page.route('**/api/v1/runs/run-a/watch', route => route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify({
      schema_version: 'report-api/v1', request_id: 'watch-test',
      data: { run_id: 'run-a', current_revision: 'unchanged', source_token: 'changed-source' },
    }),
  }));
  await live.click();
  await expect(live).toHaveAttribute('aria-pressed', 'true');
  await expect.poll(() => refreshes).toBe(1);
  await expect(page.locator('#toast-region')).toContainText('New run data loaded.');
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
  await card.locator('.section-title').click();
  await expect(tooltip).toBeHidden();
  await expect(hitbox).toHaveAttribute('aria-pressed', 'false');

  await hitbox.click({ position: { x: box.width * 0.6, y: box.height * 0.45 } });
  await expect(tooltip).toHaveClass(/cursor-pinned/);
  await hitbox.press('Escape');
  await expect(tooltip).toBeHidden();

  await page.mouse.move(box.x - 10, box.y - 10);
  await expect(tooltip).toBeHidden();
});

test('points and bars pin one shared tooltip and charts expose zoom controls', async ({ page }) => {
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  await page.locator('.nav-item[data-view="files"]').click();

  const scatter = page.locator('[data-module="file-sizes"]');
  const point = scatter.locator('circle.file-sizes-series-0-point').first();
  const tooltip = page.locator('#vine-tooltip');
  await point.click();
  expect(await scatter.locator('circle[role="button"]').evaluateAll(nodes => nodes.every(node => !node.__on))).toBe(true);
  await expect(point).toHaveAttribute('aria-pressed', 'true');
  await expect(tooltip).toHaveClass(/cursor-pinned/);
  await expect(tooltip.locator('.mark-tooltip-status')).toContainText('Pinned');
  const pinnedPointText = await tooltip.textContent();
  await scatter.locator('circle.file-sizes-series-0-point').nth(1).hover();
  await expect(tooltip).toHaveText(pinnedPointText);
  await page.keyboard.press('Escape');
  await point.press('Enter');
  await expect(point).toHaveAttribute('aria-pressed', 'true');
  await expect(tooltip).toHaveClass(/cursor-pinned/);
  await scatter.locator('.section-description').click();
  await expect(point).toHaveAttribute('aria-pressed', 'false');
  await expect(tooltip).toBeHidden();

  const zoom = scatter.locator('.chart-zoom-controls');
  await expect(zoom).toContainText('Ctrl/⌘ + scroll');
  await expect(zoom.getByRole('button', { name: 'Reset zoom' })).toHaveText('100%');
  const zoomPosition = await zoom.boundingBox();
  await zoom.getByRole('button', { name: 'Zoom in' }).click();
  await expect(zoom.getByRole('button', { name: 'Reset zoom' })).toHaveText('120%');
  const zoomedPosition = await zoom.boundingBox();
  expect(Math.abs(zoomedPosition.x - zoomPosition.x)).toBeLessThanOrEqual(1);
  expect(Math.abs(zoomedPosition.y - zoomPosition.y)).toBeLessThanOrEqual(1);
  await zoom.evaluate(node => {
    const zoomIn = node.querySelector('[data-zoom="in"]');
    for (let index = 0; index < 24; index += 1) zoomIn.click();
  });
  await expect(zoom.getByRole('button', { name: 'Reset zoom' })).toHaveText('2000%');
  const maximumZoomPosition = await zoom.boundingBox();
  expect(Math.abs(maximumZoomPosition.x - zoomPosition.x)).toBeLessThanOrEqual(1);
  expect(Math.abs(maximumZoomPosition.y - zoomPosition.y)).toBeLessThanOrEqual(1);
  await zoom.getByRole('button', { name: 'Reset zoom' }).click();
  await expect(zoom.getByRole('button', { name: 'Reset zoom' })).toHaveText('100%');

  await page.locator('.nav-item[data-view="tasks"]').click();
  const timeline = page.locator('[data-module="task-execution-details"]');
  const bar = timeline.locator('rect.task-type-successful-executing-on-worker').first();
  await expect(bar).toBeVisible();
  await bar.click();
  expect(await timeline.locator('rect[role="button"]').evaluateAll(nodes => nodes.every(node => !node.__on))).toBe(true);
  await expect(point).toHaveAttribute('aria-pressed', 'false');
  await expect(bar).toHaveAttribute('aria-pressed', 'true');
  await expect(tooltip).toHaveClass(/cursor-pinned/);
  await expect(tooltip).toContainText('Task ID');
  await page.keyboard.press('Escape');
  await expect(tooltip).toBeHidden();
  await expect(bar).toHaveAttribute('aria-pressed', 'false');
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

test('duration axes use human units and exact cursor values', async ({ page }) => {
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  const result = await page.evaluate(async () => {
    const { durationAxisSpec, formatDuration } = await import('/static/js/modules/time_format.js?v=axis-test');
    const { GenericMetricModule } = await import('/static/js/modules/generic_metric.js?v=axis-test');
    const longAxis = durationAxisSpec([0, 12_000], 6);
    const mediumAxis = durationAxisSpec([0, 1800], 6);
    const shortAxis = durationAxisSpec([0, 90], 6);
    const metric = new GenericMetricModule('axis-test', 'Axis test', '/unused', { renderer: 'line' });
    const transformed = metric.transformFetchedData({
      available: true,
      series: [{ name: 'Tasks', points: [[0, 0], [12_000, 1]] }],
      axes: { x: { domain: [0, 12_000], unit: 'seconds' }, y: { domain: [0, 1], unit: 'count' } },
    });
    const integratedFormatter = eval(transformed.x_tick_formatter);
    const indexMetric = new GenericMetricModule('index-test', 'Index test', '/unused', { renderer: 'scatter' });
    const indexData = indexMetric.transformFetchedData({
      available: true,
      contract: { status: 'PASS' },
      series: [{ name: 'Tasks', points: [[0, 1], [3323, 2]] }],
      axes: { x: { domain: [0, 3323], unit: 'count' }, y: { domain: [0, 2], unit: 'count' } },
    });
    const indexFormatter = eval(indexData.x_tick_formatter);
    return {
      longUnit: longAxis.label,
      longLabels: longAxis.tickValues.map(longAxis.formatter),
      mediumUnit: mediumAxis.label,
      mediumLabels: mediumAxis.tickValues.map(mediumAxis.formatter),
      shortUnit: shortAxis.label,
      integratedLabels: transformed.x_tick_values.map(integratedFormatter),
      exact: formatDuration(8000),
      missing: formatDuration(null),
      index: indexFormatter(3323),
    };
  });
  expect(result.longUnit).toBe('h');
  expect(result.longLabels).toEqual(['0 h', '0.5 h', '1 h', '1.5 h', '2 h', '2.5 h', '3 h']);
  expect(result.longLabels.join(' ')).not.toContain('ks');
  expect(result.mediumUnit).toBe('min');
  expect(result.mediumLabels).toEqual(['0 min', '5 min', '10 min', '15 min', '20 min', '25 min', '30 min']);
  expect(result.shortUnit).toBe('s');
  expect(result.integratedLabels).toEqual(result.longLabels);
  expect(result.exact).toBe('2h 13m 20s');
  expect(result.missing).toBe('—');
  expect(result.index).toBe('3,323');
});

test('file and worker explorers share search, filters, and detail drawers', async ({ page }) => {
  await page.goto('/?run=run-a&view=explorer');
  await expect(page.locator('#overview-title')).toHaveText('run-a');
  await expect(page.locator('#log-selector')).toHaveValue('run-a');
  await expect(page.locator('#view-explorer')).toBeVisible();
  await expect(page.locator('#explorer-title')).toHaveText('Task Explorer');
  await expect(page.locator('#explorer-table tbody tr')).toHaveCount(3);
  await expect(page.getByRole('columnheader', { name: /Execution/ })).toHaveAttribute('aria-sort', 'descending');
  await page.getByRole('button', { name: 'Task', exact: true }).click();
  await expect(page.getByRole('columnheader', { name: /Task/ })).toHaveAttribute('aria-sort', 'ascending');
  await expect(page.locator('#explorer-table tbody tr').first()).toContainText('#1');
  await page.getByRole('button', { name: 'Task', exact: true }).click();
  await expect(page.getByRole('columnheader', { name: /Task/ })).toHaveAttribute('aria-sort', 'descending');
  await expect(page.locator('#explorer-table tbody tr').first()).toContainText('#3');

  await page.getByRole('tab', { name: 'Files' }).click();
  await expect(page.locator('#explorer-title')).toHaveText('File Explorer');
  await expect(page.getByRole('columnheader', { name: /Size/ })).toHaveAttribute('aria-sort', 'descending');
  await page.locator('#explorer-search').fill('output-b');
  await page.locator('#explorer-search-button').click();
  await expect(page.locator('#explorer-table tbody tr')).toHaveCount(1);
  await expect(page.locator('#explorer-table tbody')).toContainText('output-b');
  await page.locator('#explorer-table tbody tr').click();
  await expect(page.locator('#entity-drawer')).toHaveClass(/open/);
  await expect(page.locator('#entity-drawer-title')).toHaveText('output-b');
  await page.locator('#entity-drawer .drawer-close').click();

  await page.getByRole('tab', { name: 'Workers' }).click();
  await page.locator('#explorer-filter').selectOption('issues');
  await expect(page.locator('#explorer-table tbody tr')).toHaveCount(1);
  await expect(page.locator('#explorer-table tbody')).toContainText('worker-b');
  await page.locator('#explorer-table tbody tr').click();
  await expect(page.locator('#entity-drawer-body')).toContainText('Unsuccessful');
  await expect(page.locator('#entity-drawer-body')).toContainText('1');
});

test('Display CDF renders and restores a generic scatter metric', async ({ page }) => {
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');
  await page.locator('.nav-item[data-view="files"]').click();

  const card = page.locator('[data-module="file-sizes"]');
  await expect(card.locator('circle.file-sizes-series-0-point').first()).toBeVisible();
  await expect(card.locator('.toolbox-container')).toBeVisible();
  await expect(card.locator('.toolbox-toggle-button')).toHaveAttribute('aria-expanded', 'false');
  await card.locator('.toolbox-toggle-button').click();
  await expect(card.locator('.toolbox-toggle-button')).toHaveAttribute('aria-expanded', 'true');
  await expect(card.locator('.toolbox-panel-title')).toHaveText('Chart controls');
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

  await page.locator('.nav-item[data-view="tasks"]').click();
  const durationCard = page.locator('[data-module="task-execution-time"]');
  await expect(durationCard.locator('circle').first()).toBeVisible();
  await durationCard.locator('.toolbox-toggle-button').click();
  await durationCard.getByRole('button', { name: 'Display CDF', exact: true }).click();
  await expect(durationCard.locator('path.cdf-path')).toBeVisible();
  const durationLabels = await durationCard.locator('svg text').allTextContents();
  expect(durationLabels.some(label => / (?:ms|s|min|h|d)$/.test(label))).toBe(true);
  expect(durationLabels.join(' ')).not.toContain('ks');
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

test('legends use a spaced responsive grid without horizontal scrolling', async ({ page }) => {
  await page.setViewportSize({ width: 600, height: 900 });
  await page.goto('/?run=run-a');
  await expectRun(page, 'run-a');

  for (const view of ['tasks', 'workers']) {
    await page.locator('#mobile-menu').click();
    await page.locator(`.nav-item[data-view="${view}"]`).click();
    const firstLegend = page.locator(`#modules-${view} .legend-container`).filter({ has: page.locator('.legend-item') }).first();
    await expect(firstLegend).toBeVisible();
    const layout = await page.locator(`#modules-${view} .legend-container`).evaluateAll(containers => containers
      .filter(container => container.querySelector('.legend-item'))
      .map(container => ({
        horizontalOverflow: container.scrollWidth - container.clientWidth,
        verticalOverflowEnabled: getComputedStyle(container.closest('.section-row.top')).overflowY === 'auto',
        layoutsUseGrid: [...container.querySelectorAll('.legend-row, .legend-flex-container')]
          .every(layout => {
            const style = getComputedStyle(layout);
            return style.display === 'grid'
              && parseFloat(style.columnGap) >= 20
              && parseFloat(style.rowGap) >= 6;
          }),
      })));
    expect(layout.every(item => item.horizontalOverflow <= 1
      && item.verticalOverflowEnabled
      && item.layoutsUseGrid)).toBe(true);
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
