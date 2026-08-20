import { moduleClasses, enabledModuleConfigs } from './modules/configs.js?v=5.3';

const REQUIRED_SERVER_CAPABILITY = 'source-backed-metrics-v1';

const moduleObjects = {};
const loadedGroups = new Set();

const state = {
    run: '',
    revision: '',
    generation: 0,
    runController: null,
    view: 'overview',
    runs: [],
    summary: null,
    timeStart: 0,
    timeEnd: 100,
    requestCount: 0,
};

function beginRunScope(run) {
    state.runController?.abort();
    state.runController = new AbortController();
    state.generation += 1;
    state.run = run;
    state.revision = '';
    return {
        run,
        revision: '',
        generation: state.generation,
        signal: state.runController.signal,
    };
}

function currentRunScope() {
    if (!state.run || !state.revision || !state.runController) return null;
    return {
        run: state.run,
        revision: state.revision,
        generation: state.generation,
        signal: state.runController.signal,
    };
}

function isCurrentScope(scope) {
    return Boolean(scope)
        && !scope.signal.aborted
        && scope.run === state.run
        && scope.generation === state.generation
        && (!scope.revision || scope.revision === state.revision);
}

const $ = selector => document.querySelector(selector);
const $$ = selector => Array.from(document.querySelectorAll(selector));

function escapeHTML(value) {
    const element = document.createElement('div');
    element.textContent = value == null ? '' : String(value);
    return element.innerHTML;
}

function formatNumber(value, digits = 0) {
    if (value == null || !Number.isFinite(Number(value))) return '—';
    return new Intl.NumberFormat('en-US', { maximumFractionDigits: digits }).format(Number(value));
}

function formatPercent(value, digits = 1) {
    if (value == null || !Number.isFinite(Number(value))) return '—';
    return `${(Number(value) * 100).toFixed(digits)}%`;
}

function formatDuration(seconds) {
    if (seconds == null || !Number.isFinite(Number(seconds))) return '—';
    seconds = Number(seconds);
    if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
    if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ${Math.round((seconds % 3600) / 60)}m`;
    return `${Math.floor(seconds / 86400)}d ${Math.round((seconds % 86400) / 3600)}h`;
}

function formatBytesFromMB(mb) {
    if (mb == null || !Number.isFinite(Number(mb))) return '—';
    mb = Number(mb);
    if (mb >= 1024 * 1024) return `${(mb / 1024 / 1024).toFixed(1)} TB`;
    if (mb >= 1024) return `${(mb / 1024).toFixed(1)} GB`;
    return `${mb.toFixed(mb < 10 ? 1 : 0)} MB`;
}

function beginRequest() {
    state.requestCount += 1;
    $('#global-progress').classList.add('loading');
}

function endRequest() {
    state.requestCount = Math.max(0, state.requestCount - 1);
    if (!state.requestCount) $('#global-progress').classList.remove('loading');
}

async function getJSON(url, options = {}) {
    const { scope = null, allowRevisionChange = false, ...fetchOptions } = options || {};
    if (scope && !fetchOptions.signal) fetchOptions.signal = scope.signal;
    beginRequest();
    try {
        const response = await fetch(url, fetchOptions);
        const payload = await response.json().catch(() => ({}));
        const apiError = typeof payload.error === 'object' ? payload.error.message : payload.error;
        if (!response.ok || apiError) throw new Error(apiError || `Request failed (${response.status})`);
        if (scope) {
            const staleIdentity = !isCurrentScope(scope)
                || (payload.run_id && payload.run_id !== scope.run)
                || (!allowRevisionChange && scope.revision && payload.revision && payload.revision !== scope.revision);
            if (staleIdentity) throw new DOMException('Stale run response discarded', 'AbortError');
        }
        return payload.schema_version === 'report-api/v1' ? payload.data : payload;
    } finally {
        endRequest();
    }
}

function toast(message) {
    const item = document.createElement('div');
    item.className = 'toast';
    item.textContent = message;
    $('#toast-region').appendChild(item);
    setTimeout(() => item.remove(), 3200);
}

function showBanner(message = '') {
    const banner = $('#app-banner');
    banner.textContent = message;
    banner.classList.toggle('hidden', !message);
}

function showRestartRequired(reason) {
    document.body.innerHTML = `
        <main style="max-width:680px;margin:12vh auto;padding:32px;font:16px/1.55 system-ui;color:#292927">
            <h1 style="margin:0 0 12px;font-size:28px">TaskVine Report server restart required</h1>
            <p>${escapeHTML(reason)}</p>
            <p>Stop the running <code>vine_report</code> process, start it again, then reload this page.</p>
        </main>`;
}

function updateURL(replace = true) {
    const url = new URL(window.location.href);
    if (state.run) url.searchParams.set('run', state.run); else url.searchParams.delete('run');
    url.searchParams.set('view', state.view);
    if (state.timeStart) url.searchParams.set('from', state.timeStart); else url.searchParams.delete('from');
    if (state.timeEnd !== 100) url.searchParams.set('to', state.timeEnd); else url.searchParams.delete('to');
    history[replace ? 'replaceState' : 'pushState']({}, '', url);
}

function attachModuleResizer(card, module) {
    const handle = document.createElement('div');
    handle.className = 'module-resize-handle';
    handle.tabIndex = 0;
    handle.setAttribute('role', 'separator');
    handle.setAttribute('aria-orientation', 'horizontal');
    handle.setAttribute('aria-label', `Resize ${module.title} chart vertically`);
    handle.title = 'Drag to resize chart height; double-click to reset';
    card.appendChild(handle);

    const applyHeight = height => {
        const minimum = 480;
        const maximum = Math.max(900, window.innerHeight * 1.5);
        const next = Math.round(Math.min(maximum, Math.max(minimum, height)));
        card.classList.add('user-resized');
        card.style.height = `${next}px`;
        handle.setAttribute('aria-valuenow', String(next));
    };
    const redraw = () => requestAnimationFrame(() => module.resizePlot?.());

    handle.addEventListener('pointerdown', event => {
        if (event.button !== 0) return;
        event.preventDefault();
        const startY = event.clientY;
        const startHeight = card.getBoundingClientRect().height;
        handle.setPointerCapture(event.pointerId);
        card.classList.add('is-resizing');

        const move = moveEvent => applyHeight(startHeight + moveEvent.clientY - startY);
        const finish = () => {
            card.classList.remove('is-resizing');
            handle.removeEventListener('pointermove', move);
            handle.removeEventListener('pointerup', finish);
            handle.removeEventListener('pointercancel', finish);
            redraw();
        };
        handle.addEventListener('pointermove', move);
        handle.addEventListener('pointerup', finish);
        handle.addEventListener('pointercancel', finish);
    });

    handle.addEventListener('keydown', event => {
        if (!['ArrowUp', 'ArrowDown'].includes(event.key)) return;
        event.preventDefault();
        const step = event.shiftKey ? 80 : 32;
        applyHeight(card.getBoundingClientRect().height + (event.key === 'ArrowDown' ? step : -step));
        redraw();
    });

    handle.addEventListener('dblclick', () => {
        card.classList.remove('user-resized');
        card.style.removeProperty('height');
        handle.removeAttribute('aria-valuenow');
        redraw();
    });
}

function setupModules() {
    enabledModuleConfigs.forEach(config => {
        const root = document.getElementById(`modules-${config.group}`);
        if (!root) return;
        const card = document.createElement('article');
        card.className = 'module-card';
        card.dataset.module = config.id;
        const module = new moduleClasses[config.id](config.id, config.title, config.api_url);
        moduleObjects[config.id] = module;
        card.appendChild(module.renderSkeleton());
        const header = card.querySelector('.section-header');
        if (header && config.description) {
            const description = document.createElement('p');
            description.className = 'section-description';
            description.textContent = config.description;
            header.appendChild(description);
        }
        root.appendChild(card);
        module.init();
        attachModuleResizer(card, module);
    });
}

async function loadGroup(group, force = false, scope = currentRunScope()) {
    if (!isCurrentScope(scope) || !['tasks', 'workers', 'transfers', 'files'].includes(group)) return;
    const cacheKey = `${scope.run}:${scope.revision}:${group}`;
    if (!force && loadedGroups.has(cacheKey)) return;
    const configs = enabledModuleConfigs.filter(config => config.group === group);
    await Promise.all(configs.map(async config => {
        const module = moduleObjects[config.id];
        const card = document.querySelector(`[data-module="${config.id}"]`);
        card?.classList.add('is-loading');
        module.switchFolder(scope.run, scope.revision, scope.generation);
        if (config.timeLinked) module._globalTimeWindow = [state.timeStart, state.timeEnd];
        try {
            await module.fetchDataAndPlot();
        } catch (error) {
            if (error?.name === 'AbortError') return;
            console.error(`Failed to render ${config.id}`, error);
            card?.classList.add('has-error');
        } finally {
            if (isCurrentScope(scope)) card?.classList.remove('is-loading');
        }
    }));
    if (isCurrentScope(scope)) loadedGroups.add(cacheKey);
}

async function selectView(view, push = true) {
    if (!document.querySelector(`[data-view="${view}"]`)) view = 'overview';
    state.view = view;
    $$('.app-view').forEach(element => element.classList.toggle('active', element.dataset.view === view));
    $$('.nav-item[data-view]').forEach(element => element.classList.toggle('active', element.dataset.view === view));
    $('#app-sidebar').classList.remove('open');
    updateURL(!push);
    await loadGroup(view);
    if (view === 'explorer' && state.run) await loadTasks();
    $('.workspace').scrollTo({ top: 0, behavior: 'smooth' });
}

function setText(id, value) {
    const element = document.getElementById(id);
    if (element) element.textContent = value;
}

function renderFindings(findings) {
    $('#findings-list').innerHTML = findings.map(item => `
        <article class="finding ${escapeHTML(item.severity)}">
            <span class="finding-marker"></span>
            <div><strong>${escapeHTML(item.title)}</strong><p>${escapeHTML(item.detail)}</p></div>
        </article>`).join('');
}

function renderLatency(summary) {
    const metrics = [
        ['Execution median', summary.timing.execution.p50],
        ['Execution P95', summary.timing.execution.p95],
        ['Response mean', summary.timing.response.mean],
        ['Worker lifetime median', summary.timing.worker_lifetime.p50],
    ];
    const maximum = Math.max(1, ...metrics.map(([, value]) => Number(value) || 0));
    $('#latency-bars').innerHTML = metrics.map(([label, value]) => `
        <div class="latency-row"><label>${label}</label><div class="latency-track"><span style="width:${Math.max(0, (Number(value) || 0) / maximum * 100)}%"></span></div><strong>${formatDuration(value)}</strong></div>
    `).join('');
}

function renderSummary(summary) {
    state.summary = summary;
    $('#overview-empty').classList.add('hidden');
    $('#overview-dashboard').classList.remove('hidden');
    setText('overview-title', summary.run);
    const start = summary.start_time ? new Date(summary.start_time * 1000).toLocaleString() : 'Unknown start';
    setText('overview-subtitle', `${start} · ${formatDuration(summary.duration_seconds)} wall time · ${formatNumber(summary.files)} files`);
    setText('kpi-tasks', formatNumber(summary.total_tasks));
    setText('kpi-throughput', summary.throughput_tasks_per_second == null
        ? 'Unavailable under current data contract'
        : `${formatNumber(summary.throughput_tasks_per_second, 2)} successful tasks / second`);
    setText('kpi-success', formatPercent(summary.success_rate));
    setText('kpi-failures', `${formatNumber(summary.unsuccessful_tasks)} unsuccessful tasks`);
    setText('kpi-duration', formatDuration(summary.duration_seconds));
    setText('kpi-execution', `${formatDuration(summary.timing.execution.p50)} median execution`);
    setText('kpi-workers', formatNumber(summary.peak_active_workers));
    setText('kpi-worker-total', `${formatNumber(summary.workers)} total connections`);
    setText('kpi-transfer', formatBytesFromMB(summary.transferred_mb));
    setText('kpi-files', `${formatNumber(summary.files)} tracked files`);
    setText('kpi-storage', formatBytesFromMB(summary.peak_storage_mb));
    setText('kpi-created', `${formatBytesFromMB(summary.created_mb)} created`);
    renderFindings(summary.findings || []);
    renderLatency(summary);

    const quality = summary.data_quality || { status: 'UNKNOWN' };
    const severity = quality.status === 'FAIL' ? 'critical'
        : quality.status === 'PARTIAL' || quality.status === 'UNKNOWN' ? 'warning'
        : summary.findings?.some(item => item.severity === 'critical') ? 'critical'
        : summary.findings?.some(item => item.severity === 'warning') ? 'warning' : 'healthy';
    const badge = $('#health-badge');
    badge.className = `health-badge ${severity}`;
    badge.querySelector('strong').textContent = quality.status === 'FAIL' ? 'Invalid report data'
        : quality.status === 'PARTIAL' ? 'Partial report data'
        : quality.status === 'UNKNOWN' ? 'Data quality unknown'
        : severity === 'healthy' ? 'Healthy run'
        : severity === 'warning' ? 'Review suggested' : 'Action required';
    const qualityReason = (quality.reasons || []).join(', ');
    badge.title = qualityReason || `Parser evidence: ${quality.status}`;
}

async function loadSummary(scope = currentRunScope()) {
    if (!isCurrentScope(scope)) return;
    try {
        const summary = await getJSON(
            `/api/v1/runs/${encodeURIComponent(scope.run)}/revisions/${encodeURIComponent(scope.revision)}/summary`,
            { scope },
        );
        if (!isCurrentScope(scope)) return;
        renderSummary(summary);
        await loadSlowTasks(scope);
        showBanner('');
    } catch (error) {
        if (error?.name === 'AbortError') return;
        showBanner(`Unable to load dashboard: ${error.message}`);
    }
}

async function selectRun(run, { force = false } = {}) {
    if (!run || !state.runs.includes(run)) return;
    if (!force && run === state.run) return;
    const scope = beginRunScope(run);
    $('#log-selector').value = run;
    loadedGroups.clear();
    updateURL();
    try {
        const descriptor = await getJSON(`/api/v1/runs/${encodeURIComponent(run)}`, { scope });
        if (!isCurrentScope(scope)) return;
        state.revision = descriptor.current_revision;
        scope.revision = descriptor.current_revision;
    } catch (error) {
        if (error?.name === 'AbortError') return;
        showBanner(`Unable to activate run: ${error.message}`);
        return;
    }
    await loadSummary(scope);
    if (!isCurrentScope(scope)) return;
    if (['tasks', 'workers', 'transfers', 'files'].includes(state.view)) await loadGroup(state.view, true, scope);
    if (state.view === 'explorer') await loadTasks(scope);
    renderCompareOptions();
}

function taskRow(task) {
    return `<tr data-task-id="${task.task_id}" data-try-id="${task.try_id}">
        <td><strong>#${formatNumber(task.task_id)}</strong><br><small>Try ${formatNumber(task.try_id)}</small></td>
        <td><span class="status-pill ${task.status}">${task.status}</span></td>
        <td title="${escapeHTML(task.category)}">${escapeHTML(task.category)}</td>
        <td title="${escapeHTML(task.worker || '')}">${escapeHTML(task.worker || 'Not dispatched')}</td>
        <td>${formatNumber(task.cores)}</td>
        <td>${formatDuration(task.execution_seconds)}</td>
        <td>›</td>
    </tr>`;
}

async function fetchTasks({ query = '', status = 'all', limit = 50 } = {}, scope = currentRunScope()) {
    if (!isCurrentScope(scope)) return { tasks: [], matched: 0 };
    const params = new URLSearchParams({ query, status, limit });
    return getJSON(
        `/api/v1/runs/${encodeURIComponent(scope.run)}/revisions/${encodeURIComponent(scope.revision)}/tasks?${params}`,
        { scope },
    );
}

async function loadTasks(scope = currentRunScope()) {
    if (!isCurrentScope(scope)) return;
    const tableState = $('#task-table-state');
    tableState.classList.remove('hidden');
    tableState.textContent = 'Loading tasks…';
    try {
        const payload = await fetchTasks({ query: $('#task-search').value, status: $('#task-status-filter').value, limit: 100 }, scope);
        if (!isCurrentScope(scope)) return;
        $('#task-table tbody').innerHTML = payload.tasks.map(taskRow).join('');
        $('#task-table').classList.toggle('hidden', !payload.tasks.length);
        tableState.classList.toggle('hidden', Boolean(payload.tasks.length));
        if (!payload.tasks.length) tableState.textContent = 'No tasks match the current query.';
        setText('task-result-count', `${formatNumber(payload.matched)} matched · showing ${formatNumber(payload.tasks.length)}`);
        $('#task-table tbody').querySelectorAll('tr').forEach((row, index) => {
            row.addEventListener('click', () => openTaskDrawer(payload.tasks[index]));
        });
    } catch (error) {
        if (error?.name === 'AbortError') return;
        tableState.textContent = `Unable to load tasks: ${error.message}`;
    }
}

async function loadSlowTasks(scope = currentRunScope()) {
    if (!isCurrentScope(scope)) return;
    try {
        const payload = await fetchTasks({ limit: 4 }, scope);
        if (!isCurrentScope(scope)) return;
        $('#slow-task-list').innerHTML = payload.tasks.map(task => `
            <button class="compact-task" type="button" data-task-id="${task.task_id}" data-try-id="${task.try_id}">
                <span class="compact-task-id">#${task.task_id}</span><span><strong>${escapeHTML(task.category)}</strong><small>${escapeHTML(task.worker || 'Not dispatched')}</small></span><span class="compact-task-time">${formatDuration(task.execution_seconds)}</span>
            </button>`).join('') || '<div class="inline-state">No task rows available.</div>';
        $('#slow-task-list').querySelectorAll('.compact-task').forEach((button, index) => {
            button.addEventListener('click', () => openTaskDrawer(payload.tasks[index]));
        });
    } catch (error) {
        if (error?.name === 'AbortError') return;
        $('#slow-task-list').innerHTML = `<div class="inline-state">${escapeHTML(error.message)}</div>`;
    }
}

function openDrawer(id) {
    $('#modal-backdrop').classList.remove('hidden');
    const drawer = document.getElementById(id);
    drawer.classList.add('open');
    drawer.setAttribute('aria-hidden', 'false');
}

function closeDrawers() {
    const taskWasOpen = $('#task-drawer').classList.contains('open');
    const compareWasOpen = $('#compare-drawer').classList.contains('open');
    $('#modal-backdrop').classList.add('hidden');
    $$('.side-drawer').forEach(drawer => {
        drawer.classList.remove('open');
        drawer.setAttribute('aria-hidden', 'true');
    });
    if (taskWasOpen || compareWasOpen) {
        const url = new URL(window.location.href);
        if (taskWasOpen) url.searchParams.delete('task');
        if (compareWasOpen) url.searchParams.delete('compare');
        history.replaceState({}, '', url);
    }
}

function openTaskDrawer(task) {
    setText('task-drawer-title', `Task #${task.task_id}`);
    const fields = [
        ['Outcome', task.status], ['Try', task.try_id], ['Status code', task.status_code],
        ['Category', task.category], ['Worker', task.worker || 'Not dispatched'], ['Cores', task.cores],
        ['Execution', formatDuration(task.execution_seconds)], ['Recovery task', task.is_recovery ? 'Yes' : 'No'],
        ['Ready', task.ready_at], ['Running', task.running_at], ['Done', task.done_at],
        ['Input files', task.input_files || 'None'], ['Output files', task.output_files || 'None'],
    ];
    $('#task-drawer-body').innerHTML = `<div class="detail-grid">${fields.map(([label, value], index) => `
        <div class="detail-item ${index >= 11 ? 'full' : ''}"><label>${escapeHTML(label)}</label><div>${escapeHTML(value ?? '—')}</div></div>`).join('')}</div>`;
    const url = new URL(window.location.href);
    url.searchParams.set('task', task.task_id);
    history.replaceState({}, '', url);
    openDrawer('task-drawer');
}

function renderCompareOptions() {
    $('#compare-run-options').innerHTML = state.runs.map(run => `
        <label class="compare-option ${run === state.run ? 'baseline' : ''}">
            <input type="checkbox" value="${escapeHTML(run)}" ${run === state.run ? 'checked disabled' : ''}>
            <span>${escapeHTML(run)}</span><small>${run === state.run ? 'Baseline' : 'Candidate'}</small>
        </label>`).join('');
    $('#compare-run-options').querySelectorAll('input:not(:disabled)').forEach(input => {
        input.addEventListener('change', () => {
            const selected = $('#compare-run-options').querySelectorAll('input:checked');
            if (selected.length > 4) {
                input.checked = false;
                toast('Compare up to four runs at a time.');
            }
        });
    });
}

function comparisonMetric(summary, key) {
    const formatters = {
        duration_seconds: formatDuration, success_rate: formatPercent, total_tasks: formatNumber,
        workers: formatNumber, throughput_tasks_per_second: value => `${formatNumber(value, 2)}/s`,
        transferred_mb: formatBytesFromMB, peak_storage_mb: formatBytesFromMB,
    };
    return formatters[key](summary[key]);
}

function renderComparison(payload) {
    const metrics = [
        ['Wall time', 'duration_seconds', 'lower'], ['Success rate', 'success_rate', 'higher'],
        ['Application tasks', 'total_tasks', 'neutral'], ['Worker connections', 'workers', 'neutral'],
        ['Throughput', 'throughput_tasks_per_second', 'higher'], ['Data moved', 'transferred_mb', 'neutral'],
        ['Peak storage', 'peak_storage_mb', 'lower'],
    ];
    const headings = payload.runs.map(item => `<th>${escapeHTML(item.summary.run)}${item.summary.run === payload.baseline ? '<br><small>Baseline</small>' : ''}</th>`).join('');
    const rows = metrics.map(([label, key, preference]) => `<tr><td>${label}</td>${payload.runs.map(item => {
        const delta = item.delta_from_baseline[key];
        const good = delta != null && ((preference === 'higher' && delta > 0) || (preference === 'lower' && delta < 0));
        const bad = delta != null && ((preference === 'higher' && delta < 0) || (preference === 'lower' && delta > 0));
        return `<td><strong>${comparisonMetric(item.summary, key)}</strong>${delta == null || item.summary.run === payload.baseline ? '' : `<span class="delta ${good ? 'good' : bad ? 'bad' : ''}">${delta > 0 ? '+' : ''}${(delta * 100).toFixed(1)}%</span>`}</td>`;
    }).join('')}</tr>`).join('');
    $('#comparison-results').innerHTML = `<table class="comparison-table"><thead><tr><th>Metric</th>${headings}</tr></thead><tbody>${rows}</tbody></table>`;
}

async function runComparison() {
    const selected = Array.from($('#compare-run-options').querySelectorAll('input:checked')).map(input => input.value);
    if (selected.length < 2) return toast('Select at least one candidate run.');
    const button = $('#run-comparison');
    button.disabled = true;
    const scope = currentRunScope();
    try {
        const payload = await getJSON(`/api/v1/compare?runs=${encodeURIComponent(selected.join(','))}`, { scope });
        if (!isCurrentScope(scope)) return;
        renderComparison(payload);
        const url = new URL(window.location.href);
        url.searchParams.set('compare', selected.slice(1).join(','));
        history.replaceState({}, '', url);
    } catch (error) {
        if (error?.name === 'AbortError') return;
        toast(`Comparison failed: ${error.message}`);
    } finally {
        button.disabled = false;
    }
}

function setupTimeFilters() {
    const filters = $$('[data-time-filter]');
    const source = filters.find(element => element.querySelector('.time-start'));
    if (!source) return;
    const template = source.innerHTML;
    filters.forEach(element => { if (!element.innerHTML.trim()) element.innerHTML = template; });
    filters.forEach(filter => {
        const start = filter.querySelector('.time-start');
        const end = filter.querySelector('.time-end');
        const output = filter.querySelector('.time-output');
        start.value = state.timeStart;
        end.value = state.timeEnd;
        const apply = source => {
            let startValue = Number(start.value);
            let endValue = Number(end.value);
            if (endValue - startValue < 5) {
                if (source === start) startValue = endValue - 5; else endValue = startValue + 5;
                start.value = startValue;
                end.value = endValue;
            }
            state.timeStart = startValue;
            state.timeEnd = endValue;
            $$('[data-time-filter]').forEach(other => {
                other.querySelector('.time-start').value = startValue;
                other.querySelector('.time-end').value = endValue;
                other.querySelector('.time-output').textContent = `${startValue}% — ${endValue}%`;
            });
            enabledModuleConfigs.filter(config => config.timeLinked).forEach(config => {
                moduleObjects[config.id]?.applyGlobalTimeWindow(startValue, endValue);
            });
            updateURL();
        };
        start.addEventListener('input', () => apply(start));
        end.addEventListener('input', () => apply(end));
        filter.querySelector('.time-reset').addEventListener('click', () => {
            start.value = 0; end.value = 100; apply(end);
        });
        output.textContent = `${state.timeStart}% — ${state.timeEnd}%`;
    });
}

async function reloadRun() {
    if (!state.run) return toast('Select a run first.');
    const button = $('#reload-button');
    button.disabled = true;
    button.classList.add('is-reloading');
    button.setAttribute('aria-busy', 'true');
    try {
        const scope = currentRunScope();
        await getJSON(`/api/v1/runs/${encodeURIComponent(state.run)}/refresh`, {
            method: 'POST', scope, allowRevisionChange: true,
        });
        loadedGroups.clear();
        await selectRun(state.run, { force: true });
        toast('Run data reloaded.');
    } catch (error) {
        toast(`Reload failed: ${error.message}`);
    } finally {
        button.disabled = false;
        button.classList.remove('is-reloading');
        button.removeAttribute('aria-busy');
    }
}

async function shareView() {
    updateURL();
    try {
        await navigator.clipboard.writeText(window.location.href);
        toast('Shareable view link copied.');
    } catch {
        window.prompt('Copy this view link:', window.location.href);
    }
}

function bindEvents() {
    $$('.nav-item[data-view]').forEach(button => button.addEventListener('click', () => selectView(button.dataset.view)));
    $$('[data-view-target]').forEach(button => button.addEventListener('click', () => selectView(button.dataset.viewTarget)));
    $$('[data-action="compare"], #compare-button').forEach(button => button.addEventListener('click', () => { renderCompareOptions(); openDrawer('compare-drawer'); }));
    $$('.drawer-close').forEach(button => button.addEventListener('click', closeDrawers));
    $('#modal-backdrop').addEventListener('click', closeDrawers);
    document.addEventListener('keydown', event => { if (event.key === 'Escape') closeDrawers(); });
    $('#log-selector').addEventListener('change', event => selectRun(event.target.value));
    $('#reload-button').addEventListener('click', reloadRun);
    $('#share-button').addEventListener('click', shareView);
    $('#run-comparison').addEventListener('click', runComparison);
    $('#task-search-button').addEventListener('click', loadTasks);
    $('#task-search').addEventListener('keydown', event => { if (event.key === 'Enter') loadTasks(); });
    $('#task-status-filter').addEventListener('change', loadTasks);
    $('#mobile-menu').addEventListener('click', () => $('#app-sidebar').classList.toggle('open'));
    window.addEventListener('popstate', () => {
        const params = new URLSearchParams(window.location.search);
        selectView(params.get('view') || 'overview', false);
    });
}

async function initialize() {
    const requiredShell = ['#view-overview', '#overview-dashboard', '#modal-backdrop', '[data-time-filter]'];
    const missingShell = requiredShell.filter(selector => !document.querySelector(selector));
    if (missingShell.length) {
        showRestartRequired('This page loaded a new JavaScript bundle with an older cached HTML template.');
        console.error('TaskVine UI bundle/template mismatch. Restart vine_report to refresh the cached template.');
        return;
    }
    try {
        const health = await getJSON('/api/v1/health');
        if (!health.capabilities?.includes(REQUIRED_SERVER_CAPABILITY)) {
            showRestartRequired('This page loaded a newer JavaScript bundle than the running backend.');
            console.error(`TaskVine backend is missing capability ${REQUIRED_SERVER_CAPABILITY}.`);
            return;
        }
    } catch (error) {
        showBanner(`Dashboard initialization failed: ${error.message}`);
        return;
    }
    const params = new URLSearchParams(window.location.search);
    state.view = params.get('view') || 'overview';
    state.timeStart = Math.max(0, Math.min(95, Number(params.get('from')) || 0));
    state.timeEnd = Math.max(state.timeStart + 5, Math.min(100, Number(params.get('to')) || 100));
    setupModules();
    setupTimeFilters();
    bindEvents();
    try {
        const payload = await getJSON('/api/v1/runs');
        state.runs = payload.runs;
        const selector = $('#log-selector');
        selector.innerHTML = '<option value="">Select a run</option>' + state.runs.map(run => `<option value="${escapeHTML(run)}">${escapeHTML(run)}</option>`).join('');
        const requestedRun = params.get('run');
        const initialRun = state.runs.includes(requestedRun) ? requestedRun : state.runs[0];
        await selectView(state.view, false);
        if (initialRun) await selectRun(initialRun);
        else showBanner('No parsed runs with csv-files were found under this logs directory.');
        const compareRuns = (params.get('compare') || '').split(',').filter(Boolean);
        if (compareRuns.length && initialRun) {
            renderCompareOptions();
            compareRuns.forEach(run => {
                const input = Array.from($('#compare-run-options').querySelectorAll('input')).find(item => item.value === run);
                if (input) input.checked = true;
            });
            openDrawer('compare-drawer');
            await runComparison();
        }
        const requestedTask = params.get('task');
        if (requestedTask && initialRun) {
            const payload = await fetchTasks({ query: requestedTask, limit: 50 });
            const exact = payload.tasks.find(task => String(task.task_id) === String(requestedTask));
            if (exact) openTaskDrawer(exact);
        }
    } catch (error) {
        showBanner(`Dashboard initialization failed: ${error.message}`);
    }
}

document.addEventListener('DOMContentLoaded', initialize);
