import { Toolbox } from './toolbox.js?v=5.0';
import { LineCursor } from './line_cursor.js?v=1.3';
import { durationAxisSpec } from './time_format.js?v=1.0';
import { hasPinnedTooltip, pinTooltip, unpinTooltip } from './tooltip_pin.js?v=1.0';

let jsPDF = null;
let svg2pdf = null;

async function ensurePdfDependencies() {
    if (!jsPDF) {
        try {
            const module = await import("https://cdn.jsdelivr.net/npm/jspdf@2.5.1/+esm");
            jsPDF = module?.default ?? module.jsPDF ?? module;
        } catch (error) {
            console.error('Failed to load jsPDF:', error);
        }
    }

    if (!svg2pdf) {
        try {
            const module = await import("https://cdn.jsdelivr.net/npm/svg2pdf@1.1.1/src/index.min.js");
            svg2pdf = module?.default ?? module.svg2pdf ?? module;
        } catch (error) {
            console.error('Failed to load svg2pdf:', error);
        }
    }
}


const _hoverInstances = new Set();
let _hoverSafetyNetBound = false;

function _setupHoverSafetyNet() {
    if (_hoverSafetyNetBound) return;
    _hoverSafetyNetBound = true;
    document.addEventListener('mousemove', (event) => {
        if (hasPinnedTooltip()) return;
        const tooltip = document.getElementById('vine-tooltip');
        if (!tooltip || tooltip.style.visibility !== 'visible') return;
        const el = document.elementFromPoint(event.clientX, event.clientY);
        if (el?.closest('.plotting-container') || el?.id === 'vine-tooltip') return;
        _hoverInstances.forEach(inst => inst._resetHoverState?.());
    });
}

export class BaseModule {
    constructor(id, title, api_url) {
        this.id = id;
        this.title = title;
        this.api_url = api_url;
        this.endpointKind = 'charts';

        this._folder = null;
        this._revision = null;
        this._runGeneration = 0;
        this._requestSequence = 0;
        this._renderSequence = 0;
        this._abortController = null;
        this._fetchDataParams = {}; // Parameters for fetchData calls

        this.svgContainer = null;
        this.svgElement = null;
        this.svgNode = null;
        this.legendContainer = null;

        this.toolbox = null;
        this.toolboxContainer = null;
        
        this.tooltip = null;

        this.tickFontSize = '14px';
        this.padding = 48;
        this.highlightColor = '#d96f24';
        this.highlightRadius = 5;
        this.highlightStrokeWidth = 2;

        /* plotting parameters */
        this.svg = null;

        this.margin = null;

        this.legendCheckboxName = null;
        this.legendSingleSelect = false;

        this.topDomain = null;
        this.topTickValues = null;
        this.topFormatter = null;
        this.topScaleType = null;
        this.topScale = null;
        this.topAxis = null;

        this.rightDomain = null;
        this.rightTickValues = null;
        this.rightFormatter = null;
        this.rightScaleType = null;
        this.rightScale = null;
        this.rightAxis = null;

        this.bottomDomain = null;
        this.bottomTickValues = null;
        this.bottomFormatter = null;
        this.bottomScaleType = null;
        this.bottomScale = null;
        this.bottomAxis = null;

        this.leftDomain = null;
        this.leftTickValues = null;
        this.leftFormatter = null;
        this.leftScaleType = null;
        this.leftScale = null;
        this.leftAxis = null;

        this.xScale = null;
        this.yScale = null;

        // Add new properties for toolbox items
        this.toolboxSelectorItems = [];
        this.toolboxInputItems = [];
        this.toolboxButtonItems = [];

        this._hasScatterPlot = false;
        this._cdfMode = false;
        this._lineCursor = null;
        this._pinnedMark = null;
        this._plotClipId = null;
    }

    switchFolder(folder, revision = null, generation = 0) {
        this._abortController?.abort();
        this._folder = folder;
        this._revision = revision;
        this._runGeneration = generation;
        this._requestSequence += 1;
        this._renderSequence += 1;
        this._cdfMode = false;
        /* if the folder is switched, clear the plot */
        this.clearPlot();
    }

    renderSkeleton() {
        const section = document.createElement('div');
        section.className = 'section';
        section.id = this.id;

        section.innerHTML = `
        <div class="section-header" id="${this.id}-header">
            <h2 class="section-title">${this.title}</h2>
            <div id="${this.id}-toolbox-container" class="toolbox-container"></div>
        </div>
      
        <div class="section-content">
            <div class="section-row top">
                <div class="legend-container" id="${this.id}-legend"></div>
            </div>
      
            <div class="section-row bottom">
                <div class="plotting-container" id="${this.id}-container">
                    <svg id="${this.id}-d3-svg" xmlns="http://www.w3.org/2000/svg"></svg>
                </div>
            </div>
        </div>
      `;

        return section;
    }

    init() {
        this.svgContainer = document.getElementById(`${this.id}-container`);
        if (!this.svgContainer) {
            console.error(`SVG container not found for ${this.id}`);
            return;
        }
        this.svgElement = d3.select(`#${this.id}-d3-svg`);
        if (!this.svgElement) {
            console.error(`SVG element not found for ${this.id}`);
            return;
        }
        this.svgNode = this.svgElement.node();
        if (!this.svgNode) {
            console.error(`SVG node not found for ${this.id}`);
            return;
        }
        this.legendContainer = d3.select(document.getElementById(`${this.id}-legend`));
        if (!this.legendContainer.node()) {
            console.error(`Legend container not found for ${this.id}`);
            return;
        }
        this.toolboxContainer = document.getElementById(`${this.id}-toolbox-container`);
        if (!this.toolboxContainer) {
            console.error(`Toolbox container not found for ${this.id}`);
            return;
        }
        this.toolbox = new Toolbox({ id: `${this.id}-toolbox` });
        _hoverInstances.add(this);
        _setupHoverSafetyNet();
    }

    _resetHoverState() {
        if (hasPinnedTooltip()) return;
        this._lineCursor?.hide();
        const tooltip = document.getElementById('vine-tooltip');
        if (tooltip) tooltip.style.visibility = 'hidden';
        if (!this.svgNode) return;
        const svg = this.svgNode.closest('svg');
        if (!svg) return;
        svg.querySelectorAll('path[data-original-stroke]').forEach(p => {
            p.setAttribute('stroke', p.getAttribute('data-original-stroke') ?? '');
            p.setAttribute('stroke-width', p.getAttribute('data-original-stroke-width') ?? '');
        });
        svg.querySelectorAll('rect[data-original-fill]').forEach(r => {
            r.setAttribute('fill', r.getAttribute('data-original-fill') ?? '');
            r.setAttribute('opacity', r.getAttribute('data-original-opacity') ?? '1');
        });
        svg.querySelectorAll('circle[data-original-fill]').forEach(c => {
            c.setAttribute('fill', c.getAttribute('data-original-fill') ?? '');
            const r = c.getAttribute('data-original-r');
            if (r != null) c.setAttribute('r', r);
        });
    }

    async fetchDataAndPlot() {
        const renderSequence = ++this._renderSequence;
        this._clearModuleState();
        this.clearSVG();
        this.plotSpinner();
        try {
            const isReadyToPlot = await this.fetchData();
            if (!isReadyToPlot || renderSequence !== this._renderSequence) {
                return;
            }
            this._invokePlot();
            if (this._globalTimeWindow) {
                this.applyGlobalTimeWindow(...this._globalTimeWindow);
            }
        } catch (error) {
            if (error?.name === 'AbortError') return;
            if (renderSequence !== this._renderSequence) return;
            console.error(`Error fetching data for module ${this.id}:`, error);
            this._showModuleState('This chart could not be loaded. Retry or reload the run.', 'error');
        } finally {
            const existingSpinner = document.getElementById(`${this.id}-spinner`);
            if (existingSpinner && renderSequence === this._renderSequence) {
                existingSpinner.remove();
            }
        }
    }

    _clearModuleState() {
        document.getElementById(`${this.id}-module-state`)?.remove();
    }

    _showModuleState(message, kind = 'empty') {
        this._clearModuleState();
        const content = document.querySelector(`#${this.id} .section-content`);
        if (!content) return;
        const state = document.createElement('div');
        state.id = `${this.id}-module-state`;
        state.className = `module-state ${kind}`;
        state.textContent = message;
        content.appendChild(state);
    }

    _setDurationBottomAxis(domain, tickCount = 6) {
        if (this.data?.axes?.x?.unit !== 'seconds') return false;
        const axis = durationAxisSpec(domain, tickCount);
        this.setBottomTickValues(axis.tickValues);
        this.setBottomFormatter(axis.formatter);
        return true;
    }

    applyGlobalTimeWindow(startPercent = 0, endPercent = 100) {
        this._globalTimeWindow = [startPercent, endPercent];
        const fullDomain = this.data?.x_domain;
        if (!Array.isArray(fullDomain) || fullDomain.length !== 2 || this.bottomScaleType !== 'linear') {
            return false;
        }
        const [fullMin, fullMax] = fullDomain.map(Number);
        if (!Number.isFinite(fullMin) || !Number.isFinite(fullMax) || fullMax <= fullMin) {
            return false;
        }
        const start = Math.max(0, Math.min(95, Number(startPercent))) / 100;
        const end = Math.max(start + 0.01, Math.min(100, Number(endPercent))) / 100;
        const span = fullMax - fullMin;
        const domain = [fullMin + span * start, fullMin + span * end];
        this.setBottomDomain(domain);
        if (!this._setDurationBottomAxis(domain)) {
            this.setBottomTickValues(this._dedupeTicksByFormatter(
                this._computeLinearTicks(domain, 6), this.bottomFormatter
            ));
        }
        this.clearSVG();
        this._plotAxes();
        this._invokePlot();
        return true;
    }

    createToolboxItemDownloadSVG() {
        return this.toolbox.createButtonItem(`${this.id}-download-svg`, 'Download SVG', () => this.downloadSVG());
    }

    createToolboxItemRefetch() {
        return this.toolbox.createButtonItem(`${this.id}-refetch`, 'Reload and Plot', () => this.fetchDataAndPlot());
    }

    createToolboxItemReset() {
        return this.toolbox.createButtonItem(`${this.id}-reset`, 'Reset Plot', () => this.resetPlot());
    }

    createToolboxItemExport() {
        return this.toolbox.createSelectorItem(
            `${this.id}-export`,
            'Export',
            [
                { value: 'svg', label: 'SVG' },
                { value: 'pdf', label: 'PDF' },
                { value: 'png', label: 'PNG' },
                { value: 'jpg', label: 'JPG' },
                { value: 'jpeg', label: 'JPEG' },
                { value: 'csv', label: 'CSV' }
            ],
            (id, value) => {
                if (value === 'svg') {
                    this.downloadSVG();
                } else if (value === 'png') {
                    this.downloadPNG();
                } else if (value === 'pdf') {
                    this.downloadPDF();
                } else if (value === 'jpg') {
                    this.downloadJPG();
                } else if (value === 'jpeg') {
                    this.downloadJPEG();
                } else if (value === 'csv') {
                    this.downloadCSV();
                }
            }
        );
    }

    setXLimits({ xmin = null, xmax = null }) {
        if (!this.bottomScaleType || this.bottomScaleType !== 'linear') {
            console.warn('setXLimits only applies to linear bottom scale');
            return;
        }
    
        if (!Array.isArray(this.bottomDomain) || this.bottomDomain.length !== 2) {
            console.warn('Invalid bottom domain');
            return;
        }
    
        const [oldMin, oldMax] = this.bottomDomain;
        const newMin = xmin !== null ? +xmin : oldMin;
        const newMax = xmax !== null ? +xmax : oldMax;
    
        if (isNaN(newMin) || isNaN(newMax) || newMin < 0 || newMax <= newMin) {
            console.warn(`Invalid new domain: [${newMin}, ${newMax}]`);
            return;
        }
    
        this.setBottomDomain([newMin, newMax]);

        if (this._setDurationBottomAxis([newMin, newMax])) {
            this.clearSVG();
            this._plotAxes();
            this._invokePlot();
            return;
        }
    
        const oldTicks = this.bottomTickValues;
    
        const oldRange = oldMax - oldMin;
        const newRange = newMax - newMin;
        const scaleRatio = newRange / oldRange;
    
        const newTicks = oldTicks.map(v => {
            if (v <= oldMin) return newMin;
            if (v >= oldMax) return newMax;
            const offset = v - oldMin;
            return newMin + offset * scaleRatio;
        });
    
        this.setBottomTickValues(newTicks);
        
        this.clearSVG();
        this._plotAxes();
        this._invokePlot();
    }      

    createToolboxItemSetXMin() {
        return this.toolbox.createInputItem(`${this.id}-x-min`, 'Set X Min', (id, value) => {
            const parsed = parseFloat(value);
            if (!isNaN(parsed)) {
                this.setXLimits({ xmin: parsed });
            } else {
                console.warn('Invalid value for X Min');
            }
        });
    }    

    createToolboxItemSetXMax() {
        return this.toolbox.createInputItem(`${this.id}-x-max`, 'Set X Max', (id, value) => {
            const parsed = parseFloat(value);
            if (!isNaN(parsed)) {
                this.setXLimits({ xmax: parsed });
            } else {
                console.warn('Invalid value for X Max');
            }
        });
    }

    setYLimits({ ymin = null, ymax = null }) {
        if (!this.leftScaleType || this.leftScaleType !== 'linear') {
            console.warn('setYLimits only applies to linear left scale');
            return;
        }
    
        if (!Array.isArray(this.leftDomain) || this.leftDomain.length !== 2) {
            console.warn('Invalid left domain');
            return;
        }
    
        const [oldMin, oldMax] = this.leftDomain;
        const newMin = ymin !== null ? +ymin : oldMin;
        const newMax = ymax !== null ? +ymax : oldMax;
    
        if (isNaN(newMin) || isNaN(newMax) || newMin < 0 || newMax <= newMin) {
            console.warn(`Invalid new domain: [${newMin}, ${newMax}]`);
            return;
        }
    
        this.setLeftDomain([newMin, newMax]);
    
        const oldTicks = this.leftTickValues;
        const tickCount = oldTicks?.length || 5;
    
        const oldRange = oldMax - oldMin;
        const newRange = newMax - newMin;
        const scaleRatio = newRange / oldRange;
    
        const newTicks = oldTicks.map(v => {
            if (v <= oldMin) return newMin;
            if (v >= oldMax) return newMax;
            const offset = v - oldMin;
            return newMin + offset * scaleRatio;
        });
    
        this.setLeftTickValues(newTicks);
        
        // Clear the SVG and replot axes before calling plot()
        this.clearSVG();
        this._plotAxes();
        this._invokePlot();
    }    

    createToolboxItemSetYMin() {
        return this.toolbox.createInputItem(`${this.id}-y-min`, 'Set Y Min', (id, value) => {
            const parsed = parseFloat(value);
            if (!isNaN(parsed)) {
                this.setYLimits({ ymin: parsed });
            } else {
                console.warn('Invalid value for Y Min');
            }
        });
    }
    
    createToolboxItemSetYMax() {
        return this.toolbox.createInputItem(`${this.id}-y-max`, 'Set Y Max', (id, value) => {
            const parsed = parseFloat(value);
            if (!isNaN(parsed)) {
                this.setYLimits({ ymax: parsed });
            } else {
                console.warn('Invalid value for Y Max');
            }
        });
    }

    _selectAllLegendCheckboxes() {
        this._queryAllLegendCheckboxes()
            .property('checked', true)
            .each((d, i, nodes) => {
                const element = d3.select(nodes[i]).node();
                if (element) {
                    const id = element.getAttribute('id');
                    const visible = true;
                    this.legendOnToggle(id, visible);
                } else {
                    console.warn('Invalid element in each()');
                }
            });
    }

    _unselectAllLegendCheckboxes() {
        this._queryAllLegendCheckboxes()
            .property('checked', false)
            .each((d, i, nodes) => {
                const element = d3.select(nodes[i]).node();
                if (element) {
                    const id = element.getAttribute('id');
                    const visible = false;
                    this.legendOnToggle(id, visible);
                } else {
                    console.warn('Invalid element in each()');
                }
            });
    }

    createToolboxItemSelectAll() {
        return this.toolbox.createButtonItem(`${this.id}-select-all`, 'Select All Legend', () => this._selectAllLegendCheckboxes());
    }

    createToolboxItemClearAll() {
        return this.toolbox.createButtonItem(`${this.id}-clear-all`, 'Clear All Legend', () => this._unselectAllLegendCheckboxes());
    }
    
    _setToolboxItems(items) {
        this.toolbox.setItems(items);
        this.toolbox.mount(this.toolboxContainer);
    }

    _initToolbox() {
        /* if the toolbox is already initialized, clear it and initialize it again */
        if (this.toolbox) {
            this.clearToolbox();
        }

        // Initialize default items
        this.toolboxSelectorItems = [this.createToolboxItemExport()];
        this.toolboxInputItems = [];
        this.toolboxButtonItems = [];

        // Add scale-related input items
        if (this.bottomScaleType === 'linear') {
            this.toolboxInputItems.push(this.createToolboxItemSetXMin());
            this.toolboxInputItems.push(this.createToolboxItemSetXMax());
        }
        if (this.leftScaleType === 'linear') {
            this.toolboxInputItems.push(this.createToolboxItemSetYMin());
            this.toolboxInputItems.push(this.createToolboxItemSetYMax());
        }

        // Add legend-related button items
        if (this._queryAllLegendCheckboxes().size() > 0 && !this.legendSingleSelect) {
            this.toolboxButtonItems.push(this.createToolboxItemSelectAll());
            this.toolboxButtonItems.push(this.createToolboxItemClearAll());
        }

        // Allow subclasses to add their own items
        this._addCustomToolboxItems();

        if (this._hasScatterPlot) {
            this.toolboxButtonItems.push(this.createToolboxItemCdfToggle());
        }

        // Combine all items, ensuring Reset and Refetch are in the correct positions
        const items = [
            ...this.toolboxSelectorItems,
            ...this.toolboxInputItems,
            ...this.toolboxButtonItems,
            this.createToolboxItemReset(),  // Always second to last
            this.createToolboxItemRefetch() // Always last
        ];

        this._setToolboxItems(items);
    }

    // New method for subclasses to override and add custom items
    _addCustomToolboxItems() {
        // To be overridden by subclasses
    }

    // Helper methods to add items
    addToolboxSelectorItem(item) {
        this.toolboxSelectorItems.push(item);
    }

    addToolboxInputItem(item) {
        this.toolboxInputItems.push(item);
    }

    addToolboxButtonItem(item) {
        this.toolboxButtonItems.push(item);
    }

    removeToolboxItem(itemId) {
        // Remove from all toolbox item arrays
        this.toolboxSelectorItems = this.toolboxSelectorItems.filter(item => item.id !== itemId);
        this.toolboxInputItems = this.toolboxInputItems.filter(item => item.id !== itemId);
        this.toolboxButtonItems = this.toolboxButtonItems.filter(item => item.id !== itemId);
        
        // Remove the actual DOM element
        const element = document.getElementById(itemId);
        if (element) {
            element.remove();
        }
    }

    clearLegend() {
        if (this.legendContainer && this.legendContainer.node()) {
            this.legendContainer.html('');
        }
        this.legendCheckboxName = null;
    }

    clearSVG() {
        this.releasePinnedTooltip();
        this._lineCursor?.destroy();
        this._lineCursor = null;
        /* remove the node from the DOM */
        while (this.svgNode.firstChild) {
            this.svgNode.removeChild(this.svgNode.firstChild);
        }

        /* remove the width and height attributes */
        this.svgNode.removeAttribute('width');
        this.svgNode.removeAttribute('height');
        this.svg = null;

        /* remove the spinner */
        const existingSpinner = document.getElementById(`${this.id}-spinner`);
        if (existingSpinner) {
            existingSpinner.remove();
        }
    }

    plotSpinner() {
        /* remove the existing spinner */
        const existingSpinner = document.getElementById(`${this.id}-spinner`);
        if (existingSpinner) {
            existingSpinner.remove();
        }
        
        /* create the spinner container */
        const spinnerContainer = document.createElement('div');
        spinnerContainer.className = 'vine-spinner-container';
        spinnerContainer.id = `${this.id}-spinner`;
        
        /* create the spinner element */
        const spinner = document.createElement('div');
        spinner.className = 'vine-spinner';
        
        /* add the spinner styles */
        const style = document.createElement('style');
        if (!document.getElementById('vine-spinner-styles')) {
            style.id = 'vine-spinner-styles';
            style.textContent = `
                .vine-spinner-container {
                    position: absolute;
                    top: 0;
                    left: 0;
                    right: 0;
                    bottom: 0;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    background-color: rgba(255, 255, 255, 0.8);
                    z-index: 1000;
                }
                
                .vine-spinner {
                    width: 40px;
                    height: 40px;
                    border: 4px solid #f3f3f3;
                    border-top: 4px solid #3498db;
                    border-radius: 50%;
                    animation: vine-spin 1s linear infinite;
                }
                
                @keyframes vine-spin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                }
            `;
            document.head.appendChild(style);
        }
        
        spinnerContainer.appendChild(spinner);
        
        /* ensure the svg container has relative positioning */
        if (getComputedStyle(this.svgContainer).position === 'static') {
            this.svgContainer.style.position = 'relative';
        }
        
        /* add the spinner to the container */
        this.svgContainer.appendChild(spinnerContainer);
    }

    clearToolbox() {
        if (this.toolbox) {
            this.toolbox.destroy();
            this.toolbox = new Toolbox({ id: `${this.id}-toolbox` });
        }
    
        if (this.toolboxContainer) {
            this.toolboxContainer.innerHTML = '';
        }
    }

    legendOnToggle(id, visible) {}
    
    defineFetchDataParams(params = {}) {
        this._fetchDataParams = { ...params };
    }

    updateFetchDataParam(key, value) {
        this._fetchDataParams[key] = value;
    }

    _parseFetchDataParams() {
        return { ...this._fetchDataParams };
    }
    
    async fetchData(extraParams = {}) {
        // Parse parameters from the module-specific parameter definition
        const moduleParams = this._parseFetchDataParams();
        
        // Merge module params with any extra params (extra params take precedence)
        const allParams = { ...moduleParams, ...extraParams };
        
        const requestFolder = this._folder;
        const requestRevision = this._revision;
        const requestGeneration = this._runGeneration;
        const requestSequence = ++this._requestSequence;
        this._abortController?.abort();
        const controller = new AbortController();
        this._abortController = controller;

        const params = new URLSearchParams();
        
        // Add any extra parameters passed by subclasses
        // Support both simple key-value pairs and JSON-style objects
        Object.entries(allParams).forEach(([key, value]) => {
            if (value !== undefined && value !== null) {
                if (Array.isArray(value)) {
                    // Handle arrays by appending each value
                    value.forEach(item => {
                        params.append(key, String(item));
                    });
                } else if (typeof value === 'object') {
                    // Handle objects by converting to JSON string
                    params.append(key, JSON.stringify(value));
                } else {
                    // Handle primitive values
                    params.append(key, String(value));
                }
            }
        });
        
        const baseURL = requestRevision
            ? `/api/v1/runs/${encodeURIComponent(requestFolder)}/revisions/${encodeURIComponent(requestRevision)}/${this.endpointKind}/${encodeURIComponent(this.id)}`
            : this.api_url;
        if (!requestRevision) params.set('folder', requestFolder);
        const response = await fetch(`${baseURL}?${params.toString()}`, { signal: controller.signal });
        const payload = await response.json();
        const apiError = typeof payload?.error === 'object' ? payload.error.message : payload?.error;
        const rawData = payload?.schema_version === 'report-api/v1' ? payload.data : payload;

        const stale = controller.signal.aborted
            || requestFolder !== this._folder
            || requestRevision !== this._revision
            || requestGeneration !== this._runGeneration
            || requestSequence !== this._requestSequence
            || (payload?.run_id && payload.run_id !== requestFolder)
            || (payload?.revision && payload.revision !== requestRevision);
        if (stale) return false;
        const data = this.transformFetchedData(rawData);
        
        if (!response.ok || !data || apiError || data.error) {
            console.warn(
                `Skipping plot for ${this.id} due to invalid backend response from ${this.api_url}`,
                payload
            );
            this.clearLegend();
            this.clearToolbox();
            this.clearSVG();
            this._showModuleState(apiError || data?.error || 'No compatible data is available for this chart.', 'empty');
            return false;
        }

        this.data = data;

        this._setAxesFromFetchedData();

        /* we are safe to plot axes after data is fetched */
        this._plotAxes();
        /* legend must be set after axes are plotted */
        this.initLegend();
        /* after the axes are plotted, we can setup zoom and scroll */
        this._setupZoomAndScroll();
        this._initResizeHandler();
        
        /* toolbox must be set after the legend is set b/c it checks if there are checkboxes */
        this._initToolbox();
        return true;
    }

    transformFetchedData(data) {
        return data;
    }

    plot() {}

    _initZoomTrackingAfterRender() {
        const scrollbarAllowance = 4;
        this.originalWidth = Math.floor(this.svgContainer.clientWidth - scrollbarAllowance);
        this.originalHeight = Math.floor(this.svgContainer.clientHeight - scrollbarAllowance);
    
        this.svgNode.style.width = `${this.originalWidth}px`;
        this.svgNode.style.height = `${this.originalHeight}px`;
    
        this.currentWidth = this.originalWidth;
        this.currentHeight = this.originalHeight;
    }
    
    _setupZoomAndScroll() {
        this._initZoomTrackingAfterRender();
        this._ensureZoomControls();
        
        /* do not bind the handler multiple times */
        if (this._zoomHandlerInitialized) return;
        this._zoomHandlerInitialized = true;

        this.svgNode.removeAttribute('width');
        this.svgNode.removeAttribute('height');
    
        this.svgContainer.addEventListener('wheel', (event) => {
            if (event.ctrlKey || event.metaKey) {
                event.preventDefault();
                const rect = this.svgNode.getBoundingClientRect();
                this._applyZoom(event.deltaY < 0 ? 1.08 : 0.92, {
                    x: event.clientX - rect.left,
                    y: event.clientY - rect.top,
                });
            }
        });
    }

    _applyZoom(factor, anchor = null) {
        if (!this.svgNode || !this.originalWidth || !this.originalHeight) return;
        const rect = this.svgNode.getBoundingClientRect();
        const point = anchor || { x: rect.width / 2, y: rect.height / 2 };
        const scaleX = rect.width ? point.x / rect.width : 0.5;
        const scaleY = rect.height ? point.y / rect.height : 0.5;
        const width = Math.min(this.originalWidth * 8, Math.max(this.originalWidth, this.currentWidth * factor));
        const height = Math.min(this.originalHeight * 8, Math.max(this.originalHeight, this.currentHeight * factor));
        if (width === this.currentWidth && height === this.currentHeight) return;

        this.svgNode.style.width = `${Math.round(width)}px`;
        this.svgNode.style.height = `${Math.round(height)}px`;
        const next = this.svgNode.getBoundingClientRect();
        this.svgContainer.scrollLeft += scaleX * next.width - point.x;
        this.svgContainer.scrollTop += scaleY * next.height - point.y;
        this.currentWidth = next.width;
        this.currentHeight = next.height;
        this._updateZoomControls();
    }

    _resetZoom() {
        if (!this.svgNode) return;
        this.svgNode.style.width = `${this.originalWidth}px`;
        this.svgNode.style.height = `${this.originalHeight}px`;
        this.svgContainer.scrollTo({ left: 0, top: 0 });
        this.currentWidth = this.originalWidth;
        this.currentHeight = this.originalHeight;
        this._updateZoomControls();
    }

    _ensureZoomControls() {
        let controls = this.svgContainer.querySelector('.chart-zoom-controls');
        if (!controls) {
            controls = document.createElement('div');
            controls.className = 'chart-zoom-controls';
            controls.innerHTML = `
                <span class="chart-zoom-hint">Ctrl/⌘ + scroll</span>
                <button type="button" data-zoom="out" aria-label="Zoom out">−</button>
                <button type="button" data-zoom="reset" aria-label="Reset zoom">100%</button>
                <button type="button" data-zoom="in" aria-label="Zoom in">+</button>`;
            controls.addEventListener('click', event => {
                const action = event.target.closest('button')?.dataset.zoom;
                if (action === 'in') this._applyZoom(1.2);
                else if (action === 'out') this._applyZoom(1 / 1.2);
                else if (action === 'reset') this._resetZoom();
            });
            this.svgContainer.appendChild(controls);
        }
        this._zoomControls = controls;
        this._updateZoomControls();
    }

    _updateZoomControls() {
        if (!this._zoomControls || !this.originalWidth) return;
        const percent = Math.round(100 * this.currentWidth / this.originalWidth);
        const reset = this._zoomControls.querySelector('[data-zoom="reset"]');
        const out = this._zoomControls.querySelector('[data-zoom="out"]');
        if (reset) reset.textContent = `${percent}%`;
        if (out) out.disabled = percent <= 100;
    }
    
    
    setTopDomain(domain) {
        this.topDomain = domain;
    }

    setRightDomain(domain) {
        this.rightDomain = domain;
    }

    setBottomDomain(domain) {
        this.bottomDomain = domain;
    }

    setLeftDomain(domain) {
        this.leftDomain = domain;
    }

    setTopTickValues(tickValues) {
        this.topTickValues = tickValues;
    }

    setRightTickValues(tickValues) {
        this.rightTickValues = tickValues;
    }

    setBottomTickValues(tickValues) {
        this.bottomTickValues = tickValues;
    }

    setLeftTickValues(tickValues) {
        this.leftTickValues = tickValues;
    }

    setTopFormatter(formatter) {
        this.topFormatter = formatter;
    }

    setRightFormatter(formatter) {
        this.rightFormatter = formatter;
    }   

    setBottomFormatter(formatter) {
        this.bottomFormatter = formatter;
    }

    setLeftFormatter(formatter) {
        this.leftFormatter = formatter;
    }

    setTopScaleType(scaleType) {
        this.topScaleType = scaleType;
    }

    setRightScaleType(scaleType) {
        this.rightScaleType = scaleType;
    }

    setBottomScaleType(scaleType) {
        this.bottomScaleType = scaleType;
    }

    setLeftScaleType(scaleType) {
        this.leftScaleType = scaleType;
    }

    createTopAxis() {
        if (!this.topScaleType) {
            return;
        }
        return d3.axisTop(this.topScale)
            .tickSizeOuter(0)
            .tickValues(this.topTickValues)
            .tickFormat(this.topFormatter);
    }

    createBottomAxis() {
        if (!this.bottomScaleType) {
            return;
        }
        return d3.axisBottom(this.bottomScale)
            .tickSizeOuter(0)
            .tickValues(this.bottomTickValues)
            .tickFormat(this.bottomFormatter);
    }

    createLeftAxis() {
        if (!this.leftScaleType) {
            return;
        }
        return d3.axisLeft(this.leftScale)
            .tickSizeOuter(0)
            .tickValues(this.leftTickValues)
            .tickFormat(this.leftFormatter);
    }

    createRightAxis() {
        if (!this.rightScaleType) {
            return;
        }
        return d3.axisRight(this.rightScale)
            .tickSizeOuter(0)
            .tickValues(this.rightTickValues)
            .tickFormat(this.rightFormatter);
    }

    createScale(scaleType) {
        if (scaleType === 'linear') {
            return d3.scaleLinear();
        } else if (scaleType === 'band') {
            return d3.scaleBand();
        } else {
            /* silently reject */
            return null;
        }
    }

    createScales() {
        if (this.topScaleType) {
            this.topScale = this.createScale(this.topScaleType);
            this.topScale.domain(this.topDomain);
        }
        if (this.rightScaleType) {
            this.rightScale = this.createScale(this.rightScaleType);
            this.rightScale.domain(this.rightDomain);
        }
        if (this.bottomScaleType) {
            this.bottomScale = this.createScale(this.bottomScaleType);
            this.bottomScale.domain(this.bottomDomain);
        }
        if (this.leftScaleType) {
            this.leftScale = this.createScale(this.leftScaleType);
            this.leftScale.domain(this.leftDomain);
        }
    }

    setScaleRanges(svgWidth, svgHeight) {
        if (this.topScaleType) {
            this.topScale.range([0, svgWidth]);
        }
        if (this.rightScaleType) {
            this.rightScale.range([svgHeight, 0]);
        }
        if (this.bottomScaleType) {
            this.bottomScale.range([0, svgWidth]);
        }
        if (this.leftScaleType) {
            this.leftScale.range([svgHeight, 0]);
        }
    }

    createAxes() {
        if (this.topScaleType) {
            this.topAxis = this.createTopAxis();
        }
        if (this.rightScaleType) {
            this.rightAxis = this.createRightAxis();
        }
        if (this.bottomScaleType) {
            this.bottomAxis = this.createBottomAxis();
        }
        if (this.leftScaleType) {
            this.leftAxis = this.createLeftAxis();
        }
    }

    getScaleBandWidth(scale) {
        if (!scale) {
            console.error('Invalid scale type for getScaleBandWidth');
            return 0;
        }
        return Math.max(0, scale.bandwidth());
    }

    /* plot all axes */
    _plotAxes() {

        /* create the scales */
        this.createScales();

        /* calculate the margin */
        const margin = this.calculateAxesMargin();

        /* calculate svg dimensions */
        const svgWidth = this.svgContainer.clientWidth - margin.left - margin.right;
        const svgHeight = this.svgContainer.clientHeight - margin.top - margin.bottom;

        /* update the scale ranges */
        this.setScaleRanges(svgWidth, svgHeight);

        /* define axes */
        this.createAxes();

        /* initialize svg */
        this.svg = this.svgElement
            .attr('viewBox', `0 0 ${this.svgContainer.clientWidth} ${this.svgContainer.clientHeight}`)
            .attr('preserveAspectRatio', 'xMidYMid meet')
            .append('g')
            .attr('transform', `translate(${margin.left}, ${margin.top})`);
        this._plotClipId = `${this.id.replace(/[^a-zA-Z0-9_-]/g, '-')}-plot-clip-${this._renderSequence}`;
        this.svg.append('defs')
            .append('clipPath')
            .attr('id', this._plotClipId)
            .append('rect')
            .attr('width', svgWidth)
            .attr('height', svgHeight);
        
        /* plot the axes */
        if (this.topScaleType) {
            this.svg.append('g')
                .attr('transform', `translate(0, 0)`)
                .call(this.topAxis)
                .selectAll('text')
                .style('font-size', this.tickFontSize);
        }
        if (this.rightScaleType) {
            this.svg.append('g')
                .attr('transform', `translate(${svgWidth}, 0)`)
                .call(this.rightAxis)
                .selectAll('text')
                .style('font-size', this.tickFontSize);
        }
        if (this.bottomScaleType) {
            this.svg.append('g')
                .attr('transform', `translate(0, ${svgHeight})`)
                .call(this.bottomAxis)
                .selectAll('text')
                .style('font-size', this.tickFontSize);
        }
        if (this.leftScaleType) {
            this.svg.append('g')
                .attr('transform', `translate(0, 0)`)
                .call(this.leftAxis)
                .selectAll('text')
                .style('font-size', this.tickFontSize);
        }
    }

    plotPath(points, options = {}) {
        const {
            stroke = '#2077B4',
            strokeWidth = 1.5,
            className = 'data-path',
            id = '',
            curveType = d3.curveStepAfter,
            tooltipInnerHTML = null,
            tooltipFormatter = null,
            disableHover = false
        } = options;

        const tooltip = document.getElementById('vine-tooltip');

        const line = d3.line()
            .x(d => this.bottomScale(d[0]))
            .y(d => this.leftScale(d[1]))
            .defined(d => d != null && d[0] != null && d[1] != null && !Number.isNaN(d[0]) && !Number.isNaN(d[1]))
            .curve(curveType);

        const pathElement = this.svg.append('path')
            .datum(points)
            .attr('fill', 'none')
            .attr('stroke', stroke)
            .attr('stroke-width', strokeWidth)
            .attr('d', line)
            .attr('data-original-stroke', stroke)
            .attr('data-original-stroke-width', strokeWidth)
            .attr('class', `${className} ${className}-${id}`)
            .attr('id', id)
            .attr('clip-path', this._plotClipId ? `url(#${this._plotClipId})` : null);

        if (!disableHover) {
            pathElement
                .on('mouseover', (event) => {
                    this.svg.selectAll(`path.${className}`)
                        .filter(el => el !== event.currentTarget)
                        .attr('stroke', '#ddd')
                        .attr('stroke-width', strokeWidth);
                    d3.select(event.currentTarget)
                        .attr('stroke', this.highlightColor)
                        .attr('stroke-width', this.highlightStrokeWidth)
                        .raise();
                    if (tooltipInnerHTML) {
                        tooltip.innerHTML = tooltipInnerHTML;
                        tooltip.style.visibility = 'visible';
                        tooltip.style.top = (event.pageY + 10) + 'px';
                        tooltip.style.left = (event.pageX + 10) + 'px';
                    } else if (tooltipFormatter) {
                        tooltip.innerHTML = tooltipFormatter(event.currentTarget);
                        tooltip.style.visibility = 'visible';
                        tooltip.style.top = (event.pageY + 10) + 'px';
                        tooltip.style.left = (event.pageX + 10) + 'px';
                    } else {
                        tooltip.style.visibility = 'hidden';
                    }
                })
                .on('mouseout', () => {
                    this.svg.selectAll(`path.${className}`)
                        .each(function () {
                            const sel = d3.select(this);
                            sel.attr('stroke', sel.attr('data-original-stroke'))
                               .attr('stroke-width', sel.attr('data-original-stroke-width'));
                        });
                    tooltip.style.visibility = 'hidden';
                });
        }
    }

    installLineCursor(series, options = {}) {
        this._lineCursor?.destroy();
        this._lineCursor = new LineCursor({
            svg: this.svg,
            xScale: this.bottomScale,
            yScale: this.leftScale,
            series,
            tooltip: document.getElementById('vine-tooltip'),
            ...options,
        });
        this._lineCursor.mount();
    }

    _positionMarkTooltip(event) {
        const tooltip = document.getElementById('vine-tooltip');
        if (!tooltip) return;
        const gap = 12;
        const bounds = tooltip.getBoundingClientRect();
        let left = event.clientX + gap;
        if (left + bounds.width > window.innerWidth - 10) left = event.clientX - bounds.width - gap;
        const top = Math.max(10, Math.min(event.clientY + gap, window.innerHeight - bounds.height - 10));
        tooltip.style.left = `${Math.max(10, left)}px`;
        tooltip.style.top = `${top}px`;
    }

    _showMarkTooltip(event, html, pinned = false) {
        const tooltip = document.getElementById('vine-tooltip');
        if (!tooltip || html == null) return;
        tooltip.classList.remove('line-cursor-tooltip');
        tooltip.classList.toggle('cursor-pinned', pinned);
        tooltip.innerHTML = `${html}<div class="mark-tooltip-status">${pinned ? 'Pinned · Press Esc to release' : 'Click to pin'}</div>`;
        tooltip.style.visibility = 'visible';
        this._positionMarkTooltip(event);
    }

    _bindPinnableMark(selection, { tooltipHTML, highlight, restore, label }) {
        const tooltip = document.getElementById('vine-tooltip');
        const show = (event, datum, pinned = false) => {
            highlight(event.currentTarget);
            this._showMarkTooltip(event, tooltipHTML(datum), pinned);
        };
        selection
            .attr('tabindex', 0)
            .attr('role', 'button')
            .attr('aria-pressed', 'false')
            .attr('aria-label', label)
            .on('mouseover', (event, datum) => {
                if (hasPinnedTooltip()) return;
                show(event, datum);
            })
            .on('mousemove', event => {
                if (!hasPinnedTooltip()) this._positionMarkTooltip(event);
            })
            .on('mouseout', event => {
                if (hasPinnedTooltip()) return;
                restore(event.currentTarget);
                if (tooltip) tooltip.style.visibility = 'hidden';
            })
            .on('click', (event, datum) => {
                event.stopPropagation();
                event.currentTarget.focus();
                if (this._pinnedMark?.node === event.currentTarget) {
                    this.releasePinnedTooltip();
                    return;
                }
                if (this._pinnedMark) {
                    this._pinnedMark.restore(this._pinnedMark.node);
                    d3.select(this._pinnedMark.node)
                        .attr('aria-pressed', 'false')
                        .classed('is-pinned', false);
                }
                pinTooltip(this);
                this._pinnedMark = { node: event.currentTarget, restore };
                d3.select(event.currentTarget).attr('aria-pressed', 'true').classed('is-pinned', true);
                show(event, datum, true);
            })
            .on('keydown', (event, datum) => {
                if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault();
                    event.currentTarget.dispatchEvent(new MouseEvent('click', {
                        bubbles: true,
                        clientX: event.currentTarget.getBoundingClientRect().left,
                        clientY: event.currentTarget.getBoundingClientRect().top,
                    }));
                } else if (event.key === 'Escape') {
                    this.releasePinnedTooltip();
                }
            });
        return selection;
    }

    releasePinnedTooltip() {
        if (!this._pinnedMark) return;
        const { node, restore } = this._pinnedMark;
        this._pinnedMark = null;
        restore(node);
        d3.select(node).attr('aria-pressed', 'false').classed('is-pinned', false);
        unpinTooltip(this);
        const tooltip = document.getElementById('vine-tooltip');
        if (tooltip) {
            tooltip.style.visibility = 'hidden';
            tooltip.classList.remove('cursor-pinned');
        }
    }

    plotHorizontalRect(x_start, x_width, y_start, y_height, fill, opacity, tooltipInnerHTML, className = '') {
        const [xmin, xmax] = this.bottomDomain ?? [null, null];
        const [ymin, ymax] = this.leftDomain ?? [null, null];
    
        if (xmin === null || xmax === null || ymin === null || ymax === null) {
            console.error('Domain values (xmin, xmax, ymin, ymax) not set, skipping rectangle plot');
            return;
        }

        if (x_start < xmin) {
            x_width = Math.max(0, x_width - (xmin - x_start));
            x_start = xmin;
        }
        if (x_start + x_width > xmax) {
            x_width = Math.max(0, x_width - (x_start + x_width - xmax));
        }
    
        const x = this.bottomScale(x_start);
        const width = this.bottomScale(x_start + x_width) - this.bottomScale(x_start);
        const y = this.leftScale(y_start);
        const height = y_height;

        if (y === undefined || Number.isNaN(y) || width <= 0 || height <= 0) return;
    
        const rect = this.svg.append('rect')
            .attr('x', x)
            .attr('y', y)
            .attr('width', width)
            .attr('height', height)
            .attr('fill', fill)
            .attr('opacity', opacity)
            .attr('data-original-fill', fill)
            .attr('data-original-opacity', opacity)
            .attr('class', className);
        this._bindPinnableMark(rect, {
            tooltipHTML: () => tooltipInnerHTML,
            highlight: node => d3.select(node).attr('fill', this.highlightColor),
            restore: node => d3.select(node).attr('fill', fill).attr('opacity', opacity),
            label: 'Task bar; click to pin details',
        });
    }

    plotVerticalRect(xStart, xWidth, yHeight, fill = 'steelblue', opacity = 1, tooltipInnerHTML = null, className = '') {
        const [ymin, ymax] = this.leftDomain ?? [null, null];
        if (ymin === null || ymax === null) {
            console.error('Domain values (ymin, ymax) not set, skipping rectangle plot');
            return;
        }
    
        const x = this.bottomScale(xStart);
        const width = xWidth;
    
        let y = this.leftScale(yHeight);
        let height = -(this.leftScale(yHeight) - this.leftScale(ymin));
    
        if (yHeight < ymin || yHeight > ymax) return;
    
        if (yHeight > ymax) {
            height = this.leftScale(ymax) - this.leftScale(ymin);
        } else if (yHeight < ymin) {
            height = this.leftScale(yHeight) - this.leftScale(ymin);
        }

        const rect = this.svg.append('rect')
            .attr('x', x)
            .attr('y', y)
            .attr('width', width)
            .attr('height', height)
            .attr('fill', fill)
            .attr('opacity', opacity)
            .attr('data-original-fill', fill)
            .attr('data-original-opacity', opacity)
            .attr('class', className);
        this._bindPinnableMark(rect, {
            tooltipHTML: () => tooltipInnerHTML,
            highlight: node => d3.select(node).attr('fill', this.highlightColor),
            restore: node => d3.select(node).attr('fill', fill).attr('opacity', opacity),
            label: 'Bar; click to pin details',
        });
    }
    
    /* calculate the margin for each axis */
    calculateAxesMargin() {  
        this.clearSVG();      
        
        const margin = { top: 0, right: 0, bottom: 0, left: 0 };
        const marginProbeGroup = this.svgElement.append('g').attr('class', 'margin-probe');

        this.createAxes();
        const g = marginProbeGroup.append('g');
        if (this.topAxis) {
            g.call(this.topAxis);
            g.selectAll('text').style('font-size', this.tickFontSize);
            g.selectAll('.tick text').each(function () {
                try {
                    const box = this.getBBox();
                    margin.top = Math.max(margin.top, box.height);
                } catch {}
            });
        }
        if (this.rightAxis) {
            g.call(this.rightAxis);
            g.selectAll('text').style('font-size', this.tickFontSize);
            g.selectAll('.tick text').each(function () {
                try {
                    const box = this.getBBox();
                    margin.right = Math.max(margin.right, box.width);
                } catch {}
            });
        }
        if (this.bottomAxis) {
            g.call(this.bottomAxis);
            g.selectAll('text').style('font-size', this.tickFontSize);
            g.selectAll('.tick text').each(function () {
                try {
                    const box = this.getBBox();
                    margin.bottom = Math.max(margin.bottom, box.height);
                } catch {}
            });
        }
        if (this.leftAxis) {
            g.call(this.leftAxis);
            g.selectAll('text').style('font-size', this.tickFontSize);
            g.selectAll('.tick text').each(function () {
                try {
                    const box = this.getBBox();
                    margin.left = Math.max(margin.left, box.width);
                } catch {}
            });
        }

        margin.top += this.padding;
        margin.right += this.padding;
        margin.bottom += this.padding;
        margin.left += this.padding;

        marginProbeGroup.remove();
        this.clearSVG();

        return margin;
    }

    _rasterizeAndDownload(type = 'png', filename = null, quality = 0.95) {
        if (!this.svgNode) {
            console.error('SVG node not found');
            return;
        }
    
        const mimeMap = {
            png: 'image/png',
            jpg: 'image/jpeg',
            jpeg: 'image/jpeg'
        };
    
        const ext = type.toLowerCase();
        const mime = mimeMap[ext];
        if (!mime) {
            console.error(`Unsupported image type: ${type}`);
            return;
        }
    
        if (!filename) {
            filename = this.id.replace(/-/g, '_') + '.' + ext;
        }
    
        const clonedSvg = this.svgNode.cloneNode(true);
        const applyInlineStyles = (el) => {
            const style = el.getAttribute('style');
            if (style) {
                style.split(';').forEach(prop => {
                    const [k, v] = prop.split(':');
                    if (k && v) el.setAttribute(k.trim(), v.trim());
                });
                el.removeAttribute('style');
            }
            Array.from(el.children).forEach(applyInlineStyles);
        };
        applyInlineStyles(clonedSvg);
    
        const bbox = this.svgNode.getBoundingClientRect();
        const width = Math.ceil(bbox.width);
        const height = Math.ceil(bbox.height);
        const dpr = window.devicePixelRatio || 2;
    
        clonedSvg.setAttribute('width', width);
        clonedSvg.setAttribute('height', height);
        clonedSvg.setAttribute('viewBox', `0 0 ${width} ${height}`);
    
        const svgString = new XMLSerializer().serializeToString(clonedSvg);
        const svgBlob = new Blob([svgString], { type: 'image/svg+xml;charset=utf-8' });
        const svgUrl = URL.createObjectURL(svgBlob);
    
        const img = new Image();
        img.onload = () => {
            const canvas = document.createElement('canvas');
            canvas.width = width * dpr;
            canvas.height = height * dpr;
            canvas.style.width = `${width}px`;
            canvas.style.height = `${height}px`;
    
            const ctx = canvas.getContext('2d');
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
            ctx.fillStyle = '#ffffff';
            ctx.fillRect(0, 0, canvas.width, canvas.height);
            ctx.drawImage(img, 0, 0);
            URL.revokeObjectURL(svgUrl);
    
            const dataUrl = mime === 'image/jpeg'
                ? canvas.toDataURL(mime, quality)
                : canvas.toDataURL(mime);
    
            const link = document.createElement('a');
            link.href = dataUrl;
            link.download = filename;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        };
        img.onerror = (e) => {
            console.error('Error loading SVG image', e);
            URL.revokeObjectURL(svgUrl);
        };
        img.src = svgUrl;
    }
    
    downloadPNG(filename = null) {
        this._rasterizeAndDownload('png', filename);
    }
    
    downloadJPG(filename = null, quality = 0.95) {
        this._rasterizeAndDownload('jpg', filename, quality);
    }
    
    downloadJPEG(filename = null, quality = 1.0) {
        this._rasterizeAndDownload('jpeg', filename, quality);
    }    
    
    async downloadPDF() {
        await ensurePdfDependencies();

        if (typeof jsPDF !== 'function') {
            console.error('jsPDF is not available. Unable to export PDF.');
            return;
        }

        if (typeof svg2pdf !== 'function') {
            console.error('svg2pdf is not available. Unable to export PDF.');
            return;
        }

        const svgElement = this.svgNode;

        const width = svgElement.clientWidth;
        const height = svgElement.clientHeight;
        svgElement.setAttribute('width', width);
        svgElement.setAttribute('height', height);
        svgElement.setAttribute('viewBox', `0 0 ${width} ${height}`);
    
        function applyInlineStyles(el) {
            const style = el.getAttribute('style');
            if (style) {
                style.split(';').forEach(prop => {
                    const [k, v] = prop.split(':');
                    if (k && v) el.setAttribute(k.trim(), v.trim());
                });
                el.removeAttribute('style');
            }
            Array.from(el.children).forEach(applyInlineStyles);
        }
        applyInlineStyles(svgElement);
    
        try {
            const doc = new jsPDF({
                orientation: 'landscape',
                unit: 'pt',
                format: [width, height],
            });

            await svg2pdf(svgElement, doc, { x: 0, y: 0, width, height });
            doc.save(`${this.id.replace(/-/g, '_')}.pdf`);
        } catch (error) {
            console.error('Failed to export PDF:', error);
        }
    }

    downloadSVG(filename = null) {
        if (!this.svgElement) {
            console.error('SVG element not found');
            return;
        }
        if (!filename) {
            filename = this.id.replace(/-/g, '_');
            if (filename.endsWith('svg')) {
                filename = filename.substring(0, filename.length - 4);
            }
            filename = filename + '.svg';
        }
    
        function applyInlineStyles(element) {
            const style = element.getAttribute('style');
            if (style) {
                const styleProperties = style.split(';');
                styleProperties.forEach(property => {
                    const [key, value] = property.split(':');
                    if (key && value) {
                        element.setAttribute(key.trim(), value.trim());
                    }
                });
    
                element.removeAttribute('style');
            }
            Array.from(element.children).forEach(applyInlineStyles);
        }
        applyInlineStyles(this.svgNode);
    
        const serializer = new XMLSerializer();
        let svgString = serializer.serializeToString(this.svgNode);
    
        const blob = new Blob([svgString], {type: "image/svg+xml"});
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }

    downloadCSV(filename = null) {
        if (!filename) {
            filename = this.id.replace(/-/g, '_') + '.csv';
        }
    
        const link = document.createElement('a');
        link.href = this._revision
            ? `/api/v1/runs/${encodeURIComponent(this._folder)}/revisions/${encodeURIComponent(this._revision)}/exports/${encodeURIComponent(this.id)}.csv`
            : `${this.api_url}/export-csv?folder=${encodeURIComponent(this._folder)}`;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
    
    _initResizeHandler() {
        if (this._boundResize) {
            window.removeEventListener('resize', this._boundResize);
        }
        this._boundResize = _.debounce(() => this.resizePlot(), 300);
        window.addEventListener('resize', this._boundResize);
    }

    resizePlot() {
        if (!this.data || !this.svgContainer?.isConnected) return;
        this.clearSVG();
        this._setAxesFromFetchedData();
        this._plotAxes();
        this._setupZoomAndScroll();
        this._invokePlot();
    }


    clearPlot() {
        /* clear legend */
        this.clearLegend();

        /* clear toolbox */
        this.clearToolbox();

        /* clear SVG */
        this.clearSVG();
    }

    initLegend() {}

    _filterPoints(points) {
        const [xmin, xmax] = this.bottomDomain ?? [null, null];
        const [ymin, ymax] = this.leftDomain ?? [null, null];
        return points.filter(p => {
            if (!Array.isArray(p) || p.length < 2 || p[1] == null) return false;
            const x = Number(p[0]), y = Number(p[1]);
            return !Number.isNaN(x) && !Number.isNaN(y) &&
                (xmin == null || x >= xmin) && (xmax == null || x <= xmax) &&
                (ymin == null || y >= ymin) && (ymax == null || y <= ymax);
        });
    }

    plotPoints(points, options = {}) {
        const {
            radius = 1.5,
            color = 'steelblue',
            tooltipFormatter = null,
            className = 'data-point'
        } = options;
    
        /** get xmin and xmax */
        const [xmin, xmax] = this.bottomDomain ?? [null, null];
        const [ymin, ymax] = this.leftDomain ?? [null, null];
    
        /** if xmin or xmax are not set, do not plot */
        if (xmin === null || xmax === null || ymin === null || ymax === null) {
            console.error('xmin or xmax not set, unable to plot data');
            return;
        }

        const filteredPoints = this._filterPoints(points);
    
        const circles = this.svg.selectAll(`circle.${className}`)
            .data(filteredPoints)
            .enter()
            .append('circle')
            .attr('class', className)
            .attr('cx', d => this.bottomScale(Number(d[0])))
            .attr('cy', d => this.leftScale(Number(d[1])))
            .attr('r', radius)
            .attr('fill', color)
            .attr('data-original-fill', color)
            .attr('data-original-r', radius);
        if (tooltipFormatter) {
            this._bindPinnableMark(circles, {
                tooltipHTML: tooltipFormatter,
                highlight: node => d3.select(node).attr('fill', this.highlightColor).attr('r', radius * 4),
                restore: node => d3.select(node).attr('fill', color).attr('r', radius),
                label: 'Data point; click to pin details',
            });
        }
    }
    
    _queryAllLegendCheckboxes() {
        return this.legendContainer.selectAll(`input[name="${this.legendCheckboxName}"]`)
    }

    resetPlot() {
        /** 1. clear the existing svg */
        this.clearSVG();

        /** 2. clear the custom params */
        this.clearCustomParams();

        /** 3. exit CDF mode if active */
        this._cdfMode = false;

        /** 4. plot the axes from the fetched data */
        this._setAxesFromFetchedData();
        this._plotAxes();

        /** 5. check all checkboxes */
        this._queryAllLegendCheckboxes().each(function () {
            this.checked = true;
        });

        /** 6. plot the data */
        this._invokePlot();
    }

    _invokePlot() {
        if (this._cdfMode && this._hasScatterPlot) {
            this._plotCdf();
        } else {
            this.plot();
        }
    }

    _getPointsForCdf() {
        if (Array.isArray(this.data?.points)) return this.data.points;
        if (!Array.isArray(this.data?.series)) return [];
        return this.data.series.flatMap(series => Array.isArray(series.points) ? series.points : []);
    }

    _transformToCdfPoints(points, numPercentiles = 100) {
        const valid = points.filter(p => Array.isArray(p) && p.length >= 2 && p[1] != null && !Number.isNaN(Number(p[1])));
        if (valid.length === 0) return [];
        const sorted = [...valid].sort((a, b) => Number(a[1]) - Number(b[1]));
        const n = sorted.length;
        return Array.from({ length: numPercentiles }, (_, i) => {
            const p = (i + 1) / numPercentiles;
            const idx = Math.min(Math.floor(p * n), n - 1);
            return [Number(sorted[idx][1]), p];
        });
    }

    _setAxesForCdf() {
        const pts = this._getPointsForCdf();
        const cdfPts = this._transformToCdfPoints(pts);
        if (cdfPts.length === 0) return;
        const xs = cdfPts.map(p => p[0]);
        const uniqueXs = [...new Set(xs)].sort((a, b) => a - b);
        const xMin = Math.min(...xs);
        const xMax = Math.max(...xs);
        const xDomain = xMin === xMax ? [xMin - 0.5, xMax + 0.5] : [xMin, xMax];
        this.setBottomDomain(xDomain);
        const origYFormatter = this.data?.['y_tick_formatter'] ? eval(this.data['y_tick_formatter']) : (d => String(d));
        const isDuration = this.data?.axes?.y?.unit === 'seconds';
        const durationAxis = isDuration ? durationAxisSpec(xDomain, 5) : null;
        const tickValues = durationAxis?.tickValues || (uniqueXs.length <= 5
            ? uniqueXs
            : this._computeLinearTicks(xDomain, 5));
        this.setBottomTickValues(tickValues);
        this.setBottomFormatter(durationAxis?.formatter || origYFormatter);
        this.setLeftDomain([0, 1]);
        this.setLeftTickValues([0, 0.25, 0.5, 0.75, 1]);
        this.setLeftFormatter(d => (d * 100).toFixed(0) + '%');
    }

    _computeLinearTicks(domain, numTicks = 5) {
        const [lo, hi] = domain;
        if (lo === hi) return [lo];
        const step = (hi - lo) / (numTicks - 1);
        return Array.from({ length: numTicks }, (_, i) => lo + i * step);
    }

    _plotCdf() {
        const pts = this._getPointsForCdf();
        const cdfPts = this._transformToCdfPoints(pts, 100);
        if (cdfPts.length === 0) return;
        const origYFormatter = this.data?.['y_tick_formatter'] ? eval(this.data['y_tick_formatter']) : (d => String(d));
        this.plotPath(cdfPts, {
            stroke: 'steelblue',
            strokeWidth: 1.5,
            className: 'cdf-path',
            curveType: d3.curveLinear,
            disableHover: true,
        });
        this.plotPoints(cdfPts, {
            radius: 2,
            color: 'steelblue',
            className: 'cdf-point',
            tooltipFormatter: d => `Value: ${origYFormatter(d[0])}<br>CDF: ${(d[1] * 100).toFixed(0)}%`,
        });
        this.installLineCursor([{
            id: `${this.id}-cdf`,
            name: 'CDF',
            color: 'steelblue',
            points: cdfPts,
        }], {
            interpolation: 'linear',
            xFormatter: origYFormatter,
            yFormatter: d => `${(d * 100).toFixed(0)}%`,
            xLabel: this.data?.axes?.y?.label || 'Value',
            xUnit: this.data?.axes?.y?.unit,
        });
    }

    _redrawForCdfToggle() {
        this.clearSVG();
        if (this._cdfMode) {
            const pts = this._getPointsForCdf();
            if (pts.length === 0) {
                this._cdfMode = false;
            }
        }
        if (this._cdfMode) {
            this._setAxesForCdf();
        } else {
            this._setAxesFromFetchedData();
        }
        this._plotAxes();
        this._setupZoomAndScroll();
        this._invokePlot();
        this._initToolbox();
    }

    createToolboxItemCdfToggle() {
        const label = this._cdfMode ? 'Undisplay CDF' : 'Display CDF';
        return this.toolbox.createButtonItem(`${this.id}-cdf-toggle`, label, () => {
            this._cdfMode = !this._cdfMode;
            this._redrawForCdfToggle();
        });
    }

    clearCustomParams() {
        this.customParams = {};
    }

    _setAxesFromFetchedData() {
        if (this.bottomScaleType) {
            this.setBottomDomain(this.data['x_domain']);
            this.setBottomTickValues(this.data['x_tick_values']);
            this.setBottomFormatter(eval(this.data['x_tick_formatter']));
            this.setBottomTickValues(this._dedupeTicksByFormatter(this.bottomTickValues, this.bottomFormatter));
        } else if (this.topScaleType) {
            this.setTopDomain(this.data['x_domain']);
            this.setTopTickValues(this.data['x_tick_values']);
            this.setTopFormatter(eval(this.data['x_tick_formatter']));
            this.setTopTickValues(this._dedupeTicksByFormatter(this.topTickValues, this.topFormatter));
        } else {
            /* some modules do not require setting domains */
            return;
        }

        if (this.leftScaleType) {
            this.setLeftDomain(this.data['y_domain']);
            this.setLeftTickValues(this.data['y_tick_values']);
            this.setLeftFormatter(eval(this.data['y_tick_formatter']));
            this.setLeftTickValues(this._dedupeTicksByFormatter(this.leftTickValues, this.leftFormatter));
        } else if (this.rightScaleType) {
            this.setRightDomain(this.data['y_domain']);
            this.setRightTickValues(this.data['y_tick_values']);
            this.setRightFormatter(eval(this.data['y_tick_formatter']));
            this.setRightTickValues(this._dedupeTicksByFormatter(this.rightTickValues, this.rightFormatter));
        } else {
            /* some modules do not require setting domains */
            return;
        }
    }

    _dedupeTicksByFormatter(ticks, formatter) {
        if (!Array.isArray(ticks) || ticks.length <= 1 || typeof formatter !== 'function') {
            return ticks;
        }

        const labels = ticks.map(t => {
            try {
                return String(formatter(t));
            } catch {
                return String(t);
            }
        });

        const uniqueLabels = [];
        const firstIndexByLabel = new Map();
        const lastIndexByLabel = new Map();
        labels.forEach((label, idx) => {
            if (!firstIndexByLabel.has(label)) {
                uniqueLabels.push(label);
                firstIndexByLabel.set(label, idx);
            }
            lastIndexByLabel.set(label, idx);
        });

        if (uniqueLabels.length === labels.length) {
            return ticks;
        }

        const selectedIndices = uniqueLabels.map((label, order) => {
            if (order === uniqueLabels.length - 1) {
                return lastIndexByLabel.get(label);
            }
            return firstIndexByLabel.get(label);
        });

        return selectedIndices.map(i => ticks[i]);
    }

    _getUniqueLegendCheckboxId(id) {
        const uniquePrefix = (this.legendContainer && this.legendContainer.id) ? this.legendContainer.id : (this.id || 'legend');
        return `legend-checkbox-${uniquePrefix}-${id}`;
    }

    createLegendRow(items, options = {}) {
        const {
            singleSelect = false,
            onToggle = () => {},
            columnsPerRow = 6,
            checkboxName = 'legend-checkbox',
        } = options;

        this.legendContainer.html('');
        this.legendCheckboxName = checkboxName;
        this.legendSingleSelect = singleSelect;

        const actualColumns = Math.min(columnsPerRow, items.length);
        for (let i = 0; i < items.length; i += actualColumns) {
            const rowItems = items.slice(i, i + actualColumns);
            const row = this.legendContainer.append('div')
                .attr('class', 'legend-row')
                .style('display', 'grid')
                .style('grid-template-columns', `repeat(${actualColumns}, 1fr)`)
                .style('gap', '0px')
                .style('margin-bottom', '5px')
                .style('width', '100%');

            rowItems.forEach(item => {
                const uniquePrefix = (this.legendContainer && this.legendContainer.id) ? this.legendContainer.id : (this.id || 'legend');
                const safeCheckboxId = `legend-checkbox-${uniquePrefix}-${item.id}`;
                const legendItem = row.append('div')
                    .attr('class', 'legend-item')
                    .style('display', 'flex')
                    .style('align-items', 'center')
                    .style('box-sizing', 'border-box')
                    .style('min-width', 0)
                    .style('padding', '2px 0')
                    .style('cursor', 'pointer')
                    .on('click', function(e) {
                        if (e.target.tagName !== 'INPUT' && e.target.tagName !== 'LABEL') {
                            const checkbox = this.querySelector('input[type="checkbox"]');
                            if (checkbox) {
                                checkbox.checked = !checkbox.checked;
                                checkbox.dispatchEvent(new Event('change', { bubbles: true }));
                            }
                        }
                    });

                legendItem.append('input')
                    .attr('type', 'checkbox')
                    .attr('name', this.legendCheckboxName)
                    .attr('id', item.id)
                    .property('checked', item.checked !== false)    /* default checked unless this field is specified as false */
                    .style('accent-color', item.color || '#666')
                    .on('click', function(e) { e.stopPropagation(); })
                    .on('change', (event) => {
                        const checkbox = event.currentTarget;
                        const visible = checkbox.checked;
                        onToggle(item.id, visible);
                    
                        if (this.legendSingleSelect && visible) {
                            this._queryAllLegendCheckboxes()
                                .each(function () {
                                    if (this !== checkbox) {
                                        this.checked = false;
                                    }
                                });
                        }
                    });   

                legendItem.append('label')
                    .attr('for', item.id)
                    .attr('class', 'legend-label')
                    .text(item.label)
                    .on('click', function(e) { e.stopPropagation(); });
            });
        }
        setTimeout(() => {
            const legendNode = this.legendContainer.node();
            const items = legendNode.querySelectorAll('.legend-item');
            let maxItemWidth = 0;
            items.forEach(item => {
                maxItemWidth = Math.max(maxItemWidth, item.scrollWidth);
            });
            const cellWidth = maxItemWidth + 20;
            const minWidth = actualColumns * cellWidth;
            this.legendContainer.selectAll('.legend-row').style('min-width', `${minWidth}px`);
        }, 0);
    }

    createLegendGroup(groups, options = {}) {
        const {
            checkboxName = 'legend-checkbox',
            onToggle = () => {}
        } = options;

        this.legendCheckboxName = checkboxName;
    
        this.legendContainer.html('');
    
        const legendGroupContainer = this.legendContainer.append('div')
            .attr('class', 'legend-flex-container');

        groups.forEach(group => {
            const groupDiv = legendGroupContainer.append('div')
                .attr('class', 'legend-group');

            if (group.showGroupLabel) {
                groupDiv.append('div')
                    .attr('class', 'legend-group-title-container')
                    .append('div')
                    .attr('class', 'legend-group-title')
                    .text(group.groupLabel);
            }

            group.items.forEach(item => {
                const legendItem = groupDiv.append('div')
                    .attr('class', 'legend-item' + (item.checked ? ' checked' : ''));

                const uniqueId = `${this.id}-${item.id}`;
                legendItem.append('input')
                    .attr('type', 'checkbox')
                    .attr('name', this.legendCheckboxName)
                    .attr('id', uniqueId)
                    .property('checked', item.checked !== false)   /* default checked unless this field is specified as false */
                    .style('accent-color', item.color || '#666')
                    .on('click', function(e) { e.stopPropagation(); })
                    .on('change', (event) => {
                        const checkbox = event.currentTarget;
                        const visible = checkbox.checked;
                        onToggle(item.id, visible);
                    });

                if (item.showLabel) {
                    legendItem.append('label')
                        .attr('for', uniqueId)
                        .attr('class', 'legend-label')
                        .text(item.label)
                        .on('click', function(e) { e.stopPropagation(); });
                }
            });
        });
    }
}
