import { Toolbox } from './toolbox.js';
import { jsPDF } from 'https://cdn.skypack.dev/jspdf';
import { svg2pdf } from 'https://cdn.skypack.dev/svg2pdf.js';


export class BaseModule {
    constructor(id, title, api_url) {
        this.id = id;
        this.title = title;
        this.api_url = api_url;

        this._folder = null;
        this._fetchDataParams = {}; // Parameters for fetchData calls

        this.svgContainer = null;
        this.svgElement = null;
        this.svgNode = null;
        this.legendContainer = null;

        this.toolbox = null;
        this.toolboxContainer = null;
        
        this.tooltip = null;

        this.tickFontSize = 12;
        this.padding = 40;
        this.highlightColor = 'orange';
        this.highlightRadius = 6;
        this.highlightStrokeWidth = 3;

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
    }

    switchFolder(folder) {
        this._folder = folder;

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
        </div>
      
        <div class="section-content">
            <div class="section-row top">
                <div class="legend-container" id="${this.id}-legend"></div>
                <div class="legend-placeholder"></div>
            </div>
      
            <div class="section-row bottom">
                <div class="plotting-container" id="${this.id}-container">
                    <svg id="${this.id}-d3-svg" xmlns="http://www.w3.org/2000/svg"></svg>
                </div>
                <div id="${this.id}-toolbox-container" class="toolbox-container"></div>
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
    }

    async fetchDataAndPlot() {
        this.clearSVG();
        this.plotSpinner();
        await this.fetchData();
        this.plot();
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
        this.plot();
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
        this.plot();
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

        const items = [
            this.createToolboxItemExport(),
        ];
        if (this.bottomScaleType === 'linear') {
            items.push(this.createToolboxItemSetXMin());
            items.push(this.createToolboxItemSetXMax());
        }
        if (this.leftScaleType === 'linear') {
            items.push(this.createToolboxItemSetYMin());
            items.push(this.createToolboxItemSetYMax());
        }
        if (this._queryAllLegendCheckboxes().size() > 0 && !this.legendSingleSelect) {
            items.push(this.createToolboxItemSelectAll());
            items.push(this.createToolboxItemClearAll());
        }
        items.push(this.createToolboxItemRefetch());
        items.push(this.createToolboxItemReset());

        this._setToolboxItems(items);
    }

    clearLegend() {
        if (this.legendContainer && this.legendContainer.node()) {
            this.legendContainer.html('');
        }
        this.legendCheckboxName = null;
    }

    clearSVG() {
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
        
        // Build query parameters starting with folder
        const params = new URLSearchParams({
            folder: this._folder
        });
        
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
        
        const response = await fetch(`${this.api_url}?${params.toString()}`);
        const data = await response.json();
        
        if (!data) {
            console.warn('Invalid or missing data');
            return;
        }

        this.data = data;

        this._setAxesFromFetchedData();

        /* legend and toolbox must be set after data is fetched */
        this._initToolbox();
        this._plotAxes();
        this._setupZoomAndScroll();
        this._initResizeHandler();

        /* legend must be set after axes are plotted */
        this.initLegend();
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
        
        /* do not bind the handler multiple times */
        if (this._zoomHandlerInitialized) return;
        this._zoomHandlerInitialized = true;

        this.svgNode.removeAttribute('width');
        this.svgNode.removeAttribute('height');
    
        this.svgContainer.addEventListener('wheel', (event) => {
            if (event.ctrlKey) {
                event.preventDefault();
    
                const zoomFactor = event.deltaY < 0 ? 1.05 : 0.95;
                let newWidth = this.currentWidth * zoomFactor;
                let newHeight = this.currentHeight * zoomFactor;

                if (zoomFactor < 1 && (this.currentWidth <= this.originalWidth || this.currentHeight <= this.originalHeight)) {
                    return;
                }
                
                if (newWidth <= this.originalWidth || newHeight <= this.originalHeight) {
                    newWidth = this.originalWidth;
                    newHeight = this.originalHeight;
                }                
    
                const rect = this.svgNode.getBoundingClientRect();
                const mouseX = event.clientX - rect.left;
                const mouseY = event.clientY - rect.top;
                const scaleX = mouseX / rect.width;
                const scaleY = mouseY / rect.height;
    
                this.svgNode.style.width = `${Math.round(newWidth)}px`;
                this.svgNode.style.height = `${Math.round(newHeight)}px`;
    
                const newRect = this.svgNode.getBoundingClientRect();
                const targetX = scaleX * newRect.width;
                const targetY = scaleY * newRect.height;
    
                const offsetX = targetX - mouseX;
                const offsetY = targetY - mouseY;
    
                this.svgContainer.scrollLeft += offsetX;
                this.svgContainer.scrollTop += offsetY;
    
                this.currentWidth = newRect.width;
                this.currentHeight = newRect.height;
                  
            }
        });
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
        const [xmin, xmax] = this.bottomDomain ?? [0, Infinity];
        const [ymin, ymax] = this.leftDomain ?? [0, Infinity];

        const filteredPoints = this._filterPoints(points);

        const {
            stroke = '#2077B4',
            strokeWidth = 1.5,
            className = 'data-path',
            id = '',
            curveType = d3.curveStepAfter,
            tooltipInnerHTML = null,
            tooltipFormatter = null
        } = options;

        const tooltip = document.getElementById('vine-tooltip');

        const line = d3.line()
            .x(d => this.bottomScale(d[0]))
            .y(d => this.leftScale(d[1]))
            .defined(d => {
                const x = d[0];
                const y = d[1];
                return x >= xmin && x <= xmax && y >= ymin && y <= ymax;
            })
            .curve(curveType);

        this.svg.append('path')
            .datum(filteredPoints)
            .attr('fill', 'none')
            .attr('stroke', stroke)
            .attr('stroke-width', strokeWidth)
            .attr('d', line)
            .attr('original-stroke', stroke)
            .attr('original-stroke-width', strokeWidth)
            .attr('class', `${className} ${className}-${id}`)
            .attr('id', id)
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
                        sel.attr('stroke', sel.attr('original-stroke'))
                           .attr('stroke-width', sel.attr('original-stroke-width'));
                    });
                tooltip.style.visibility = 'hidden';
            });
    }

    plotHorizontalRect(x_start, x_width, y_start, y_height, fill, opacity, tooltipInnerHTML, className = '') {
        const tooltip = document.getElementById('vine-tooltip');
    
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
    
        this.svg.append('rect')
            .attr('x', x)
            .attr('y', y)
            .attr('width', width)
            .attr('height', height)
            .attr('fill', fill)
            .attr('opacity', opacity)
            .attr('class', className)
            .on('mouseover', (event) => {
                d3.select(event.currentTarget)
                    .attr('fill', this.highlightColor);
                tooltip.innerHTML = tooltipInnerHTML;
                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mouseout', (event) => {
                d3.select(event.currentTarget)
                    .attr('fill', fill)
                    .attr('opacity', opacity);
                tooltip.style.visibility = 'hidden';
            });
    }

    plotVerticalRect(xStart, xWidth, yHeight, fill = 'steelblue', opacity = 1, tooltipInnerHTML = null, className = '') {
        const tooltip = document.getElementById('vine-tooltip');
    
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

        this.svg.append('rect')
            .attr('x', x)
            .attr('y', y)
            .attr('width', width)
            .attr('height', height)
            .attr('fill', fill)
            .attr('opacity', opacity)
            .attr('class', className)
            .on('mouseover', (event) => {
                d3.select(event.currentTarget)
                    .attr('fill', this.highlightColor);
                tooltip.innerHTML = tooltipInnerHTML;
                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mouseout', (event) => {
                d3.select(event.currentTarget)
                    .attr('fill', fill)
                    .attr('opacity', opacity);
                tooltip.style.visibility = 'hidden';
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
    
    downloadPDF() {
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
    
        const doc = new jsPDF({
            orientation: 'landscape',
            unit: 'pt',
            format: [width, height],
        });
    
        svg2pdf(svgElement, doc, { x: 0, y: 0, width, height }).then(() => {
            doc.save(`${this.id.replace(/-/g, '_')}.pdf`);
        });
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
        link.href = `${this.api_url}/export-csv`;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
    
    _initResizeHandler() {
        if (this._boundResize) {
            window.removeEventListener('resize', this._boundResize);
        }
        this._boundResize = _.debounce(() => this.plot(), 300);
        window.addEventListener('resize', this._boundResize);
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

        return points.filter(p =>
            Array.isArray(p) &&
            p.length >= 2 &&
            typeof p[0] === 'number' &&
            typeof p[1] === 'number' &&
            !Number.isNaN(p[0]) &&
            !Number.isNaN(p[1]) &&
            p[1] !== null &&
            p[0] >= xmin && p[0] <= xmax &&
            p[1] >= ymin && p[1] <= ymax
        )
    }

    plotPoints(points, options = {}) {
        const {
            radius = 1.5,
            color = 'steelblue',
            tooltipFormatter = null,
            className = 'data-point'
        } = options;
    
        const tooltip = document.getElementById('vine-tooltip');
    
        /** get xmin and xmax */
        const [xmin, xmax] = this.bottomDomain ?? [null, null];
        const [ymin, ymax] = this.leftDomain ?? [null, null];
    
        /** if xmin or xmax are not set, do not plot */
        if (xmin === null || xmax === null || ymin === null || ymax === null) {
            console.error('xmin or xmax not set, unable to plot data');
            return;
        }

        const filteredPoints = this._filterPoints(points);
    
        this.svg.selectAll(`circle.${className}`)
            .data(filteredPoints)
            .enter()
            .append('circle')
            .attr('class', className)
            .attr('cx', d => this.bottomScale(d[0]))
            .attr('cy', d => this.leftScale(d[1]))
            .attr('r', radius)
            .attr('fill', color)
            .on('mouseover', (event, d) => {
                d3.select(event.currentTarget)
                    .attr('fill', this.highlightColor)
                    .attr('r', radius * 4);
    
                if (tooltipFormatter) {
                    tooltip.innerHTML = tooltipFormatter(d);
                    tooltip.style.visibility = 'visible';
                    tooltip.style.top = (event.pageY + 10) + 'px';
                    tooltip.style.left = (event.pageX + 10) + 'px';
                }
            })
            .on('mousemove', (event) => {
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mouseout', (event) => {
                d3.select(event.currentTarget)
                    .attr('fill', color)
                    .attr('r', radius);
    
                tooltip.style.visibility = 'hidden';
            });
    }
    
    _queryAllLegendCheckboxes() {
        return this.legendContainer.selectAll(`input[name="${this.legendCheckboxName}"]`)
    }

    resetPlot() {
        /** 1. clear the existing svg */
        this.clearSVG();

        /** 2. plot the axes from the fetched data */
        this._setAxesFromFetchedData();
        this._plotAxes();

        /** 3. check all checkboxes */
        this._queryAllLegendCheckboxes().each(function () {
            this.checked = true;
        });

        /** 4. plot the data */
        this.plot();
    }

    _setAxesFromFetchedData() {
        if (this.bottomScaleType) {
            this.setBottomDomain(this.data['x_domain']);
            this.setBottomTickValues(this.data['x_tick_values']);
            this.setBottomFormatter(eval(this.data['x_tick_formatter']));
        } else if (this.topScaleType) {
            this.setTopDomain(this.data['x_domain']);
            this.setTopTickValues(this.data['x_tick_values']);
            this.setTopFormatter(eval(this.data['x_tick_formatter']));
        } else {
            /* some modules do not require setting domains */
            return;
        }

        if (this.leftScaleType) {
            this.setLeftDomain(this.data['y_domain']);
            this.setLeftTickValues(this.data['y_tick_values']);
            this.setLeftFormatter(eval(this.data['y_tick_formatter']));
        } else if (this.rightScaleType) {
            this.setRightDomain(this.data['y_domain']);
            this.setRightTickValues(this.data['y_tick_values']);
            this.setRightFormatter(eval(this.data['y_tick_formatter']));
        } else {
            /* some modules do not require setting domains */
            return;
        }
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

                legendItem.append('input')
                    .attr('type', 'checkbox')
                    .attr('name', this.legendCheckboxName)
                    .attr('id', item.id)
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
                        .attr('for', item.id)
                        .attr('class', 'legend-label')
                        .text(item.label)
                        .on('click', function(e) { e.stopPropagation(); });
                }
            });
        });
    }
}
