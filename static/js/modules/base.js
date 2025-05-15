import { Toolbox, createToolbox } from './toolbox.js';


export class BaseModule {
    constructor(id, title, api_url) {
        this.id = id;
        this.title = title;
        this.api_url = api_url;

        this.svgContainer = null;
        this.svgElement = null;
        this.svgNode = null;
        this.buttonsContainer = null;
        this.legendContainer = null;
        this.resetButton = null;
        this.downloadButton = null;

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

    renderSkeleton() {
        const section = document.createElement('div');
        section.className = 'section';
        section.id = this.id;

        section.innerHTML = `
        <div class="section-header" id="${this.id}-header">
            <h2 class="section-title">${this.title}</h2>
            <div class="section-buttons" id="${this.id}-buttons">
                <button id="${this.id}-reset-button" class="report-button">Reset</button>
                <button id="${this.id}-download-button" class="report-button">Download</button>
            </div>
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
        this.buttonsContainer = document.getElementById(`${this.id}-buttons`);
        if (!this.buttonsContainer) {
            console.error(`Buttons container not found for ${this.id}`);
            return;
        }
        this.legendContainer = d3.select(document.getElementById(`${this.id}-legend`));
        if (!this.legendContainer.node()) {
            console.error(`Legend container not found for ${this.id}`);
            return;
        }
        this.resetButton = document.getElementById(`${this.id}-reset-button`);
        if (!this.resetButton) {
            console.error(`Reset button not found for ${this.id}`);
            return;
        }
        this.downloadButton = document.getElementById(`${this.id}-download-button`);
        if (!this.downloadButton) {
            console.error(`Download button not found for ${this.id}`);
            return;
        }
        this.toolboxContainer = document.getElementById(`${this.id}-toolbox-container`);
        if (!this.toolboxContainer) {
            console.error(`Toolbox container not found for ${this.id}`);
            return;
        }

        // temp hack
        this.initToolbox();
    }

    initToolbox() {
        this.toolbox = createToolbox(this.id);
        this.toolbox.mount(this.toolboxContainer);
    }

    clearSVG() {
        while (this.svgNode.firstChild) {
            this.svgNode.removeChild(this.svgNode.firstChild);
        }
    
        this.svgNode.removeAttribute('width');
        this.svgNode.removeAttribute('height');
    
        this.svgNode.style.width = '';
        this.svgNode.style.height = '';

        this.svg = null;
    }
    
    async fetchData(folder) {
        this.clearSVG();

        const response = await fetch(
            `${this.api_url}?` +
            `folder=${folder}`
        );
        const data = await response.json();
        
        if (!data) {
            console.warn('Invalid or missing data');
            return;
        }

        this.data = data;

        /* set domains and tick values */
        if (this.bottomScaleType) {
            this.setBottomDomain(data['x_domain']);
            this.setBottomTickValues(data['x_tick_values']);
            this.setBottomFormatter(eval(data['x_tick_formatter']));
        } else if (this.topScaleType) {
            this.setTopDomain(data['x_domain']);
            this.setTopTickValues(data['x_tick_values']);
            this.setTopFormatter(eval(data['x_tick_formatter']));
        } else {
            console.error('Invalid scale type for fetchData');
        }
        if (this.leftScaleType) {
            this.setLeftDomain(data['y_domain']);
            this.setLeftTickValues(data['y_tick_values']);
            this.setLeftFormatter(eval(data['y_tick_formatter']));
        } else if (this.rightScaleType) {
            this.setRightDomain(data['y_domain']);
            this.setRightTickValues(data['y_tick_values']);
            this.setRightFormatter(eval(data['y_tick_formatter']));
        } else {
            console.error('Invalid scale type for fetchData');
        }
    }

    plot() {}

    initZoomTrackingAfterRender() {
        const scrollbarAllowance = 4;
        this.originalWidth = Math.floor(this.svgContainer.clientWidth - scrollbarAllowance);
        this.originalHeight = Math.floor(this.svgContainer.clientHeight - scrollbarAllowance);
    
        this.svgNode.style.width = `${this.originalWidth}px`;
        this.svgNode.style.height = `${this.originalHeight}px`;
    
        this.currentWidth = this.originalWidth;
        this.currentHeight = this.originalHeight;
    }
    
    setupZoomAndScroll() {
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
        } else if (scaleType === 'point') {
            return d3.scalePoint();
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
    plotAxes() {
        this.clearSVG();

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
        points = points.filter(p => Array.isArray(p) && p.length >= 2 && !isNaN(p[0]) && !isNaN(p[1]) && p[0] >= 0 && p[1] >= 0);
        const {
            stroke = '#2077B4',
            strokeWidth = 1.5,
            className = 'data-path',
            id = '',
            tooltipInnerHTML = null,
            tooltipFormatter = null
        } = options;

        const tooltip = document.getElementById('vine-tooltip');

        const line = d3.line()
            .x(d => this.bottomScale(d[0]))
            .y(d => this.leftScale(d[1]))
            .defined(d => !isNaN(d[0]) && !isNaN(d[1]) && d[1] >= 0)
            .curve(d3.curveStepAfter);

        this.svg.append('path')
            .datum(points)
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

    plotRect(x, y, width, height, fill, opacity, tooltipInnerHTML) {
        const tooltip = document.getElementById('vine-tooltip');
    
        this.svg.append('rect')
            .attr('x', x)
            .attr('y', y)
            .attr('width', width)
            .attr('height', height)
            .attr('fill', fill)
            .attr('opacity', opacity)
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

    initResizeHandler() {
        if (this._boundResize) {
            window.removeEventListener('resize', this._boundResize);
        }
        this._boundResize = _.debounce(() => this.plot(), 300);
        window.addEventListener('resize', this._boundResize);
    }

    resetLegend() {
        if (this.legendContainer.node()) {
            /* handle both createLegendRow and createLegendGroup created legends */
            const checkboxes = this.legendContainer.selectAll('input[type="checkbox"]');
            checkboxes.each(function() {
                this.checked = true;
                /* add checked class to parent legend-item for createLegendGroup */
                const legendItem = this.closest('.legend-item');
                if (legendItem) {
                    legendItem.classList.add('checked');
                }
                /* trigger change event */
                this.dispatchEvent(new Event('change', { bubbles: true }));
            });
        }
    }

    initResetButton() {
        if (this._boundPlot) {
            this.resetButton.removeEventListener('click', this._boundPlot);
        }
    
        this._boundPlot = () => {
            this.resetLegend();
            this.plot();
        }
            
        this.resetButton.addEventListener('click', this._boundPlot);
    }

    initDownloadButton() {
        if (this._boundDownload) {
            this.downloadButton.removeEventListener('click', this._boundDownload);
        }

        this._boundDownload = () => this.downloadSVG();
        this.downloadButton.addEventListener('click', this._boundDownload);
    }

    initSVG() {
        if (!this.data) {
            return;
        }

        this.clearSVG();

        this.plotAxes();

        this.initZoomTrackingAfterRender();

        this.setupZoomAndScroll();
    }

    initLegend() {}

    plotPoints(points, options = {}) {
        const {
            radius = 1.5,
            color = 'steelblue',
            tooltipFormatter = null,
            className = 'data-point'
        } = options;

        const tooltip = document.getElementById('vine-tooltip');

        this.svg.selectAll(`circle.${className}`)
            .data(points)
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

    reset() {
        this.clearSVG();
        this.legendContainer.html('');
    }

    createLegendRow(items, options = {}) {
        const {
            singleSelect = false,
            lineWidth = 4,
            onToggle = () => {},
            columnsPerRow = 6,
            checkboxName = 'legend-checkbox',
        } = options;

        this.legendContainer.html('');
        this.legendCheckboxName = checkboxName;

        const buttonGroup = this.legendContainer
            .append('div')
            .attr('class', 'legend-button-group')
            .style('display', 'flex')
            .style('align-items', 'center')
            .style('gap', '12px')
            .style('margin-bottom', '8px')
            .style('justify-content', 'flex-start');

        function addButton(row, text, onClick) {
            const btn = row.append('button')
                .attr('class', 'report-button')
                .attr('type', 'button')
                .text(text)
                .on('click', onClick);
            return btn;
        }

        addButton(buttonGroup, 'Select All', () => {
            this._queryAllLegendCheckboxes()
                .property('checked', true)
                .each(function() {
                    const id = this.getAttribute('data-id');
                    onToggle(id, true);
                });
        });
        addButton(buttonGroup, 'Clear All', () => {
            this._queryAllLegendCheckboxes()
                .property('checked', false)
                .each(function() {
                    const id = this.getAttribute('data-id');
                    onToggle(id, false);
                });
        });

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
                    .attr('data-id', item.id)
                    .attr('id', safeCheckboxId)
                    .property('checked', item.checked !== false)    /* default checked unless this field is specified as false */
                    .style('margin-right', '5px')
                    .style('flex-shrink', 0)
                    .on('click', function(e) { e.stopPropagation(); })
                    .on('change', (event) => {
                        const checkbox = event.currentTarget;
                        const visible = checkbox.checked;
                        onToggle(item.id, visible);
                    
                        if (singleSelect && visible) {
                            this._queryAllLegendCheckboxes()
                                .each(function () {
                                    if (this !== checkbox) {
                                        this.checked = false;
                                    }
                                });
                        }
                    });                                      

                /* color line is optional */
                if (item.color && item.color.trim() !== '') {
                    legendItem.append('div')
                        .style('height', `${lineWidth}px`)
                        .style('background-color', item.color);
                }

                legendItem.append('label')
                    .attr('for', safeCheckboxId)
                    .attr('class', 'legend-label')
                    .text(item.label)
                    .style('font-size', '12px')
                    .style('flex', '1')
                    .style('padding-right', '2px')
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
    
        const flexContainer = this.legendContainer.append('div')
            .attr('class', 'legend-flex-container');
    
        groups.forEach(group => {
            const groupDiv = flexContainer.append('div')
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
    
                const checkbox = legendItem.append('input')
                    .attr('type', 'checkbox')
                    .attr('id', `${item.id}-checkbox`)
                    .attr('name', this.legendCheckboxName)
                    .property('checked', item.checked)
                    .style('display', 'none')
                    .node();
    
                legendItem.append('div')
                    .attr('class', 'legend-color')
                    .style('--color', item.color);
    
                if (item.showLabel) {
                    legendItem.append('span')
                        .text(item.label);
                }
    
                legendItem.on('click', () => {
                    checkbox.checked = !checkbox.checked;
                    legendItem.classed('checked', checkbox.checked);
                    onToggle(item.id, checkbox.checked);
                });
            });
        });
    }
}
