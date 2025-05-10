export class BaseModule {
    constructor(id, title, api_url) {
        this.id = id;
        this.title = title;
        this.api_url = api_url;

        this.svgContainer = null;
        this.svgElement = null;
        this.svgNode = null;
        this.loadingSpinner = null;
        this.buttonsContainer = null;
        this.legendContainer = null;
        this.resetButton = null;
        this.downloadButton = null;
        
        this.tooltip = null;

        this.tickFontSize = 12;
        this.padding = 40;
        this.highlightColor = 'orange';

        /* plotting parameters */
        this.margin = null;

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

            <div class="section-legend" id="${this.id}-legend"></div>
            <div class="section-controls" id="${this.id}-controls"></div>

            <div class="section-content">
                <div class="container-alpha" id="${this.id}-container">
                    <div class="loading-spinner" id="${this.id}-loading"></div>
                    <svg id="${this.id}-d3-svg" xmlns="http://www.w3.org/2000/svg"></svg>
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
        this.loadingSpinner = document.getElementById(`${this.id}-loading`);
        if (!this.loadingSpinner) {
            console.error(`Loading spinner not found for ${this.id}`);
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
        this.legendContainer = document.getElementById(`${this.id}-legend`);
        if (!this.legendContainer) {
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
    }

    clearSVG() {
        while (this.svgNode.firstChild) {
            this.svgNode.removeChild(this.svgNode.firstChild);
        }
    
        this.svgNode.removeAttribute('width');
        this.svgNode.removeAttribute('height');
    
        this.svgNode.style.width = '';
        this.svgNode.style.height = '';
    }
    
    fetchData() {}

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

    getBandWidth(scale) {
        if (!scale) {
            console.error('Invalid scale type for getBandWidth');
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
        const svg = this.svgElement
            .attr('viewBox', `0 0 ${this.svgContainer.clientWidth} ${this.svgContainer.clientHeight}`)
            .attr('preserveAspectRatio', 'xMidYMid meet')
            .append('g')
            .attr('transform', `translate(${margin.left}, ${margin.top})`);
        
        /* plot the axes */
        if (this.topScaleType) {
            svg.append('g')
                .attr('transform', `translate(0, 0)`)
                .call(this.topAxis)
                .selectAll('text')
                .style('font-size', this.tickFontSize);
        }
        if (this.rightScaleType) {
            svg.append('g')
                .attr('transform', `translate(${svgWidth}, 0)`)
                .call(this.rightAxis)
                .selectAll('text')
                .style('font-size', this.tickFontSize);
        }
        if (this.bottomScaleType) {
            svg.append('g')
                .attr('transform', `translate(0, ${svgHeight})`)
                .call(this.bottomAxis)
                .selectAll('text')
                .style('font-size', this.tickFontSize);
        }
        if (this.leftScaleType) {
            svg.append('g')
                .attr('transform', `translate(0, 0)`)
                .call(this.leftAxis)
                .selectAll('text')
                .style('font-size', this.tickFontSize);
        }

        return svg;
    }

    plotRect(svg, x, y, width, height, fill, opacity, innerHTML) {
        const tooltip = document.getElementById('vine-tooltip');
    
        svg.append('rect')
            .attr('x', x)
            .attr('y', y)
            .attr('width', width)
            .attr('height', height)
            .attr('fill', fill)
            .attr('opacity', opacity)
            .on('mouseover', (event) => {
                d3.select(event.currentTarget)
                    .attr('fill', this.highlightColor);
                tooltip.innerHTML = innerHTML;
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

    initResetButton() {
        if (this._boundPlot) {
            this.resetButton.removeEventListener('click', this._boundPlot);
        }
    
        this._boundPlot = () => this.plot();
        this.resetButton.addEventListener('click', this._boundPlot);
    }

    initDownloadButton() {
        if (this._boundDownload) {
            this.downloadButton.removeEventListener('click', this._boundDownload);
        }

        this._boundDownload = () => this.downloadSVG();
        this.downloadButton.addEventListener('click', this._boundDownload);
    }

    initLegend() {}
}
