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
        this.svgElement.selectAll('*').remove();
    }
    
    fetchData() {}

    plot() {}

    setupZoomAndScroll() {
        // store the initial width and height of the SVG.
        let initialWidth = this.svgNode.getBoundingClientRect().width;
        let initialHeight = this.svgNode.getBoundingClientRect().height;
    
        // define the maximum and minimum zoom scales.
        const maxWidth = initialWidth * 64;
        const maxHeight = initialHeight * 64; 
        const minWidth = initialWidth * 0.95;
        const minHeight = initialHeight * 0.95;
    
        this.svgContainer.addEventListener('wheel', function(event) {
            if (event.ctrlKey) { // check if the ctrl key is pressed during scroll.
                event.preventDefault(); // prevent the default scroll behavior.
    
                const zoomFactor = event.deltaY < 0 ? 1.1 : 0.9; // determine the zoom direction.
                let newWidth = initialWidth * zoomFactor; // calculate the new width based on the zoom factor.
                let newHeight = initialHeight * zoomFactor; // calculate the new height based on the zoom factor.
    
                // check if the new dimensions exceed the zoom limits.
                if ((newWidth >= maxWidth && zoomFactor > 1) || (newWidth <= minWidth && zoomFactor < 1) ||
                    (newHeight >= maxHeight && zoomFactor > 1) || (newHeight <= minHeight && zoomFactor < 1)) {
                    return; // if the new dimensions are outside the limits, exit the function.
                }
    
                // calculate the mouse position relative to the SVG content before scaling.
                const rect = this.svgElement.getBoundingClientRect(); // get the current size and position of the SVG.
                const mouseX = event.clientX - rect.left; // mouse X position within the SVG.
                const mouseY = event.clientY - rect.top; // mouse Y position within the SVG.
    
                // determine the mouse position as a fraction of the SVG's width and height.
                const scaleX = mouseX / rect.width; 
                const scaleY = mouseY / rect.height; 
    
                // apply the new dimensions to the SVG element.
                this.svgElement.style.width = `${newWidth}px`;
                this.svgElement.style.height = `${newHeight}px`;
    
                // after scaling, calculate where the mouse position would be relative to the new size.
                const newRect = this.svgElement.getBoundingClientRect(); // get the new size and position of the SVG.
                const targetX = scaleX * newRect.width; 
                const targetY = scaleY * newRect.height; 
    
                // calculate the scroll offsets needed to keep the mouse-over point visually static.
                const offsetX = targetX - mouseX; 
                const offsetY = targetY - mouseY; 
    
                // adjust the scroll position of the container to compensate for the scaling.
                this.svgContainer.scrollLeft += offsetX;
                this.svgContainer.scrollTop += offsetY;
    
                // update the initial dimensions for the next scaling operation.
                initialWidth = newWidth;
                initialHeight = newHeight;
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

        /* setup zoom and scroll */
        this.svgElement.style('width', '100%');
        this.svgElement.style('height', '100%');
        this.setupZoomAndScroll();

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

    onResize() {}

    initResetButton() {
        this.svgNode.setAttribute('width', '100%');
        this.svgNode.setAttribute('height', '100%');
    
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
