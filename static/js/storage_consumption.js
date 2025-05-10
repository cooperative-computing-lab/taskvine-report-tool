import { downloadSVG } from './tools.js';
import { setupZoomAndScroll } from './tools.js';

const buttonReset = document.getElementById('button-reset-storage-consumption');
const buttonDownload = document.getElementById('button-download-storage-consumption');
const svgContainer = document.getElementById('storage-consumption-container');
const svgElement = d3.select('#storage-consumption');
const tooltip = document.getElementById('vine-tooltip');
const loadingSpinner = document.getElementById('storage-consumption-loading');

const HIGHLIGHT_COLOR = 'orange';

const state = {
    worker_storage_consumption: null,
    file_size_unit: null,
    xMin: null,
    xMax: null,
    yMin: null,
    yMax: null,
    xTickValues: null,
    yTickValues: null,
    tickFontSize: null,
    showPercentage: false,
    worker_resources: {},
    showPBBWorkers: false
}

async function initialize(detail) {
    try {
        // init dom elements
        buttonReset = document.getElementById('button-reset-storage-consumption');
        buttonDownload = document.getElementById('button-download-storage-consumption');
        svgContainer = document.getElementById('storage-consumption-container');
        svgElement = d3.select('#storage-consumption');
        loadingSpinner = document.getElementById('storage-consumption-loading');
        
        // show loading spinner
        loadingSpinner.style.display = 'block';
        
        // clear previous content
        svgElement.selectAll('*').remove();

        // setup event listeners
        setupEventListeners();

        const response = await fetch('/api/storage-consumption');
        const data = await response.json();

        if (data) {
            state.storageData = data.storage_data;
            state.tickValues = {
                storageX: data.storage_x_tick_values,
                storageY: data.storage_y_tick_values
            };
            state.tickFontSize = data.tickFontSize;

            document.querySelector('#storage-consumption').style.width = '100%';
            document.querySelector('#storage-consumption').style.height = '100%';
            plotStorageConsumption();
            setupZoomAndScroll('#storage-consumption', '#storage-consumption-container');
        }
    } catch (error) {
        console.error('Error:', error);
    } finally {
        // hide loading spinner
        loadingSpinner.style.display = 'none';
        if (detail && detail.hideSpinner) {
            detail.hideSpinner('storage-consumption');
        }
    }
}

function calculateMargin() {
    if (!state.worker_storage_consumption) {
        return { top: 40, right: 30, bottom: 40, left: 30 };
    }

    const margin = { top: 40, right: 30, bottom: 40, left: 30 };

    const tempSvg = svgElement
        .append('g')
        .attr('class', 'temp');

    const tempYScale = d3.scaleLinear()
        .domain([state.yMin, state.yMax]);

    const tempYAxis = d3.axisLeft(tempYScale)
        .tickValues(state.yTickValues)
        .tickFormat(d => `${d.toFixed(2)} ${state.file_size_unit}`);

    tempSvg.call(tempYAxis);
    tempSvg.selectAll('text').style('font-size', state.tickFontSize);
    
    const maxYLabelWidth = d3.max(tempSvg.selectAll('.tick text').nodes(), 
        d => d.getBBox().width);
    tempSvg.remove();

    margin.left = Math.ceil(maxYLabelWidth + 20);

    return margin;
}

function plotStorageConsumption() {
    if (!state.worker_storage_consumption) return;

    svgElement.selectAll('*').remove();

    const margin = calculateMargin();
    const width = Math.max(100, svgContainer.clientWidth - margin.left - margin.right);
    const height = Math.max(100, svgContainer.clientHeight - margin.top - margin.bottom);

    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    // ensure valid domains
    const xMin = Math.max(0, state.xMin || 0);
    const xMax = Math.max(xMin + 1, state.xMax || 1);
    const yMin = Math.max(0, state.yMin || 0);
    const yMax = Math.max(yMin + 1, state.yMax || 1);

    const xScale = d3.scaleLinear()
        .domain([xMin, xMax])
        .range([0, width]);

    const yScale = d3.scaleLinear()
        .domain([yMin, yMax])
        .range([height, 0]);

    // create line generator with step curve
    const line = d3.line()
        .x(d => {
            const x = xScale(d[0]);
            return isNaN(x) ? 0 : x;
        })
        .y(d => {
            const y = yScale(d[1]);
            return isNaN(y) ? height : y;
        })
        .defined(d => !isNaN(d[0]) && !isNaN(d[1]) && d[1] >= 0)  // skip invalid points
        .curve(d3.curveStepAfter);

    // draw lines for each worker
    Object.entries(state.worker_storage_consumption).forEach(([workerId, points], index) => {
        // filter out invalid points
        const validPoints = points.filter(p => 
            !isNaN(p[0]) && !isNaN(p[1]) && 
            p[0] >= xMin && p[0] <= xMax && 
            p[1] >= yMin && p[1] <= yMax
        );

        if (validPoints.length === 0) return;  // skip if no valid points

        const color = d3.schemeCategory10[index % 10];
        const safeWorkerId = workerId.replace(/[.:]/g, '\\$&'); // escape special characters
        const workerResource = state.worker_resources[workerId] || {};
        const isPBBWorker = workerResource.is_pbb || false;
        
        svg.append('path')
            .datum(validPoints)
            .attr('fill', 'none')
            .attr('stroke', color)
            .attr('stroke-width', 0.8)
            .attr('class', `worker-line worker-${safeWorkerId} ${isPBBWorker ? 'pbb-worker' : ''}`)
            .attr('d', line)
            .on('mouseover', function(e) {
                d3.select(this)
                    .attr('stroke', HIGHLIGHT_COLOR)
                    .attr('stroke-width', 3)
                    .raise();
                
                svg.selectAll('.worker-line')
                    .filter(function() {
                        return !this.classList.contains(`worker-${safeWorkerId}`);
                    })
                    .attr('stroke', '#ddd')
                    .attr('stroke-width', 0.8);
                
                let lastNonZeroIndex = points.length - 1;
                while (lastNonZeroIndex >= 0 && points[lastNonZeroIndex][1] === 0) {
                    lastNonZeroIndex--;
                }
                const currentValue = (lastNonZeroIndex >= 0 ? points[lastNonZeroIndex][1] : 0).toFixed(2);
                tooltip.style.visibility = 'visible';
                tooltip.innerHTML = `
                    Worker: ${workerId}${isPBBWorker ? ' (PBB)' : ''}<br>
                    Current Usage: ${currentValue} ${state.file_size_unit}<br>
                    Cores: ${workerResource.cores || 'N/A'}<br>
                    Memory: ${workerResource.memory_mb ? formatSize(workerResource.memory_mb, 'MB') : 'N/A'}<br>
                    Disk: ${workerResource.disk_mb ? formatSize(workerResource.disk_mb, 'MB') : 'N/A'}<br>
                    ${workerResource.gpus ? `GPUs: ${workerResource.gpus}<br>` : ''}
                `;
                tooltip.style.top = (e.pageY - 15) + 'px';
                tooltip.style.left = (e.pageX + 10) + 'px';
            })
            .on('mouseout', function() {
                d3.select(this)
                    .attr('stroke', color)
                    .attr('stroke-width', 0.8);

                svg.selectAll('.worker-line')
                    .attr('stroke', (d, i) => d3.schemeCategory10[i % 10])
                    .attr('stroke-width', 0.8);

                tooltip.style.visibility = 'hidden';
            });
    });

    // add axes with validated tick values
    const xTickValues = state.xTickValues.filter(v => !isNaN(v) && v >= xMin && v <= xMax);
    const yTickValues = state.yTickValues.filter(v => !isNaN(v) && v >= yMin && v <= yMax);

    svg.append('g')
        .attr('transform', `translate(0, ${height})`)
        .call(d3.axisBottom(xScale)
            .tickValues(xTickValues)
            .tickFormat(d => `${d.toFixed(2)}s`)
            .tickSizeOuter(0))
        .style('font-size', `${state.tickFontSize}px`);

    svg.append('g')
        .call(d3.axisLeft(yScale)
            .tickValues(yTickValues)
            .tickFormat(d => `${d.toFixed(2)} ${state.file_size_unit}`)
            .tickSizeOuter(0))
        .style('font-size', `${state.tickFontSize}px`);
}

function handleResetClick() {
    document.querySelector('#storage-consumption').style.width = '100%';
    document.querySelector('#storage-consumption').style.height = '100%';
    plotStorageConsumption();
}

// helper function for formatting sizes
function formatSize(size, unit) {
    if (size >= 1024 && unit === 'MB') {
        return `${(size/1024).toFixed(2)} GB`;
    }
    return `${size.toFixed(2)} ${unit}`;
}

// initialize when data is loaded
window.document.addEventListener('dataLoaded', (event) => initialize(event.detail));
window.addEventListener('resize', _.debounce(() => plotStorageConsumption(), 300));