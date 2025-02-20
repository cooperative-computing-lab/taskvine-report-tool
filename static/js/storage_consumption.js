import { downloadSVG } from './tools.js';
import { setupZoomAndScroll } from './tools.js';

const buttonReset = document.getElementById('button-reset-storage-consumption');
const buttonDownload = document.getElementById('button-download-storage-consumption');
const svgContainer = document.getElementById('storage-consumption-container');
const svgElement = d3.select('#storage-consumption');
const tooltip = document.getElementById('vine-tooltip');

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
}

async function initialize() {
    try {
        d3.select('#storage-consumption').selectAll('*').remove();
        state.worker_storage_consumption = null;

        let retries = 0;
        const maxRetries = 5;
        const retryDelay = 2000;

        while (retries < maxRetries) {
            try {
                const response = await fetch(`/api/storage-consumption`);
                const data = await response.json();

                if (data && data.worker_storage_consumption) {
                    state.worker_storage_consumption = data.worker_storage_consumption;
                    state.file_size_unit = data.file_size_unit;
                    state.xMin = data.xMin;
                    state.xMax = data.xMax;
                    state.yMin = data.yMin;
                    state.yMax = data.yMax;
                    state.xTickValues = data.xTickValues;
                    state.yTickValues = data.yTickValues;
                    state.tickFontSize = data.tickFontSize;

                    plotStorageConsumption();
                    setupZoomAndScroll('#storage-consumption', '#storage-consumption-container');

                    buttonDownload.addEventListener('click', () => downloadSVG('storage-consumption'));
                    buttonReset.addEventListener('click', () => plotStorageConsumption());
                    return;
                }
            } catch (error) {
                if (error.name === 'AbortError') {
                    console.log('Request timed out, retrying...');
                } else {
                    throw error;
                }
            }

            retries++;
            if (retries < maxRetries) {
                await new Promise(resolve => setTimeout(resolve, retryDelay));
            }
        }
        console.warn('Failed to get storage consumption data after multiple retries');
    } catch (error) {
        console.error('Error:', error);
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
    if (!state.worker_storage_consumption) {
        return;
    }

    svgElement.selectAll('*').remove();

    const margin = calculateMargin();
    const width = svgContainer.clientWidth - margin.left - margin.right;
    const height = svgContainer.clientHeight - margin.top - margin.bottom;

    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const xScale = d3.scaleLinear()
        .domain([state.xMin, state.xMax])
        .range([0, width]);

    const yScale = d3.scaleLinear()
        .domain([state.yMin, state.yMax])
        .range([height, 0]);

    const xAxis = d3.axisBottom(xScale)
        .tickValues(state.xTickValues)
        .tickFormat(d => `${d3.format('.2f')(d)} s`);
    
    svg.append('g')
        .attr('class', 'x-axis')
        .attr('transform', `translate(0, ${height})`)
        .call(xAxis)
        .attr('stroke-width', 0.8)
        .selectAll('text')
        .style('font-size', state.tickFontSize);

    const yAxis = d3.axisLeft(yScale)
        .tickValues(state.yTickValues)
        .tickFormat(d => `${d.toFixed(2)} ${state.file_size_unit}`);
    
    svg.append('g')
        .attr('class', 'y-axis')
        .call(yAxis)
        .attr('stroke-width', 0.8)
        .selectAll('text')
        .style('font-size', state.tickFontSize);

    // Create line generator
    const line = d3.line()
        .x(d => xScale(d[0]))
        .y(d => yScale(d[1]))
        .curve(d3.curveStepAfter);

    // Draw lines for each worker
    Object.entries(state.worker_storage_consumption).forEach(([workerId, points], index) => {
        const color = d3.schemeCategory10[index % 10];
        const safeWorkerId = workerId.replace(/[.:]/g, '\\$&'); // Escape special characters
        
        svg.append('path')
            .datum(points)
            .attr('fill', 'none')
            .attr('stroke', color)
            .attr('stroke-width', 0.8)
            .attr('class', `worker-line worker-${safeWorkerId}`)
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
                
                tooltip.style.visibility = 'visible';
                tooltip.innerHTML = `Worker: ${workerId}`;
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
}

window.document.addEventListener('dataLoaded', initialize);
window.addEventListener('resize', _.debounce(() => plotStorageConsumption(), 300));