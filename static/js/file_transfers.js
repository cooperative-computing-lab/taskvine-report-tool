import { downloadSVG } from './tools.js';
import { setupZoomAndScroll } from './tools.js';

const buttonReset = document.getElementById('button-reset-file-transfers');
const buttonDownload = document.getElementById('button-download-file-transfers');
const buttonToggleType = document.getElementById('button-toggle-transfer-type');
const svgContainer = document.getElementById('file-transfers-container');
const svgElement = d3.select('#file-transfers');
const tooltip = document.getElementById('vine-tooltip');
const transferTypeDisplay = document.getElementById('transfer-type-display');
const loadingSpinner = document.getElementById('file-transfers-loading');

const LINE_WIDTH = 0.8;
const HIGHLIGHT_WIDTH = 2;
const HIGHLIGHT_COLOR = 'orange';
const LEGEND_LINE_WIDTH = 3;

const state = {
    transfers: {},
    xMin: null,
    xMax: null,
    yMin: null,
    yMax: null,
    xTickValues: null,
    yTickValues: null,
    tickFontSize: null,
    showIncoming: true  // Toggle between incoming and outgoing
};

async function fetchData() {
    try {
        svgElement.selectAll('*').remove();

        state.transfers = {};
        state.xMin = null;
        state.xMax = null;
        state.yMin = null;
        state.yMax = null;
        state.xTickValues = null;
        state.yTickValues = null;

        const transferType = state.showIncoming ? 'incoming' : 'outgoing';
        const response = await fetch(`/api/file-transfers?type=${transferType}`);
        const data = await response.json();

        if (data) {
            state.transfers = data.transfers;
            state.xMin = data.xMin;
            state.xMax = data.xMax;
            state.yMin = data.yMin;
            state.yMax = data.yMax;
            state.xTickValues = data.xTickValues;
            state.yTickValues = data.yTickValues;
            state.tickFontSize = data.tickFontSize;

            document.querySelector('#file-transfers').style.width = '100%';
            document.querySelector('#file-transfers').style.height = '100%';
            plotWorkerTransfers();
            setupZoomAndScroll('#file-transfers', '#file-transfers-container');
        }
    } catch (error) {
        console.error('Error:', error);
    }
}

function calculateMargin() {
    if (!Object.keys(state.transfers).length) {
        return { top: 40, right: 30, bottom: 40, left: 30 };
    }

    const margin = { top: 40, right: 30, bottom: 40, left: 30 };

    // Calculate left margin based on y-axis labels
    const tempSvg = svgElement
        .append('g')
        .attr('class', 'temp');

    const tempYScale = d3.scaleLinear()
        .domain([state.yMin, state.yMax]);

    const tempYAxis = d3.axisLeft(tempYScale)
        .tickValues(state.yTickValues)
        .tickFormat(d => d);

    tempSvg.call(tempYAxis);
    tempSvg.selectAll('text').style('font-size', state.tickFontSize);

    const maxYLabelWidth = d3.max(tempSvg.selectAll('.tick text').nodes(),
        d => d.getBBox().width);
    tempSvg.remove();

    margin.left = Math.ceil(maxYLabelWidth + 20);

    return margin;
}

function plotWorkerTransfers() {
    if (!Object.keys(state.transfers).length) return;

    svgElement.selectAll('*').remove();

    const margin = calculateMargin();
    const width = svgContainer.clientWidth - margin.left - margin.right;
    const height = svgContainer.clientHeight - margin.top - margin.bottom;

    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    // Set up scales
    const xScale = d3.scaleLinear()
        .domain([state.xMin, state.xMax])
        .range([0, width]);

    const yScale = d3.scaleLinear()
        .domain([state.yMin, state.yMax])
        .range([height, 0]);

    // Create color scale for different workers
    const workerIds = Object.keys(state.transfers);
    const colorScale = d3.scaleOrdinal()
        .domain(workerIds)
        .range(d3.schemeCategory10);

    // Draw lines for each worker
    workerIds.forEach(workerId => {
        const transfers = state.transfers[workerId];
        const line = d3.line()
            .x(d => xScale(d[0]))
            .y(d => yScale(d[1]))
            .curve(d3.curveStepAfter);  // Use step-after curve

        svg.append('path')
            .datum({transfers: transfers, workerId: workerId})
            .attr('class', 'line')
            .attr('fill', 'none')
            .attr('stroke', colorScale(workerId))
            .attr('stroke-width', LINE_WIDTH)
            .attr('d', line(transfers))
            .on('mouseover', function(e) {
                d3.selectAll('.line')
                    .attr('stroke-opacity', 0.2);
                d3.select(this)
                    .attr('stroke', HIGHLIGHT_COLOR)
                    .attr('stroke-opacity', 1)
                    .attr('stroke-width', HIGHLIGHT_WIDTH);

                const data = d3.select(this).datum();
                const mouseX = xScale.invert(d3.pointer(e)[0]);
                
                // Find the closest point
                let closestPoint = data.transfers[0];
                let minDistance = Math.abs(mouseX - closestPoint[0]);
                
                for (let i = 0; i < data.transfers.length; i++) {
                    const distance = Math.abs(mouseX - data.transfers[i][0]);
                    if (distance < minDistance) {
                        minDistance = distance;
                        closestPoint = data.transfers[i];
                    }
                }

                tooltip.style.visibility = 'visible';
                tooltip.innerHTML = `
                    Worker: ${data.workerId}<br>
                    Time: ${closestPoint[0].toFixed(2)}s<br>
                    Concurrent Transfers: ${closestPoint[1]}
                `;
                tooltip.style.top = (e.pageY - 15) + 'px';
                tooltip.style.left = (e.pageX + 10) + 'px';
            })
            .on('mousemove', function(e) {
                const data = d3.select(this).datum();
                const mouseX = xScale.invert(d3.pointer(e)[0]);
                
                // Find the closest point
                let closestPoint = data.transfers[0];
                let minDistance = Math.abs(mouseX - closestPoint[0]);
                
                for (let i = 0; i < data.transfers.length; i++) {
                    const distance = Math.abs(mouseX - data.transfers[i][0]);
                    if (distance < minDistance) {
                        minDistance = distance;
                        closestPoint = data.transfers[i];
                    }
                }

                tooltip.style.visibility = 'visible';
                tooltip.innerHTML = `
                    Worker: ${data.workerId}<br>
                    Time: ${closestPoint[0].toFixed(2)}s<br>
                    Concurrent Transfers: ${closestPoint[1]}
                `;
                tooltip.style.top = (e.pageY - 15) + 'px';
                tooltip.style.left = (e.pageX + 10) + 'px';
            })
            .on('mouseout', function() {
                d3.selectAll('.line')
                    .attr('stroke', function(d) { return colorScale(d.workerId); })
                    .attr('stroke-opacity', 1)
                    .attr('stroke-width', LINE_WIDTH);
                tooltip.style.visibility = 'hidden';
            });
    });

    // Add axes
    svg.append('g')
        .attr('transform', `translate(0, ${height})`)
        .call(d3.axisBottom(xScale)
            .tickValues(state.xTickValues)
            .tickFormat(d => `${d.toFixed(2)}s`)
            .tickSizeOuter(0))
        .style('font-size', `${state.tickFontSize}px`);

    svg.append('g')
        .call(d3.axisLeft(yScale)
            .tickValues(state.yTickValues)
            .tickFormat(d => d)
            .tickSizeOuter(0))
        .style('font-size', `${state.tickFontSize}px`);
}

function handleResetClick() {
    document.querySelector('#file-transfers').style.width = '100%';
    document.querySelector('#file-transfers').style.height = '100%';
    plotWorkerTransfers();
}

function setupEventListeners() {
    // Remove existing event listeners
    buttonDownload.removeEventListener('click', handleDownloadClick);
    buttonReset.removeEventListener('click', handleResetClick);
    buttonToggleType.removeEventListener('click', async () => {
        state.showIncoming = !state.showIncoming;
        buttonToggleType.textContent = state.showIncoming ? 'Show Outgoing' : 'Show Incoming';
        transferTypeDisplay.textContent = state.showIncoming ? 'Incoming' : 'Outgoing';
        await fetchData();
    });

    // Add new event listeners
    buttonDownload.addEventListener('click', handleDownloadClick);
    buttonReset.addEventListener('click', handleResetClick);
    buttonToggleType.addEventListener('click', async () => {
        state.showIncoming = !state.showIncoming;
        buttonToggleType.textContent = state.showIncoming ? 'Show Outgoing' : 'Show Incoming';
        transferTypeDisplay.textContent = state.showIncoming ? 'Incoming' : 'Outgoing';
        await fetchData();
    });
}

function handleDownloadClick() {
    downloadSVG('file-transfers');
}

async function initialize(detail) {
    try {
        // init dom elements
        buttonReset = document.getElementById('button-reset-file-transfers');
        buttonDownload = document.getElementById('button-download-file-transfers');
        buttonToggleType = document.getElementById('button-toggle-transfer-type');
        svgContainer = document.getElementById('file-transfers-container');
        svgElement = d3.select('#file-transfers');
        loadingSpinner = document.getElementById('file-transfers-loading');
        
        // Show loading spinner
        loadingSpinner.style.display = 'block';
        
        // clear previous content
        svgElement.selectAll('*').remove();

        // setup event listeners
        setupEventListeners();

        const response = await fetch('/api/file-transfers');
        const data = await response.json();

        if (data) {
            state.fileTransfers = data.file_transfers;
            state.tickValues = {
                transfersX: data.transfers_x_tick_values,
                transfersY: data.transfers_y_tick_values
            };
            state.tickFontSize = data.tickFontSize;

            document.querySelector('#file-transfers').style.width = '100%';
            document.querySelector('#file-transfers').style.height = '100%';
            plotFileTransfers();
            setupZoomAndScroll('#file-transfers', '#file-transfers-container');
        }
    } catch (error) {
        console.error('Error:', error);
    } finally {
        loadingSpinner.style.display = 'none';
        if (detail && detail.hideSpinner) {
            detail.hideSpinner('file-transfers');
        }
    }
}

setupEventListeners();
window.document.addEventListener('dataLoaded', (event) => initialize(event.detail));
window.addEventListener('resize', _.debounce(() => plotWorkerTransfers(), 300));
