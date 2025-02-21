import { downloadSVG } from './tools.js';
import { setupZoomAndScroll } from './tools.js';

const buttonReset = document.getElementById('button-reset-worker-transfers');
const buttonDownload = document.getElementById('button-download-worker-transfers');
const buttonToggleType = document.getElementById('button-toggle-transfer-type');
const svgContainer = document.getElementById('worker-transfers-container');
const svgElement = d3.select('#worker-transfers');
const tooltip = document.getElementById('vine-tooltip');

const HIGHLIGHT_COLOR = 'orange';
const LINE_WIDTH = 0.8;  // Match other charts' line width
const HIGHLIGHT_WIDTH = 2;  // Adjusted highlight width
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
        // Clear existing data
        state.transfers = {};
        state.xMin = null;
        state.xMax = null;
        state.yMin = null;
        state.yMax = null;
        state.xTickValues = null;
        state.yTickValues = null;

        const transferType = state.showIncoming ? 'incoming' : 'outgoing';
        const response = await fetch(`/api/worker-transfers?type=${transferType}`);
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

            // Clear SVG and redraw
            svgElement.selectAll('*').remove();
            plotWorkerTransfers();
            setupZoomAndScroll('#worker-transfers', '#worker-transfers-container');
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
    Object.entries(state.transfers).forEach(([workerId, transfers]) => {
        const line = d3.line()
            .x(d => xScale(d[0]))
            .y(d => yScale(d[1]));

        svg.append('path')
            .datum(transfers)
            .attr('fill', 'none')
            .attr('stroke', colorScale(workerId))
            .attr('stroke-width', LINE_WIDTH)
            .attr('d', line)
            .on('mouseover', function(e) {
                d3.select(this)
                    .attr('stroke', HIGHLIGHT_COLOR)
                    .attr('stroke-width', HIGHLIGHT_WIDTH);

                // Find closest point to mouse position
                const mouseX = xScale.invert(d3.pointer(e)[0]);
                const bisect = d3.bisector(d => d[0]).left;
                const index = bisect(transfers, mouseX);
                const point = transfers[index];

                if (point) {
                    tooltip.style.visibility = 'visible';
                    tooltip.innerHTML = `
                        Worker: ${workerId}<br>
                        Time: ${point[0].toFixed(2)}s<br>
                        Concurrent Transfers: ${point[1]}
                    `;
                    tooltip.style.top = (e.pageY - 15) + 'px';
                    tooltip.style.left = (e.pageX + 10) + 'px';
                }
            })
            .on('mousemove', function(e) {
                const mouseX = xScale.invert(d3.pointer(e)[0]);
                const bisect = d3.bisector(d => d[0]).left;
                const index = bisect(transfers, mouseX);
                const point = transfers[index];

                if (point) {
                    tooltip.style.top = (e.pageY - 15) + 'px';
                    tooltip.style.left = (e.pageX + 10) + 'px';
                    tooltip.innerHTML = `
                        Worker: ${workerId}<br>
                        Time: ${point[0].toFixed(2)}s<br>
                        Concurrent Transfers: ${point[1]}
                    `;
                }
            })
            .on('mouseout', function() {
                d3.select(this)
                    .attr('stroke', colorScale(workerId))
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

function setupEventListeners() {
    buttonDownload.addEventListener('click', () => downloadSVG('worker-transfers'));
    buttonReset.addEventListener('click', () => plotWorkerTransfers());
    buttonToggleType.addEventListener('click', async () => {
        state.showIncoming = !state.showIncoming;
        buttonToggleType.textContent = state.showIncoming ? 'Show Outgoing' : 'Show Incoming';
        await fetchData();  // Fetch new data and redraw
    });
}

async function initialize() {
    try {
        svgElement.selectAll('*').remove();
        await fetchData();
    } catch (error) {
        console.error('Error:', error);
    }
}

setupEventListeners();
window.document.addEventListener('dataLoaded', initialize);
window.addEventListener('resize', _.debounce(() => plotWorkerTransfers(), 300));
