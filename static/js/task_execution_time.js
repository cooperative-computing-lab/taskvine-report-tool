import { downloadSVG } from './tools.js';
import { setupZoomAndScroll } from './tools.js';

// colors for visualization
const PRIMARY_COLOR = '#2077B4';
const HIGHLIGHT_COLOR = 'orange';

const dotRadius = 1.5;
const highlightRadius = 3;

// state for visualization
const state = {
    taskExecutionTime: [],
    taskExecutionTimeCDF: [],
    tickValues: {},
    tickFontSize: null,
    showCDF: false
};

// tooltip element
const tooltip = d3.select('body').select('#vine-tooltip');

// dom elements
let buttonReset;
let buttonDownload;
let buttonToggleCDF;
let svgContainer;
let svgElement;

function calculateMargin() {
    if (!state.taskExecutionTime.length) {
        return { top: 40, right: 30, bottom: 40, left: 30 };
    }

    const margin = { top: 40, right: 30, bottom: 40, left: 30 };

    const tempSvg = svgElement
        .append('g')
        .attr('class', 'temp');

    if (state.showCDF) {
        const tempYScale = d3.scaleLinear()
            .domain([0, 1]);

        const tempYAxis = d3.axisLeft(tempYScale)
            .tickValues(state.tickValues.probabilityY)
            .tickFormat(d => `${(d * 100).toFixed(0)}%`);

        tempSvg.call(tempYAxis);
        tempSvg.selectAll('text').style('font-size', state.tickFontSize);
    } else {
        const tempYScale = d3.scaleLinear()
            .domain([0, d3.max(state.taskExecutionTime, d => d[1])]);

        const tempYAxis = d3.axisLeft(tempYScale)
            .tickValues(state.tickValues.executionTimeY)
            .tickFormat(d => `${d.toFixed(2)}s`);

        tempSvg.call(tempYAxis);
        tempSvg.selectAll('text').style('font-size', state.tickFontSize);
    }

    const maxYLabelWidth = d3.max(tempSvg.selectAll('.tick text').nodes(),
        d => d.getBBox().width);
    tempSvg.remove();

    margin.left = Math.ceil(maxYLabelWidth + 20);

    return margin;
}

function setupEventListeners() {
    buttonDownload.addEventListener('click', () => downloadSVG('task-execution-time'));
    buttonReset.addEventListener('click', () => handleResetClick());
    buttonToggleCDF.addEventListener('click', () => {
        state.showCDF = !state.showCDF;
        buttonToggleCDF.textContent = state.showCDF ? 'Display Time' : 'Display CDF';
        plotExecutionTime();
    });
}

async function initialize() {
    try {
        // init dom elements
        buttonReset = document.getElementById('button-reset-task-execution-time');
        buttonDownload = document.getElementById('button-download-task-execution-time');
        buttonToggleCDF = document.getElementById('button-toggle-cdf');
        svgContainer = document.getElementById('task-execution-time-container');
        svgElement = d3.select('#task-execution-time');
        
        // clear previous content
        svgElement.selectAll('*').remove();
        state.taskExecutionTime = [];
        state.taskExecutionTimeCDF = [];

        // setup event listeners
        setupEventListeners();

        const response = await fetch('/api/task-execution-time');
        const data = await response.json();

        if (data) {
            state.taskExecutionTime = data.task_execution_time;
            state.taskExecutionTimeCDF = data.task_execution_time_cdf;
            state.tickValues = {
                executionTimeX: data.execution_time_x_tick_values,
                executionTimeY: data.execution_time_y_tick_values,
                probabilityX: data.probability_x_tick_values,
                probabilityY: data.probability_y_tick_values
            };
            state.tickFontSize = data.tickFontSize;

            document.querySelector('#task-execution-time').style.width = '100%';
            document.querySelector('#task-execution-time').style.height = '100%';
            plotExecutionTime();
            setupZoomAndScroll('#task-execution-time', '#task-execution-time-container');
        }
    } catch (error) {
        console.error('Error:', error);
    }
}

function plotExecutionTime() {
    if (!state.taskExecutionTime.length) return;

    svgElement.selectAll('*').remove();

    const margin = calculateMargin();
    const width = svgContainer.clientWidth - margin.left - margin.right;
    const height = svgContainer.clientHeight - margin.top - margin.bottom;

    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    if (state.showCDF) {
        const xScale = d3.scaleLinear()
            .domain([0, d3.max(state.taskExecutionTimeCDF, d => d[0])])
            .range([0, width]);

        const yScale = d3.scaleLinear()
            .domain([0, 1])
            .range([height, 0]);

        // Draw CDF line
        svg.append('path')
            .datum(state.taskExecutionTimeCDF)
            .attr('fill', 'none')
            .attr('stroke', PRIMARY_COLOR)
            .attr('stroke-width', 2)
            .attr('d', d3.line()
                .x(d => xScale(d[0]))
                .y(d => yScale(d[1]))
            );

        // Add interactive points on the CDF line
        svg.selectAll('circle')
            .data(state.taskExecutionTimeCDF)
            .enter()
            .append('circle')
            .attr('cx', d => xScale(d[0]))
            .attr('cy', d => yScale(d[1]))
            .attr('r', dotRadius)
            .attr('fill', PRIMARY_COLOR)
            .on('mouseover', function(event, d) {
                d3.select(this)
                    .attr('fill', HIGHLIGHT_COLOR)
                    .attr('r', highlightRadius);

                tooltip
                    .style('visibility', 'visible')
                    .html(`Time: ${d[0].toFixed(2)}s<br>Probability: ${(d[1] * 100).toFixed(2)}%`);
            })
            .on('mousemove', function(event) {
                tooltip
                    .style('top', (event.pageY - 10) + 'px')
                    .style('left', (event.pageX + 10) + 'px');
            })
            .on('mouseout', function() {
                d3.select(this)
                    .attr('fill', PRIMARY_COLOR)
                    .attr('r', dotRadius);

                tooltip.style('visibility', 'hidden');
            });

        // Add axes with custom tick values
        svg.append('g')
            .attr('transform', `translate(0, ${height})`)
            .call(d3.axisBottom(xScale)
                .tickValues(state.tickValues.probabilityX)
                .tickFormat(d => `${d.toFixed(2)}s`)
                .tickSizeOuter(0))
            .style('font-size', `${state.tickFontSize}px`);

        svg.append('g')
            .call(d3.axisLeft(yScale)
                .tickValues(state.tickValues.probabilityY)
                .tickFormat(d => `${(d * 100).toFixed(0)}%`)
                .tickSizeOuter(0))
            .style('font-size', `${state.tickFontSize}px`);

    } else {
        // Plot execution time
        const xScale = d3.scaleLinear()
            .domain([0, state.taskExecutionTime.length])
            .range([0, width]);

        const yScale = d3.scaleLinear()
            .domain([0, d3.max(state.taskExecutionTime, d => d[1])])
            .range([height, 0]);

        // Draw scatter plot with hover effects
        svg.selectAll('circle')
            .data(state.taskExecutionTime)
            .enter()
            .append('circle')
            .attr('cx', (d, i) => xScale(i))
            .attr('cy', d => yScale(d[1]))
            .attr('r', dotRadius)
            .attr('fill', PRIMARY_COLOR)
            .on('mouseover', function(event, d) {
                d3.select(this)
                    .attr('fill', HIGHLIGHT_COLOR)
                    .attr('r', highlightRadius);

                tooltip
                    .style('visibility', 'visible')
                    .html(`Task ID: ${d[0]}<br>Execution Time: ${d[1].toFixed(2)}s`);
            })
            .on('mousemove', function(event) {
                tooltip
                    .style('top', (event.pageY - 10) + 'px')
                    .style('left', (event.pageX + 10) + 'px');
            })
            .on('mouseout', function() {
                d3.select(this)
                    .attr('fill', PRIMARY_COLOR)
                    .attr('r', dotRadius);

                tooltip.style('visibility', 'hidden');
            });

        // Add axes with custom tick values
        svg.append('g')
            .attr('transform', `translate(0, ${height})`)
            .call(d3.axisBottom(xScale)
                .tickValues(state.tickValues.executionTimeX)
                .tickFormat(d => Math.floor(d))
                .tickSizeOuter(0))
            .style('font-size', `${state.tickFontSize}px`);

        svg.append('g')
            .call(d3.axisLeft(yScale)
                .tickValues(state.tickValues.executionTimeY)
                .tickFormat(d => `${d.toFixed(2)}s`)
                .tickSizeOuter(0))
            .style('font-size', `${state.tickFontSize}px`);
    }
}

function handleResetClick() {
    document.querySelector('#task-execution-time').style.width = '100%';
    document.querySelector('#task-execution-time').style.height = '100%';
    plotExecutionTime();
}

// wait for dataLoaded event before initializing
window.document.addEventListener('dataLoaded', initialize);
window.addEventListener('resize', _.debounce(() => plotExecutionTime(), 300)); 