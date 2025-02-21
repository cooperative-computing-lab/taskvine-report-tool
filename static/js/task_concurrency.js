import { downloadSVG } from './tools.js';
import { setupZoomAndScroll } from './tools.js';

const buttonReset = document.getElementById('button-reset-task-concurrency');
const buttonDownload = document.getElementById('button-download-task-concurrency');
const svgContainer = document.getElementById('task-concurrency-container');
const svgElement = d3.select('#task-concurrency');
const tooltip = document.getElementById('vine-tooltip');

const HIGHLIGHT_COLOR = 'orange';

const taskTypes = {
    'tasks_waiting': { color: '#1cb4d6', label: 'Waiting' },
    'tasks_committing': { color: '#4a4a4a', label: 'Committing' },
    'tasks_executing': { color: 'steelblue', label: 'Executing' },
    'tasks_retrieving': { color: '#cc5a12', label: 'Retrieving' },
    'tasks_done': { color: '#9467bd', label: 'Done' }
};

const state = {
    tasks_waiting: null,
    tasks_committing: null,
    tasks_executing: null,
    tasks_retrieving: null,
    tasks_done: null,
    xMin: null,
    xMax: null,
    yMin: null,
    yMax: null,
    xTickValues: null,
    yTickValues: null,
    tickFontSize: null,
    selectedTypes: new Set(Object.keys(taskTypes))
};

function setupTaskTypeCheckboxes() {
    const container = document.getElementById('task-concurrency-checkboxes');
    container.className = 'checkbox-container';

    const itemsContainer = document.createElement('div');
    itemsContainer.className = 'legend-items';
    
    Object.entries(taskTypes).forEach(([type, info]) => {
        const item = document.createElement('div');
        item.className = 'legend-item';
        
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = true;
        checkbox.id = `checkbox-${type}`;
        checkbox.className = 'legend-checkbox';
        checkbox.style.accentColor = info.color;
        
        const label = document.createElement('label');
        label.htmlFor = `checkbox-${type}`;
        label.className = 'legend-label';
        label.textContent = info.label;

        checkbox.addEventListener('change', async (e) => {
            if (e.target.checked) {
                state.selectedTypes.add(type);
                label.style.opacity = '1';
            } else {
                state.selectedTypes.delete(type);
                label.style.opacity = '0.5';
            }
            svgElement.selectAll('*').remove();
            await fetchData();
        });

        item.appendChild(checkbox);
        item.appendChild(label);
        itemsContainer.appendChild(item);
    });

    container.appendChild(itemsContainer);
}

function calculateMargin() {
    if (!state.tasks_waiting) {
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
        .tickFormat(d => d.toFixed(0));

    tempSvg.call(tempYAxis);
    tempSvg.selectAll('text').style('font-size', state.tickFontSize);
    
    const maxYLabelWidth = d3.max(tempSvg.selectAll('.tick text').nodes(), 
        d => d.getBBox().width);
    tempSvg.remove();

    margin.left = Math.ceil(maxYLabelWidth + 20);

    return margin;
}

function plotTaskConcurrency() {
    if (!state.tasks_waiting) {
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

    // Add axes
    svg.append('g')
        .attr('class', 'x-axis')
        .attr('transform', `translate(0, ${height})`)
        .call(d3.axisBottom(xScale)
            .tickValues(state.xTickValues)
            .tickFormat(d => `${d.toFixed(2)}s`)
            .tickSizeOuter(0))
        .selectAll('text')
        .style('font-size', state.tickFontSize);

    svg.append('g')
        .attr('class', 'y-axis')
        .call(d3.axisLeft(yScale)
            .tickValues(state.yTickValues)
            .tickFormat(d => d.toFixed(0))
            .tickSizeOuter(0))
        .selectAll('text')
        .style('font-size', state.tickFontSize);

    // Create line generator
    const line = d3.line()
        .x(d => xScale(d[0]))
        .y(d => yScale(d[1]));

    // Draw lines for each task type
    Object.entries(taskTypes).forEach(([type, info]) => {
        if (!state.selectedTypes.has(type) || !state[type]) return;

        svg.append('path')
            .datum(state[type])
            .attr('fill', 'none')
            .attr('stroke', info.color)
            .attr('stroke-width', 2)
            .attr('class', `task-line ${type}-line`)
            .attr('d', line)
            .on('mouseover', function(e, d) {
                d3.select(this)
                    .attr('stroke', HIGHLIGHT_COLOR)
                    .attr('stroke-width', 3)
                    .raise();
                
                // Find the closest data point to mouse position
                const mouseX = xScale.invert(d3.pointer(e)[0]);
                const bisect = d3.bisector(d => d[0]).left;
                const index = bisect(d, mouseX);
                const point = d[index];

                if (point) {
                    tooltip.style.visibility = 'visible';
                    tooltip.innerHTML = `
                        Type: ${info.label}<br>
                        Time: ${point[0].toFixed(2)}s<br>
                        Concurrent Tasks: ${point[1]}
                    `;
                    tooltip.style.top = (e.pageY - 15) + 'px';
                    tooltip.style.left = (e.pageX + 10) + 'px';
                }
            })
            .on('mousemove', function(e, d) {
                // Update tooltip position and content on mouse move
                const mouseX = xScale.invert(d3.pointer(e)[0]);
                const bisect = d3.bisector(d => d[0]).left;
                const index = bisect(d, mouseX);
                const point = d[index];

                if (point) {
                    tooltip.innerHTML = `
                        Type: ${info.label}<br>
                        Time: ${point[0].toFixed(2)}s<br>
                        Concurrent Tasks: ${point[1]}
                    `;
                    tooltip.style.top = (e.pageY - 15) + 'px';
                    tooltip.style.left = (e.pageX + 10) + 'px';
                }
            })
            .on('mouseout', function() {
                d3.select(this)
                    .attr('stroke', info.color)
                    .attr('stroke-width', 2);
                
                tooltip.style.visibility = 'hidden';
            });
    });
}

async function fetchData() {
    try {
        // Clear existing data in state
        Object.keys(taskTypes).forEach(type => {
            state[type] = null;
        });
        state.xMin = null;
        state.xMax = null;
        state.yMin = null;
        state.yMax = null;
        state.xTickValues = null;
        state.yTickValues = null;

        const selectedTypesParam = Array.from(state.selectedTypes).join(',');
        const response = await fetch(`/api/task-concurrency?types=${selectedTypesParam}`);
        const data = await response.json();

        if (data) {
            // Update state with new data
            Object.keys(taskTypes).forEach(type => {
                state[type] = data[type];
            });
            state.xMin = data.xMin;
            state.xMax = data.xMax;
            state.yMin = data.yMin;
            state.yMax = data.yMax;
            state.xTickValues = data.xTickValues;
            state.yTickValues = data.yTickValues;
            state.tickFontSize = data.tickFontSize;

            // Clear SVG and redraw
            svgElement.selectAll('*').remove();
            plotTaskConcurrency();
            setupZoomAndScroll('#task-concurrency', '#task-concurrency-container');
        }
    } catch (error) {
        console.error('Error:', error);
    }
}

async function initialize() {
    try {
        // Clear SVG
        svgElement.selectAll('*').remove();
        await fetchData();
    } catch (error) {
        console.error('Error:', error);
    }
}

function handleResetClick() {
    document.querySelector('#task-concurrency').style.width = '100%';
    document.querySelector('#task-concurrency').style.height = '100%';
    plotTaskConcurrency();
}

setupTaskTypeCheckboxes();
buttonDownload.addEventListener('click', () => downloadSVG('task-concurrency'));
buttonReset.addEventListener('click', () => handleResetClick());
window.document.addEventListener('dataLoaded', initialize);
window.addEventListener('resize', _.debounce(() => plotTaskConcurrency(), 300)); 