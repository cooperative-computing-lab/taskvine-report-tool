import { downloadSVG, setupZoomAndScroll } from './tools.js';

const buttonReset = document.getElementById('button-reset-file-replicas');
const buttonDownload = document.getElementById('button-download-file-replicas');
const svgElement = d3.select('#file-replicas');
const svgContainer = document.getElementById('file-replicas-container');

const dotRadius = 1.5;
const highlightRadius = 3;

const HIGHLIGHT_COLOR = 'orange';
const LINE_COLOR = '#1f77b4';

const state = {
    data: null,
    xMin: null,
    xMax: null,
    yMin: null,
    yMax: null,
    xTickValues: null,
    yTickValues: null,
    tickFontSize: null,
    selectedOrder: 'asc'
};

const orderTypes = {
    'asc': { label: 'Replicas Ascending' },
    'desc': { label: 'Replicas Descending' }
};

function calculateMargin() {
    if (!state.data) {
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
        .tickFormat(d => `${d}`);

    tempSvg.call(tempYAxis);
    tempSvg.selectAll('text').style('font-size', state.tickFontSize);

    const maxYLabelWidth = d3.max(tempSvg.selectAll('.tick text').nodes(),
        d => d.getBBox().width);
    tempSvg.remove();

    margin.left = Math.ceil(maxYLabelWidth + 20);

    return margin;
}

function plotFileReplicas() {
    if (!state.data) return;

    svgElement.selectAll('*').remove();

    const margin = calculateMargin();
    const width = svgContainer.clientWidth - margin.left - margin.right;
    const height = svgContainer.clientHeight - margin.top - margin.bottom;

    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    // Set up scales
    const xScale = d3.scaleLinear()
        .domain([state.xMin, state.xMax])
        .range([0, width]);

    const yScale = d3.scaleLinear()
        .domain([state.yMin, state.yMax])
        .range([height, 0]);

    // Add X axis
    svg.append('g')
        .attr('transform', `translate(0,${height})`)
        .call(d3.axisBottom(xScale)
            .tickValues(state.xTickValues)
            .tickFormat(d3.format('d')))
        .selectAll('text')
        .style('font-size', state.tickFontSize);

    // Add Y axis
    svg.append('g')
        .call(d3.axisLeft(yScale)
            .tickValues(state.yTickValues)
            .tickFormat(d => d))
        .selectAll('text')
        .style('font-size', state.tickFontSize);
    
    const points = svg.selectAll('circle')
        .data(state.data)
        .enter()
        .append('circle')
        .attr('cx', d => {
            const x = xScale(d[0]);
            if (isNaN(x)) {
                console.log('Invalid x value:', d[0], 'for data point:', d);
            }
            return x;
        })
        .attr('cy', d => {
            const y = yScale(d[3]);
            if (isNaN(y)) {
                console.log('Invalid y value:', d[3], 'for data point:', d);
            }
            return y;
        })
        .attr('r', dotRadius)
        .attr('fill', LINE_COLOR)
        .on('mouseover', function(event, d) {
            d3.select(this)
                .attr('fill', HIGHLIGHT_COLOR)
                .attr('r', highlightRadius);
            
            // Show tooltip
            const tooltip = d3.select('#vine-tooltip');
            tooltip.style('visibility', 'visible')
                .html(`
                    <div>File: ${d[1]}</div>
                    <div>Size: ${d[2].toFixed(2)} MB</div>
                    <div>Replicas: ${d[3]}</div>
                `);
            tooltip.style('top', (event.pageY - 10) + 'px')
                .style('left', (event.pageX + 10) + 'px');
        })
        .on('mouseout', function() {
            d3.select(this)
                .attr('fill', LINE_COLOR)
                .attr('r', dotRadius);
            d3.select('#vine-tooltip').style('visibility', 'hidden');
        });
}

function setupControls() {
    const container = document.getElementById('file-replicas-legend');
    container.className = 'file-replicas-controls';
    container.style.display = 'flex';
    container.style.gap = '20px';
    container.style.marginBottom = '10px';
    container.style.alignItems = 'center';
    
    // Sort selector
    const sortDiv = document.createElement('div');
    sortDiv.className = 'control-group';
    sortDiv.style.display = 'flex';
    sortDiv.style.alignItems = 'center';
    sortDiv.style.gap = '8px';
    
    const sortLabel = document.createElement('label');
    sortLabel.textContent = 'Sort By:';
    sortLabel.style.color = '#4a4a4a';
    
    const sortSelect = document.createElement('select');
    sortSelect.className = 'vine-select';
    sortSelect.style.padding = '4px 8px';
    sortSelect.style.borderRadius = '4px';
    sortSelect.style.border = '1px solid #ddd';
    sortSelect.style.backgroundColor = '#fff';
    sortSelect.style.cursor = 'pointer';
    
    Object.entries(orderTypes).forEach(([order, info]) => {
        const option = document.createElement('option');
        option.value = order;
        option.textContent = info.label;
        option.selected = order === state.selectedOrder;
        sortSelect.appendChild(option);
    });
    
    sortSelect.addEventListener('change', async (event) => {
        state.selectedOrder = event.target.value;
        await initialize();
    });
    
    sortDiv.appendChild(sortLabel);
    sortDiv.appendChild(sortSelect);
    
    container.innerHTML = '';
    container.appendChild(sortDiv);
}

function handleDownloadClick() {
    downloadSVG('file-replicas', 'file-replicas.svg');
}

function handleResetClick() {
    document.querySelector('#file-replicas').style.width = '100%';
    document.querySelector('#file-replicas').style.height = '100%';
    plotFileReplicas();
}

async function initialize() {
    try {
        const response = await fetch(`/api/file-replicas?order=${state.selectedOrder}`);
        if (!response.ok) throw new Error('Network response was not ok');
        const data = await response.json();
        
        state.data = data.file_replicas;
        state.xMin = data.xMin;
        state.xMax = data.xMax;
        state.yMin = data.yMin;
        state.yMax = data.yMax;
        state.xTickValues = data.xTickValues;
        state.yTickValues = data.yTickValues;
        state.tickFontSize = data.tickFontSize;

        setupControls();
        
        plotFileReplicas();
        
        setupZoomAndScroll('#file-replicas', '#file-replicas-container');
        buttonDownload.addEventListener('click', handleDownloadClick);
        buttonReset.addEventListener('click', handleResetClick);
    } catch (error) {
        console.error('Error initializing file replicas:', error);
    }
}

window.document.addEventListener('dataLoaded', initialize);
window.addEventListener('resize', _.debounce(() => plotFileReplicas(), 300));
