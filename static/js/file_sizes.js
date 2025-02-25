import { downloadSVG, setupZoomAndScroll } from './tools.js';

const buttonReset = document.getElementById('button-reset-file-sizes');
const buttonDownload = document.getElementById('button-download-file-sizes');
const svgElement = d3.select('#file-sizes');
const svgContainer = document.getElementById('file-sizes-container');

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
    selectedOrder: 'asc',
    selectedType: 'task-created'
};

const fileTypes = {
    'all': { label: 'All Files' },
    'temp': { label: 'Temporary Files' },
    'meta': { label: 'Meta Files' },
    'buffer': { label: 'Buffer Files' },
    'task-created': { label: 'Task Created Files' },
    'transferred': { label: 'Transferred Files' }
};

const orderTypes = {
    'asc': { label: 'Size Ascending' },
    'desc': { label: 'Size Descending' },
    'created-time': { label: 'Time Created' }
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
        .tickFormat(d => `${d} ${state.fileSizeUnit}`);

    tempSvg.call(tempYAxis);
    tempSvg.selectAll('text').style('font-size', state.tickFontSize);

    const maxYLabelWidth = d3.max(tempSvg.selectAll('.tick text').nodes(),
        d => d.getBBox().width);
    tempSvg.remove();

    margin.left = Math.ceil(maxYLabelWidth + 20);

    return margin;
}

async function initialize() {
    try {
        svgElement.selectAll('*').remove();
        state.data = null;

        const response = await fetch(`/api/file-sizes?order=${state.selectedOrder}&type=${state.selectedType}`);
        if (!response.ok) throw new Error('Network response was not ok');
        const data = await response.json();
        
        state.data = data.file_sizes;
        state.xMin = data.xMin;
        state.xMax = data.xMax;
        state.yMin = data.yMin;
        state.yMax = data.yMax;
        state.xTickValues = data.xTickValues;
        state.yTickValues = data.yTickValues;
        state.tickFontSize = data.tickFontSize;
        state.fileSizeUnit = data.file_size_unit;

        setupControls();
        
        document.querySelector('#file-sizes').style.width = '100%';
        document.querySelector('#file-sizes').style.height = '100%';
        plotFileSizes();
        
        setupZoomAndScroll('#file-sizes', '#file-sizes-container');

        buttonDownload.removeEventListener('click', handleDownloadClick); 
        buttonDownload.addEventListener('click', handleDownloadClick);

        buttonReset.removeEventListener('click', handleResetClick);
        buttonReset.addEventListener('click', handleResetClick);
    } catch (error) {
        console.error('Error initializing file sizes:', error);
    }
}

function plotFileSizes() {
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

    // Calculate bar width
    const barWidth = Math.min(
        (width / state.data.length) * 0.8,  // 80% of available space
        30  // maximum width of 30 pixels
    );

    // Adjust xScale to add one bar width of padding on each side
    const xScale = d3.scaleLinear()
        .domain([state.xMin - 1, state.xMax + 1])  // Add padding of 1 unit on each side
        .range([barWidth, width - barWidth]);  // Adjust range to leave space for one bar

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
            .tickFormat(d => `${d} ${state.fileSizeUnit}`))
        .selectAll('text')
        .style('font-size', state.tickFontSize);

    // Add the bars with updated xScale
    svg.selectAll('rect')
        .data(state.data)
        .enter()
        .append('rect')
        .attr('x', d => xScale(d[0]) - barWidth/2)
        .attr('y', d => yScale(d[2]))
        .attr('width', barWidth)
        .attr('height', d => height - yScale(d[2]))
        .attr('fill', LINE_COLOR)
        .on('mouseover', function(event, d) {
            d3.select(this)
                .attr('fill', HIGHLIGHT_COLOR)
                .attr('width', barWidth * 2);
            
            const tooltip = d3.select('#vine-tooltip');
            tooltip.style('visibility', 'visible')
                .html(`
                    <div>File: ${d[1]}</div>
                    <div>Size: ${d[2].toFixed(2)} ${state.fileSizeUnit}</div>
                    <div>Created Time: ${d[3].toFixed(2)}s</div>
                `);
            tooltip.style('top', (event.pageY - 10) + 'px')
                .style('left', (event.pageX + 10) + 'px');
        })
        .on('mouseout', function() {
            d3.select(this)
                .attr('fill', LINE_COLOR)
                .attr('width', barWidth);
            d3.select('#vine-tooltip').style('visibility', 'hidden');
        });
}

function handleResetClick() {
    document.querySelector('#file-sizes').style.width = '100%';
    document.querySelector('#file-sizes').style.height = '100%';
    plotFileSizes();
}

function handleDownloadClick() {
    downloadSVG('file-sizes');
}

function setupControls() {
    const container = document.getElementById('file-sizes-legend');
    container.className = 'file-sizes-controls';
    container.style.display = 'flex';
    container.style.gap = '20px';
    container.style.marginBottom = '10px';
    container.style.alignItems = 'center';
    
    // File type selector
    const typeDiv = document.createElement('div');
    typeDiv.className = 'control-group';
    typeDiv.style.display = 'flex';
    typeDiv.style.alignItems = 'center';
    typeDiv.style.gap = '8px';
    
    const typeLabel = document.createElement('label');
    typeLabel.textContent = 'File Type:';
    typeLabel.style.color = '#4a4a4a';
    
    const typeSelect = document.createElement('select');
    typeSelect.className = 'vine-select';
    typeSelect.style.padding = '4px 8px';
    typeSelect.style.borderRadius = '4px';
    typeSelect.style.border = '1px solid #ddd';
    typeSelect.style.backgroundColor = '#fff';
    typeSelect.style.cursor = 'pointer';
    
    Object.entries(fileTypes).forEach(([type, info]) => {
        const option = document.createElement('option');
        option.value = type;
        option.textContent = info.label;
        option.selected = type === state.selectedType;
        typeSelect.appendChild(option);
    });
    
    typeSelect.addEventListener('change', async (event) => {
        state.selectedType = event.target.value;
        await initialize();
    });
    
    typeDiv.appendChild(typeLabel);
    typeDiv.appendChild(typeSelect);
    
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
    
    // Add to container
    container.innerHTML = '';
    container.appendChild(typeDiv);
    container.appendChild(sortDiv);
}

window.document.addEventListener('dataLoaded', initialize);
window.addEventListener('resize', _.debounce(() => plotFileSizes(), 300));
