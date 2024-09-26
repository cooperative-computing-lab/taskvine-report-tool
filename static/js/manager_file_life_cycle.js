import { formatUnixTimestamp, downloadSVG } from './tools.js';

const buttonReset = document.getElementById('button-reset-manager-file-life-cycle');
const buttonDownload = document.getElementById('button-download-manager-file-life-cycle');

const svgElement = d3.select('#manager-file-life-cycle');
const svgContainer = document.getElementById('manager-file-life-cycle-container');

const tooltip = document.getElementById('vine-tooltip');


function plotManagerFileLifeCycle() {
    if (!window.managerDiskUsage) {
        return;
    }
    console.log('plotManagerFileLifeCycle');

    svgElement.selectAll('*').remove();

    const data = window.managerDiskUsage;
    console.log(data);
    const margin = {top: 20, right: 20, bottom: 40, left: 60};
    const svgWidth = svgContainer.clientWidth - margin.left - margin.right;
    const svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append("g")
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const minTime = window.minTime;
    const maxTime = window.maxTime;

    const xScale = d3.scaleLinear()
        .domain([0, maxTime - minTime])
        .range([0, svgWidth]);

    const yScale = d3.scaleBand()
        .domain(data.map(d => d.id))
        .range([0, svgHeight])
        .padding(0.1);

    const xAxis = d3.axisBottom(xScale)
        .tickSizeOuter(0)
        .tickValues([
            xScale.domain()[0],
            xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.25,
            xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.5,
            xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.75,
            xScale.domain()[1]
        ])
        .tickFormat(d3.format(window.xTickFormat));
    svg.append("g")
        .attr("transform", `translate(0, ${svgHeight})`)
        .call(xAxis);
    const yAxis = d3.axisLeft(yScale)
        .tickSize(0);
    svg.append('g').call(yAxis);

    const maxBandHeight = 50;

    svg.selectAll('.file-bar')
        .data(data)
        .enter()
        .append('rect')
        .attr('class', 'file-bar')
        .attr('x', d => xScale(d.time_stage_in - minTime))
        .attr('y', d => yScale(d.id) + (yScale.bandwidth() - maxBandHeight) / 2)
        .attr('width', d => xScale(maxTime) - xScale(d.time_stage_in))
        .attr('height', Math.min(yScale.bandwidth(), maxBandHeight))
        .attr('fill', 'steelblue')
        .on('mouseover', function(event, d) {
            d3.select(this).attr('fill', 'orange');
            tooltip.innerHTML = `
                Filename: ${d.filename}<br>
                Creation Time: ${formatUnixTimestamp(d.time_stage_in)}<br>
                End Time: ${formatUnixTimestamp(maxTime)}<br>
                Lifetime: ${(maxTime - d.time_stage_in).toFixed(2)} seconds
            `;
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on('mousemove', function(event) {
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on('mouseout', function() {
            d3.select(this).attr('fill', 'steelblue');
            tooltip.style.visibility = 'hidden';
        });
}

function handleDownloadClick() {
    downloadSVG('manager-file-life-cycle');
}

function handleResetClick() {
    plotManagerFileLifeCycle();
}

window.parent.document.addEventListener('dataLoaded', function() {
    buttonDownload.removeEventListener('click', handleDownloadClick); 
    buttonDownload.addEventListener('click', handleDownloadClick);

    buttonReset.removeEventListener('click', handleResetClick);
    buttonReset.addEventListener('click', handleResetClick);

    plotManagerFileLifeCycle();
});

window.addEventListener('resize', _.debounce(() => plotManagerFileLifeCycle(), 300));
