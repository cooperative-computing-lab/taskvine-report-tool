import { formatUnixTimestamp, downloadSVG } from './tools.js';

const buttonReset = document.getElementById('button-reset-manager-disk-usage');
const buttonDownload = document.getElementById('button-download-manager-disk-usage');

const svgElement = d3.select('#manager-disk-usage');
const svgContainer = document.getElementById('manager-disk-usage-container');

const tooltip = document.getElementById('vine-tooltip');

const file_size_precesion = 2;

function plotManagerDiskUsage() {
    if (!window.managerDiskUsage) {
        return;
    }

    // first remove all existing elements
    svgElement.selectAll('*').remove();

    var data = window.managerDiskUsage;

    // set dimensions and margins
    const margin = calculateMargin();
    console.log('manager disk usage margin', margin);

    const svgWidth = svgContainer.clientWidth - margin.left - margin.right;
    const svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append("g")
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const minTime = window.minTime;
    const maxTime = window.maxTime;

    // Ensure the first and the last point have disk usage of 0
    const firstPoint = {
        time_stage_in: minTime,
        'accumulated_disk_usage(MB)': 0,
        filename: 'initial',
        'size(MB)': 0,
        from_worker: 'N/A'
    };
    const lastPoint = {
        time_stage_in: maxTime,
        'accumulated_disk_usage(MB)': 0,
        filename: 'final',
        'size(MB)': 0,
        from_worker: 'N/A'
    };
    data = [firstPoint, ...data, lastPoint];

    const xScale = d3.scaleLinear()
        .domain([0, maxTime - minTime])
        .range([0, svgWidth]);

    const maxAccumulatedDiskUsage = d3.max(data, d => d['accumulated_disk_usage(MB)']);
    const yScale = d3.scaleLinear()
        .domain([0, maxAccumulatedDiskUsage])
        .range([svgHeight, 0]);

    // plot axis
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
    svg.append('g')
        .attr('transform', `translate(0, ${svgHeight})`)
        .call(xAxis)
        .selectAll('text')
        .attr('font-size', window.xTickFontSize);

    const yAxis = d3.axisLeft(yScale)
        .tickSizeOuter(0)
        .tickValues([
            yScale.domain()[0],
            yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.25,
            yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.5,
            yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.75,
            yScale.domain()[1]
        ])
        .tickFormat(d3.format(`.${file_size_precesion}f`));
    svg.append('g')
        .call(yAxis)
        .selectAll('text')
        .attr('font-size', window.yTickFontSize);

    const line = d3.line()
        .x(d => xScale(d.time_stage_in - minTime))
        .y(d => yScale(d['accumulated_disk_usage(MB)']))
        .curve(d3.curveStepAfter);

    svg.append("path")
        .datum(data)
        .attr("class", "line")
        .attr("fill", "none")
        .attr("stroke", "steelblue")
        .attr("stroke-width", 1)
        .attr("d", line);

    // interactive
    svg.selectAll("dot")
        .data(data)
        .enter().append("circle")
        .attr("r", 1)
        .attr("cx", d => xScale(d.time_stage_in - minTime))
        .attr("cy", d => yScale(d['accumulated_disk_usage(MB)']))
        .attr("fill", "steelblue")
        .on("mouseover", function(event, d) {
            d3.select(this)
                .attr("r", 3)
                .attr("fill", 'orange');
            tooltip.innerHTML = `
                    time: ${(d.time_stage_in - minTime).toFixed(2)}s<br>
                    filename: ${d.filename}<br>
                    from worker: ${d.from_worker}<br>
                    size (MB): ${d['size(MB)'].toFixed(file_size_precesion)} MB <br>
                    accumulated disk usage (MB): ${d['accumulated_disk_usage(MB)'].toFixed(file_size_precesion)} MB
                `;
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on("mouseout", function() {
            d3.select(this)
                .attr("r", 1)
                .attr("fill", 'steelblue');
            tooltip.style.visibility = 'hidden';
        });
}

function handleDownloadClick() {
    downloadSVG('manager-disk-usage');
}

function handleResetClick() {
    plotManagerDiskUsage();
}

function calculateMargin() {
    const margin = {top: 40, right: 30, bottom: 40, left: 30};
    const svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    const tempSvg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append("g")
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const data = window.managerDiskUsage;
    const maxAccumulatedDiskUsage = d3.max(data, d => d['accumulated_disk_usage(MB)']);
    const yScale = d3.scaleLinear()
        .domain([0, maxAccumulatedDiskUsage])
        .range([svgHeight, 0]);

    const yAxis = d3.axisLeft(yScale)
        .tickSizeOuter(0)
        .tickValues([
            yScale.domain()[0],
            yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.25,
            yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.5,
            yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.75,
            yScale.domain()[1]
        ])
        .tickFormat(d3.format(`.${file_size_precesion}f`));
    tempSvg.append('g')
        .call(yAxis)
        .selectAll('text')
        .attr('font-size', window.yTickFontSize);
    tempSvg.append('g')
        .call(yAxis)
        .selectAll('text')
        .style('font-size', window.yTickFontSize);

    const maxTickWidth = d3.max(tempSvg.selectAll('.tick text').nodes(), d => d.getBBox().width);
    tempSvg.remove();

    margin.left = Math.ceil(maxTickWidth + 20);

    return margin
}

window.parent.document.addEventListener('dataLoaded', function() {
    buttonDownload.removeEventListener('click', handleDownloadClick); 
    buttonDownload.addEventListener('click', handleDownloadClick);

    buttonReset.removeEventListener('click', handleResetClick);
    buttonReset.addEventListener('click', handleResetClick);

    plotManagerDiskUsage();
});

window.addEventListener('resize', _.debounce(() => plotManagerDiskUsage(), 300));
