/*
<h2 id="tasks-category-information-title">Tasks Category Information</h2>
<div id="task-category-information-preface" class="preface">
</div>
<div class="report-toolbox">
    <button id="button-reset-task-category-information" class="report-button">Reset</button>
    <button id="button-task-category-information-sort-by-avg-time" class="report-button">Sort by Avg Execution Time</button>
    <button id="button-download-task-category-information" class="report-button">Download SVG</button>
</div>
<div id="task-category-information-container" class="container-alpha" >
    <svg id="task-category-information-svg" xmlns="http://www.w3.org/2000/svg">
    </svg>
</div>
*/

import { downloadSVG } from './tools.js';

const buttonReset = document.getElementById('button-reset-task-category-information');
const buttonDownload = document.getElementById('button-download-task-category-information');
const buttonSortByAvgExecutionTime = document.getElementById('button-task-category-information-sort-by-avg-time');
const svgContainer = document.getElementById('task-category-information-container');
const svgElement = d3.select('#task-category-information-svg');

const barColor = "#065fae";
const lineAvgExecutionTimeColor = "#f0be41";
const lineMaxExecutionTimeColor = "#8c1a11";
const lineStrokeWidth = 1;
const highlightColor = "orange";
const tooltip = document.getElementById('vine-tooltip');
const maxBarWidth = 50;

export function plotTaskCategoryInformation({ sortByAvgExecutionTime = false } = {}) {
    if (!window.categoryInfo) {
        return;
    }

    svgElement.selectAll('*').remove();
    const margin = {top: 40, right: 80, bottom: 40, left: 80};
    const svgWidth = svgContainer.clientWidth - margin.left - margin.right;
    const svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const categoryInfo = window.categoryInfo;
    categoryInfo.forEach(function(d) {
        d.id = +d.id;
        d.num_tasks = +d.num_tasks;
        d['total_task_execution_time(s)'] = +d['total_task_execution_time(s)'];
        d['avg_task_execution_time(s)'] = +d['avg_task_execution_time(s)'];
        d['max_task_execution_time(s)'] = +d['max_task_execution_time(s)'];
        d['min_task_execution_time(s)'] = +d['min_task_execution_time(s)'];
    });
    const maxExecutionTime = d3.max(categoryInfo, d => d['max_task_execution_time(s)']);
    const maxAvgExecutionTime = d3.max(categoryInfo, d => d['avg_task_execution_time(s)']);
    const maxNumTasks = d3.max(categoryInfo, d => d.num_tasks);

    if (sortByAvgExecutionTime) {
        categoryInfo.sort((a, b) => a['avg_task_execution_time(s)'] - b['avg_task_execution_time(s)']);
    } else {
        categoryInfo.sort((a, b) => a.num_tasks - b.num_tasks);
    }
    // Reassign category IDs based on sorted order
    categoryInfo.forEach((d, i) => d.sortID = i + 1);
    
    // Setup scaleBand and xAxis with dynamic tick values
    const sortIDs = categoryInfo.map(d => d.sortID);
    let tickValues = [];
    if (sortIDs.length >= 4) {
        tickValues = [
            sortIDs[0],  // First element
            sortIDs[Math.floor((sortIDs.length - 1) / 3)],  // One-third
            sortIDs[Math.floor((sortIDs.length - 1) * 2 / 3)],  // Two-thirds
            sortIDs[sortIDs.length - 1]  // Last element
        ];
    } else {
        tickValues = sortIDs;
    }
    const xScale = d3.scaleBand()
        .domain(sortIDs)
        .range([0, svgWidth])
        .padding(0.2);

    const xAxis = d3.axisBottom(xScale)
        .tickSizeOuter(0)
        .tickValues(tickValues)
        .tickFormat(d => categoryInfo.find(e => e.sortID === d).sortID);

    // Append and transform the x-axis on the SVG
    svg.append("g")
        .attr("transform", `translate(0,${svgHeight})`)
        .call(xAxis)
        .selectAll("text")
        .style("text-anchor", "end");

    // Setup yScale and yAxis
    const yScaleLeft = d3.scaleLinear()
        .domain([0, maxNumTasks])
        .range([svgHeight, 0]);
    const yAxisLeft = d3.axisLeft(yScaleLeft)
        .tickSizeOuter(0)
        .tickValues([
            yScaleLeft.domain()[0],
            yScaleLeft.domain()[0] + (yScaleLeft.domain()[1] - yScaleLeft.domain()[0]) * 0.25,
            yScaleLeft.domain()[0] + (yScaleLeft.domain()[1] - yScaleLeft.domain()[0]) * 0.5,
            yScaleLeft.domain()[0] + (yScaleLeft.domain()[1] - yScaleLeft.domain()[0]) * 0.75,
            yScaleLeft.domain()[1]
        ])
        .tickFormat(d => d.toString());
    svg.append("g")
        .call(yAxisLeft);
    const yScaleRight = d3.scaleLinear()
        .domain([0, maxExecutionTime])
        .range([svgHeight, 0]);
    const yAxisRight = d3.axisRight(yScaleRight)
        .tickSizeOuter(0)
        .tickValues([
            yScaleRight.domain()[0],
            yScaleRight.domain()[0] + (yScaleRight.domain()[1] - yScaleRight.domain()[0]) * 0.25,
            yScaleRight.domain()[0] + (yScaleRight.domain()[1] - yScaleRight.domain()[0]) * 0.5,
            yScaleRight.domain()[0] + (yScaleRight.domain()[1] - yScaleRight.domain()[0]) * 0.75,
            yScaleRight.domain()[1]
        ])
        .tickFormat(d3.format(".2f"));
    svg.append("g")
        .attr("transform", `translate(${svgWidth}, 0)`)
        .call(yAxisRight);

    ////////////////////////////////////////////////////////////
    // labels
    svg.append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", (0 - margin.left) * (2 / 3))
        .attr("x", 0 - (svgHeight / 2))
        .attr("dy", "1em")
        .style("text-anchor", "middle")
        .style("font-size", "14px")
        .text("Task Count");
    svg.append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", svgWidth + margin.right * (2 / 3))
        .attr("x", 0 - (svgHeight / 2))
        .attr("dy", "1em")
        .style("text-anchor", "middle")
        .style("font-size", "14px")
        .text("Execution Time (s)");

    ////////////////////////////////////////////////////////////
    // legend
    const legendData = [
        { color: barColor, label: "TaskCount", type: "line" },
        { color: lineAvgExecutionTimeColor, label: "Average Execution Time", type: "line" },
        { color: lineMaxExecutionTimeColor, label: "Max Execution Time", type: "line" },
    ];
    const legendX = 10;
    const legendY = 0;
    const legendWidth = 50;
    const legendSpacing = 20;

    const legend = svg.append("g")
        .attr("class", "legend")
        .attr("transform", `translate(${legendX},${legendY})`);
    legend.selectAll(".legend-item")
        .data(legendData)
        .enter()
        .append("g")
        .attr("class", "legend-item")
        .attr("transform", (d, i) => `translate(0, ${i * legendSpacing})`)
        .each(function(d) {
            if (d.type === "line") {
                d3.select(this).append("line")
                    .attr("x1", 0)
                    .attr("y1", 0)
                    .attr("x2", legendWidth)
                    .attr("y2", 0)
                    .attr("stroke", d.color)
                    .attr("stroke-width", 2);
            } else if (d.type === "bar") {
                d3.select(this).append("rect")
                    .attr("x", 0)
                    .attr("y", -5)
                    .attr("width", legendWidth)
                    .attr("height", 10)
                    .attr("fill", d.color);
            }
            d3.select(this).append("text")
                .attr("x", legendX + legendWidth + 2)
                .attr("y", 0)
                .attr("dy", "0.35em")
                .style("fill", d.color)
                .style("font-weight", "bold")
                .style("text-anchor", "start")
                .style("font-size", "14px")
                .text(d.label);
        });

    var lineGenerator = d3.line()
        .x(d => xScale(d.sortID) + xScale.bandwidth() / 2)
        .y(d => yScaleRight(d['avg_task_execution_time(s)']));
    svg.append("path")
        .datum(categoryInfo)
        .attr("fill", "none")
        .attr("stroke", lineAvgExecutionTimeColor)
        .attr("stroke-width", lineStrokeWidth)
        .attr("d", lineGenerator);

    lineGenerator = d3.line()
        .x(d => xScale(d.sortID) + xScale.bandwidth() / 2)
        .y(d => yScaleRight(d['max_task_execution_time(s)']));
    svg.append("path")
        .datum(categoryInfo)
        .attr("fill", "none")
        .attr("stroke", lineMaxExecutionTimeColor)
        .attr("stroke-width", lineStrokeWidth)
        .attr("d", lineGenerator);

    lineGenerator = d3.line()
        .x(d => xScale(d.sortID) + xScale.bandwidth() / 2)
        .y(d => yScaleLeft(d.num_tasks));
    svg.append("path")
        .datum(categoryInfo)
        .attr("fill", "none")
        .attr("stroke", barColor)
        .attr("stroke-width", lineStrokeWidth)
        .attr("d", lineGenerator);
}

function handleSortByAvgExecutionTimeClick() {
    this.classList.toggle('report-button-active');
    plotTaskCategoryInformation({sortByAvgExecutionTime: this.classList.contains('report-button-active')});
}
function handleDownloadClick() {
    downloadSVG('task-category-information-svg');
}
function handleResetClick() {
    if (buttonSortByAvgExecutionTime.classList.contains('report-button-active')) {
        buttonSortByAvgExecutionTime.classList.remove('report-button-active');
    }
    plotTaskCategoryInformation({sortByAvgExecutionTime: false});
}

window.parent.document.addEventListener('dataLoaded', function() {
    buttonSortByAvgExecutionTime.classList.remove('report-button-active');
    plotTaskCategoryInformation({sortByAvgExecutionTime: false});

    buttonDownload.removeEventListener('click', handleDownloadClick);
    buttonDownload.addEventListener('click', handleDownloadClick);

    buttonReset.removeEventListener('click', handleResetClick);
    buttonReset.addEventListener('click', handleResetClick);

    buttonSortByAvgExecutionTime.removeEventListener('click', handleSortByAvgExecutionTimeClick);
    buttonSortByAvgExecutionTime.addEventListener('click', handleSortByAvgExecutionTimeClick);
});

window.addEventListener('resize', _.debounce(() => plotTaskCategoryInformation({
    sortByAvgExecutionTime: buttonSortByAvgExecutionTime.classList.contains('report-button-active'),
}), 300));