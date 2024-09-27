/*
<div class="report-toolbox">
    <button id="button-reset-task-execution-time-distribution" class="report-button">Reset</button>
    <button id="button-download-task-execution-time-distribution" class="report-button">Download SVG</button>
</div>
<div id="task-execution-time-distribution-container" class="container-alpha" >
    <svg id="task-execution-time-distribution-svg" xmlns="http://www.w3.org/2000/svg">
    </svg>
</div>
*/

import { downloadSVG, setupZoomAndScroll } from './tools.js';

const sortContainer = document.getElementById('task-execution-time-distribution-sort-category');

const buttonReset = document.getElementById('button-reset-task-execution-time-distribution');
const buttonDisplayCDF = document.getElementById('button-execution-time-distribution-display-cdf');
const buttonDownload = document.getElementById('button-download-task-execution-time-distribution');
const svgContainer = document.getElementById('task-execution-time-distribution-container');
const svgElement = d3.select('#task-execution-time-distribution-svg');

var dotRadius = 3;
const lineColor = "#145ca0";
const dotColor = "red";
const highlightColor = "orange";
const tooltip = document.getElementById('vine-tooltip');

const margin = {top: 40, right: 50, bottom: 40, left: 50};
var svgWidth = svgContainer.clientWidth - margin.left - margin.right;
var svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;
var taskDone = '';

export function plotTaskExecutionTimeDistribution({displayCDF = false} = {}) {
    if (!window.taskDone) {
        return;
    }

    svgElement.selectAll('*').remove();
    svgWidth = svgContainer.clientWidth - margin.left - margin.right;
    svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    // Initialize the SVG
    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    taskDone = window.taskDone;

    // update dotRadius based on the number of tasks
    if (taskDone.length > 500 && taskDone.length <= 1000) {
        dotRadius = 2;
    } else if (taskDone.length > 1000) {
        dotRadius = 1;
    }

    if (displayCDF) {
        cdfplot(svg);
    } else {
        scatterplot(svg);
    }
}

function cdfplot(svg) {
    taskDone.sort((a, b) => a.execution_time - b.execution_time);

    const cdfData = taskDone.map((d, i) => ({
        execution_time: d.execution_time,
        probability: (i + 1) / taskDone.length
    }));

    const xScale = d3.scaleLinear()
        .domain([0, d3.max(cdfData, d => d.execution_time)])
        .range([0, svgWidth]);

    const yScale = d3.scaleLinear()
        .domain([0, 1])
        .range([svgHeight, 0]);

    const xAxis = d3.axisBottom(xScale)
                    .tickSizeOuter(0)
                    .tickValues([
                        xScale.domain()[0],
                        xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.25,
                        xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.5,
                        xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.75,
                        xScale.domain()[1]
                    ])
                    .tickFormat(d3.format(".2f"));
    const yAxis = d3.axisLeft(yScale)
                    .tickSizeOuter(0)
                    .tickValues([
                        yScale.domain()[0],
                        yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.25,
                        yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.5,
                        yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.75,
                        yScale.domain()[1]
                    ])
                    .tickFormat(d3.format(".2f"));

    svg.append("g")
        .attr("transform", `translate(0,${svgHeight})`)
        .call(xAxis);
    svg.append("g")
        .call(yAxis)
        .selectAll("text")
        .attr("font-size", window.yTickFontSize);

    // line
    const lineGenerator = d3.line()
        .x(d => xScale(d.execution_time))
        .y(d => yScale(d.probability));
    svg.append("path")
        .datum(cdfData)
        .attr("fill", "none")
        .attr("stroke", lineColor)
        .attr("stroke-width", 2)
        .attr("d", lineGenerator);

    // points
    svg.selectAll(".dot")
        .data(cdfData)
        .enter().append("circle")
        .attr("class", "dot")
        .attr("cx", d => xScale(d.execution_time))
        .attr("cy", d => yScale(d.probability))
        .attr("r", dotRadius)
        .attr("fill", dotColor)
        .on("mouseover", function(event, d) {
            d3.select(this)
                .attr('r', dotRadius * 2)
                .attr('fill', highlightColor);
            tooltip.innerHTML = `
                Execution Time: ${d.execution_time.toFixed(4)}s<br>
                Cumulative Probability: ${(d.probability * 100).toFixed(2)}%
            `;
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on("mouseout", function() {
            d3.select(this)
                .attr('r', dotRadius)
                .attr('fill', dotColor);
            tooltip.style.visibility = 'hidden';
        });
}


function scatterplot(svg) {
    const maxExecutionTime = d3.max(taskDone, d => d.execution_time);
    
    const xScale = d3.scaleLinear()
        .domain([0, d3.max(taskDone, d => d.task_id)])
        .range([0, svgWidth]);
    const xAxis = d3.axisBottom(xScale)
                    .tickSizeOuter(0)
                    .tickValues([
                        xScale.domain()[0],
                        xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.25,
                        xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.5,
                        xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.75,
                        xScale.domain()[1]
                    ])
                    .tickFormat(d3.format(".0f"));
    svg.append("g")
        .attr("transform", `translate(0,${svgHeight})`)
        .call(xAxis)
        .selectAll("text")
        .style("text-anchor", "end")
        .attr("font-size", window.xTickFontSize);

    const yScale = d3.scaleLinear()
        .domain([0, maxExecutionTime])
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
                    .tickFormat(d3.format(".2f"));
    svg.append("g")
        .call(yAxis)
        .selectAll("text")
        .attr("font-size", window.yTickFontSize);

    const points = svg.selectAll(".point")
    .data(taskDone)
        .enter()
        .append("circle")
        .classed("point", true)
        .attr("cx", d => xScale(d.task_id))
        .attr("cy", d => yScale(d.execution_time))
        .attr("r", dotRadius)
        .style("fill", lineColor);

    points.on("mouseover", function(event, d) {
        d3.select(this)
            .attr('r', 2)
            .style("fill", highlightColor);
        tooltip.innerHTML = `
            Task ID: ${d.task_id}<br>
            Execution Time: ${d.execution_time.toFixed(2)}s<br
        `;
        tooltip.style.visibility = 'visible';
        tooltip.style.top = (event.pageY + 10) + 'px';
        tooltip.style.left = (event.pageX + 10) + 'px';
    })
        .on("mouseout", function(d) {
            d3.select(this)
                .attr('r', dotRadius)
                .style("fill", lineColor);
            tooltip.style.visibility = 'hidden';
        });
}

function handleDownloadClick() {
    downloadSVG('task-execution-time-distribution-svg');
}
function handleResetClick() {
    document.querySelector('#task-execution-time-distribution-svg').style.width = '100%';
    document.querySelector('#task-execution-time-distribution-svg').style.height = '100%';
    buttonDisplayCDF.classList.remove('report-button-active');
    plotTaskExecutionTimeDistribution({displayCDF: false});
}
function handleDisplayCDFClick() {
    this.classList.toggle('report-button-active');
    plotTaskExecutionTimeDistribution({displayCDF: this.classList.contains('report-button-active')});
}

window.parent.document.addEventListener('dataLoaded', function() {
    plotTaskExecutionTimeDistribution({displayCDF: false});
    buttonDisplayCDF.classList.remove('report-button-active');
    setupZoomAndScroll('#task-execution-time-distribution-svg', '#task-execution-time-distribution-container');

    buttonDownload.removeEventListener('click', handleDownloadClick); 
    buttonDownload.addEventListener('click', handleDownloadClick);

    buttonReset.removeEventListener('click', handleResetClick);
    buttonReset.addEventListener('click', handleResetClick);

    buttonDisplayCDF.removeEventListener('click', handleDisplayCDFClick);
    buttonDisplayCDF.addEventListener('click', handleDisplayCDFClick);
});

window.addEventListener('resize', _.debounce(() => plotTaskExecutionTimeDistribution({
    displayCDF: buttonDisplayCDF.classList.contains('report-button-active'),
}), 300));