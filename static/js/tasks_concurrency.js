
import { downloadSVG } from './tools.js';


const buttonReset = document.getElementById('button-reset-tasks-concurrency');
const buttonDownload = document.getElementById('button-download-tasks-concurrency');

const svgElement = d3.select('#tasks-concurrency-svg');
const svgContainer = document.getElementById('tasks-concurrency-container');

var lineStrokeWidth = 1;
const lineColor = 'steelblue';
var dotRadius = lineStrokeWidth;
const dotColor = 'steelblue';
const tooltip = document.getElementById('vine-tooltip');

function plotTaskConcurrency() {
    if (!window.taskConcurrency) {
        return;
    }

    svgElement.selectAll('*').remove();

    const margin = {top: 20, right: 20, bottom: 40, left: 60};
    const svgWidth = svgContainer.clientWidth - margin.left - margin.right;
    const svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    var taskConcurrency = window.taskConcurrency;
    taskConcurrency.forEach(function(d) {
        d.time = +d.time;
        d.concurrent_tasks = +d.concurrent_tasks;
    });

    const maxConcurrentTasks = d3.max(taskConcurrency, d => d.concurrent_tasks);

    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append("g")
        .attr('transform', `translate(${margin.left}, ${margin.top})`);
    
    const xScale = d3.scaleLinear()
        .domain([0, window.maxTime - window.minTime])
        .range([0, svgWidth]);
    const yScale = d3.scaleLinear()
        .domain([0, maxConcurrentTasks])
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
        .tickFormat(d3.format(window.xTickFormat));
    svg.append("g")
        .attr("transform", `translate(0,${svgHeight})`)
        .call(xAxis)
        .selectAll("text")
        .attr("font-size", window.xTickFontSize);
    let selectedTicks;
    if (maxConcurrentTasks <= 10) {
        selectedTicks = d3.range(0, maxConcurrentTasks + 1);
    } else {
        selectedTicks = [0, Math.round(maxConcurrentTasks / 3), Math.round((2 * maxConcurrentTasks) / 3), maxConcurrentTasks];
    }
    const yAxis = d3.axisLeft(yScale)
        .tickValues(selectedTicks)
        .tickFormat(d3.format("d"))
        .tickSizeOuter(0);
    svg.append("g")
        .call(yAxis)
        .attr("font-size", window.yTickFontSize);

    const line = d3.line()
        .x(d => xScale(d.time - window.minTime))
        .y(d => yScale(d.concurrent_tasks))
        .curve(d3.curveStepAfter);
    svg.append("path")
        .datum(taskConcurrency)
        .attr("fill", "none")
        .attr("stroke", lineColor)
        .attr("stroke-width", lineStrokeWidth)
        .attr("d", line);
    svg.selectAll("dot")
        .data(taskConcurrency)
        .enter().append("circle")
        .attr("class", "dot")
        .attr("r", dotRadius)
        .attr("cx", d => xScale(d.time - window.minTime))
        .attr("cy", d => yScale(d.concurrent_tasks))
        .attr("fill", dotColor)
        .on("mouseover", function(event, d) {
            d3.select(this)
                .attr("r", dotRadius + 2)
                .attr("fill", 'orange');
            tooltip.innerHTML = `
                <div>Time: ${(d.time - window.minTime).toFixed(2)}s</div>
                <div>Concurrent Tasks: ${d.concurrent_tasks}</div>
            `;
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on("mouseout", function() {
            d3.select(this)
                .attr("r", dotRadius)
                .attr("fill", dotColor);
            tooltip.style.visibility = 'hidden';
        });
}

function handleDownloadClick() {
    downloadSVG('tasks-concurrency-svg');
}
function handleResetClick() {

    plotTaskConcurrency();
}

window.parent.document.addEventListener('dataLoaded', function() {
    plotTaskConcurrency();

    buttonDownload.removeEventListener('click', handleDownloadClick);
    buttonDownload.addEventListener('click', handleDownloadClick);

    buttonReset.removeEventListener('click', handleResetClick);
    buttonReset.addEventListener('click', handleResetClick);
});

window.addEventListener('resize', _.debounce(() => plotTaskConcurrency(), 300));