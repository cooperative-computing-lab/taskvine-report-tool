import { downloadSVG } from './tools.js';

const buttonDownload = document.getElementById('button-download-worker-concurrency');
const buttonReset = document.getElementById('button-reset-worker-concurrency');
const tooltip = document.getElementById('vine-tooltip');

const svgElement = d3.select('#worker-concurrency');
const svgContainer = document.getElementById('worker-concurrency-container');


async function plotWorkerConnections() {
    if (!window.workerConcurrency) {
        return;
    }

    const data = window.workerConcurrency;
    const minTime = window.time_manager_start;

    const margin = {top: 40, right: 30, bottom: 40, left: 30};

    const svgWidth = svgContainer.clientWidth - margin.left - margin.right;
    const svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    // first remove the current svg
    svgElement.selectAll('*').remove();
    // initialize svg
    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    data.forEach(function(d) {
        d.time = +d.time;
        d.concurrent_workers = +d.concurrent_workers;
    });
    const maxParallelWorkers = d3.max(data, d => d.concurrent_workers);

    const xScale = d3.scaleLinear()
        .domain([0, d3.max(data, d => d.time - minTime)])
        .range([0, svgWidth]);

    const yScale = d3.scaleLinear()
        .domain([0, maxParallelWorkers])
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
                    .tickFormat(d3.format(".1f"));
    svg.append("g")
        .attr("transform", `translate(0,${svgHeight})`)
        .call(xAxis)
        .selectAll("text")
        .style("text-anchor", "end");
    
    let selectedTicks;
    if (maxParallelWorkers <= 3) {
        selectedTicks = d3.range(0, maxParallelWorkers + 1);
    } else {
        selectedTicks = [0, Math.round(maxParallelWorkers / 3), Math.round((2 * maxParallelWorkers) / 3), maxParallelWorkers];
    }
    const yAxis = d3.axisLeft(yScale)
                    .tickValues(selectedTicks)
                    .tickFormat(d3.format("d"))
                    .tickSizeOuter(0);
    
    svg.append("g")
        .call(yAxis);

    const line = d3.line()
        .x(d => xScale(d.time - minTime))
        .y(d => yScale(d.concurrent_workers))
        .curve(d3.curveStepAfter);

    svg.append("path")
        .datum(data)
        .attr("fill", "none")
        .attr("stroke", "#5aa4ae")
        .attr("stroke-width", 1.5)
        .attr("d", line);

    svg.selectAll("circle")
        .data(data)
        .enter()
        .append("circle")
        .attr("cx", d => xScale(d.time - minTime))
        .attr("cy", d => yScale(d.concurrent_workers))
        .attr("r", 1)
        .attr("fill", "#145ca0");

    svgElement.on("mousemove", function(event) { 
        const [mouseX, mouseY] = d3.pointer(event, this);
        const positionX = xScale.invert(mouseX - margin.left);
        const positionY = yScale.invert(mouseY - margin.top);

        let minDistance = Infinity;
        let closestPoint = null;

        data.forEach(point => {
            const pointX = point['time'] - minTime;
            const pointY = point['concurrent_workers'];
            const distance = Math.sqrt((pointX - positionX) ** 2 + (pointY - positionY) ** 2);
            if (distance < minDistance) {
                minDistance = distance;
                closestPoint = point;
            }
        });
        if (closestPoint) {
            const pointX = xScale(closestPoint['time'] - minTime);
            const pointY = yScale(closestPoint['concurrent_workers']);

            tooltip.innerHTML = `
                Time: ${(closestPoint.time - minTime).toFixed(2)} s<br>
                Parallel Workers: ${closestPoint.concurrent_workers}<br>
                Event: ${closestPoint.event}<br>
                Worker ID: ${closestPoint.worker_id}
            `;
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (pointY + margin.top + svgContainer.getBoundingClientRect().top + window.scrollY + 5) + 'px';
            tooltip.style.left = (pointX + margin.left + svgContainer.getBoundingClientRect().left + window.scrollX + 5) + 'px';
        }
    });
    svgElement.on("mouseout", function() {
        document.getElementById('vine-tooltip').style.visibility = 'hidden';
    });

}

function handleDownloadClick() {
    downloadSVG('worker-concurrency');
}
function handleResetClick() {
    plotWorkerConnections();
}
window.parent.document.addEventListener('dataLoaded', function() {
    plotWorkerConnections();

    buttonDownload.removeEventListener('click', handleDownloadClick); 
    buttonDownload.addEventListener('click', handleDownloadClick);

    buttonReset.removeEventListener('click', handleResetClick);
    buttonReset.addEventListener('click', handleResetClick);
});

window.addEventListener('resize', _.debounce(() => plotWorkerConnections(), 300));