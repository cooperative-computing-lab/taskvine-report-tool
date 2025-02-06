import { downloadSVG, getTaskInnerHTML } from './tools.js';
import { setupZoomAndScroll } from './tools.js';

const buttonReset = document.getElementById('button-reset-task-execution-details');
const buttonDownload = document.getElementById('button-download-task-execution-details');
const svgContainer = document.getElementById('execution-details-container');
const svgElement = d3.select('#execution-details');

const tooltip = document.getElementById('vine-tooltip');

const colors = {
    'workers': {
        'normal': 'lightgrey',
        'highlight': 'orange',
    },
    'waiting-to-execute-on-worker': {
        'normal': 'lightblue',
        'highlight': '#72bbb0',
    },
    'running-tasks': {
        'normal': 'steelblue',
        'highlight': 'orange',
    },
    'retrieving-tasks': {
        'normal': '#cc5a12',
        'highlight': 'orange',
    },
    'committing-tasks': {
        'normal': '#8327cf',
        'highlight': 'orange',
    },
    'failed-tasks': {
        'normal': '#ad2c23',
        'highlight': 'orange',
    },
    'recovery-tasks': {
        'normal': '#ea67a9',
        'highlight': 'orange',
    },
}

export function plotExecutionDetails() {
    if (!window.taskDone) {
        return;
    }

    const taskDone = window.taskDone;
    const taskFailedOnWorker = window.taskFailedOnWorker;

    let margin = calculateMargin();

    console.log('execution details margin', margin);

    const svgWidth = svgContainer.clientWidth - margin.left - margin.right;
    const svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    // remove the current svg
    svgElement.selectAll('*').remove();

    // initialize svg
    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const { xScale, yScale } = plotAxis(svg, svgWidth, svgHeight);

    ////////////////////////////////////////////
    // create rectanges for each worker
    const workerEntries = window.workerSummary.map(d => ({
        worker: d.worker_hash,
        Info: {
            worker_id: +d.worker_id,
            time_connected: +d.time_connected,
            time_disconnected: +d.time_disconnected,
            cores: +d.cores,
        }
    }));
    workerEntries.forEach(({ worker, Info }) => {
        let worker_id = Info.worker_id;
        const rect = svg.append('rect')
            .attr('x', xScale(+Info.time_connected - window.minTime))
            .attr('y', yScale(worker_id + '-' + Info.cores))
            .attr('width', xScale(+Info.time_disconnected - window.minTime) - xScale(+Info.time_connected - window.minTime))
            .attr('height', yScale.bandwidth() * Info.cores + (yScale.step() - yScale.bandwidth()) * (Info.cores - 1))
            .attr('fill', colors.workers.normal)
            .attr('opacity', 0.3)
            .on('mouseover', function(event, d) {
                d3.select(this)
                    .attr('fill', colors.workers.highlight);
                // show tooltip
                tooltip.innerHTML = `
                    cores: ${Info.cores}<br>
                    worker id: ${Info.worker_id}<br>
                    when connected: ${(Info.time_connected - window.minTime).toFixed(2)}s<br>
                    when disconnected: ${(Info.time_disconnected - window.minTime).toFixed(2)}s<br>
                    life time: ${(Info.time_disconnected - Info.time_connected).toFixed(2)}s<br>`;
                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mousemove', function(event) {
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mouseout', function(event, d) {
                d3.select(this)
                    .attr('fill', colors.workers.normal);
                // hide tooltip
                tooltip.style.visibility = 'hidden';
            });
    });
    ////////////////////////////////////////////

    ////////////////////////////////////////////
    // create rectange for each successful task (time extent: when_running ~ time_worker_start)
    svg.selectAll('.task-rect')
        .data(taskDone)
        .enter()
        .append('g')
        .each(function(d) {
            var g = d3.select(this);
        /*
            g.append('rect')
                .attr('class', 'committing-tasks')
                .attr('x', d => xScale(+d.when_running - window.minTime))
                .attr('y', d => yScale(d.worker_id + '-' + d.core_id))
                .attr('width', d => xScale(+d.time_worker_start) - xScale(+d.when_running))
                .attr('height', yScale.bandwidth())
                .attr('fill', function(d) {
                    return colors['committing-tasks'].normal;
                });
        */
            g.append('rect')
                .attr('class', 'running-tasks')
                .attr('x', d => xScale(+d.time_worker_start - window.minTime))
                .attr('y', d => yScale(d.worker_id + '-' + d.core_id))
                .attr('width', d => xScale(+d.time_worker_end) - xScale(+d.time_worker_start))
                .attr('height', yScale.bandwidth())
                .attr('fill', function(d) {
                    return d.is_recovery_task === true ? colors['recovery-tasks'].normal : colors['running-tasks'].normal;
                });
        /*
            g.append('rect')
                .attr('class', 'retrieving-tasks')
                .attr('x', d => xScale(+d.time_worker_end - window.minTime))
                .attr('y', d => yScale(d.worker_id + '-' + d.core_id))
                .attr('width', d => xScale(+d.when_done) - xScale(+d.time_worker_end))
                .attr('height', yScale.bandwidth())
                .attr('fill', function(d) {
                    return colors['retrieving-tasks'].normal;
                });
        */
        
        })
        .on('mouseover', function(event, d) {
            d3.select(this).selectAll('rect').each(function() {
                if (this.classList.contains('committing-tasks')) {
                    d3.select(this).attr('fill', colors['committing-tasks'].highlight);
                } else if (this.classList.contains('running-tasks')) {
                    d3.select(this).attr('fill', colors['running-tasks'].highlight);
                } else if (this.classList.contains('retrieving-tasks')) {
                    d3.select(this).attr('fill', colors['retrieving-tasks'].highlight);
                }
            });

            // show tooltip
            tooltip.innerHTML = getTaskInnerHTML(d);
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on('mousemove', function(event) {
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on('mouseout', function() {
            // hide tooltip
            tooltip.style.visibility = 'hidden';
            // restore color
            d3.select(this).selectAll('rect').each(function() {
                if (this.classList.contains('committing-tasks')) {
                    d3.select(this).attr('fill', colors['committing-tasks'].normal);
                } else if (this.classList.contains('running-tasks')) {
                    d3.select(this).attr('fill', function(d) {
                        return d.is_recovery_task === "True" ? colors['recovery-tasks'].normal : colors['running-tasks'].normal;
                    });
                } else if (this.classList.contains('retrieving-tasks')) {
                    d3.select(this).attr('fill', colors['retrieving-tasks'].normal);
                }
            });
        });

    ////////////////////////////////////////////

    ////////////////////////////////////////////
    // create rectange for each failed task (time extent: when_running ~ when_next_ready)
    svg.selectAll('.failed-tasks')
        .data(taskFailedOnWorker)
        .enter()
        .append('rect')
        .attr('class', 'failed-tasks')
        .attr('x', d => xScale(+d.when_running - window.minTime))
        .attr('y', d => yScale(d.worker_id + '-' + d.core_id))
        .attr('width', d => {
            const width = xScale(+d.when_next_ready) - xScale(+d.when_running);
            if (width < 0) {
                throw new Error(`Invalid width: ${width} for task_id ${(d.task_id)} try_id ${d.try_id} when_running ${+d.when_running} when_next_ready ${d.when_next_ready}`);
            }
            return width;
        })
        .attr('height', yScale.bandwidth())
        .attr('fill', colors['failed-tasks'].normal)
        .attr('opacity', 0.8)
        .on('mouseover', function(event, d) {
            // change color
            d3.select(this).attr('fill', colors['failed-tasks'].highlight);
            // show tooltip
            tooltip.innerHTML = `
                task id: ${d.task_id}<br>
                worker: ${d.worker_id} (core ${d.core_id})<br>
                execution time: ${(d.when_next_ready - d.when_running).toFixed(2)}s<br>
                input size: ${(d.size_input_mgr - 0).toFixed(4)}MB<br>
                when ready: ${(d.when_ready - window.minTime).toFixed(2)}s<br>
                when running: ${(d.when_running - window.minTime).toFixed(2)}s<br>
                when next ready: ${(d.when_next_ready - window.minTime).toFixed(2)}s<br>`;
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on('mouseout', function() {
            // restore color
            d3.select(this).attr('fill', colors['failed-tasks'].normal);
            // hide tooltip
            tooltip.style.visibility = 'hidden';
        });

    ////////////////////////////////////////////
}

function calculateMargin() {
    const margin = {top: 40, right: 30, bottom: 40, left: 30};
    const svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    const tempSvg = d3.select('#execution-details')
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const workerCoresMap = [];
    window.workerSummary.forEach(d => {
        for (let i = 1; i <= +d.cores; i++) {
            workerCoresMap.push(`${d.worker_id}-${i}`);
        }
    });

    const yScale = d3.scaleBand()
        .domain(workerCoresMap)
        .range([svgHeight, 0])
        .padding(0.1);

    const yAxis = d3.axisLeft(yScale)
        .tickSizeOuter(0);

    tempSvg.append('g').call(yAxis);

    const maxTickWidth = d3.max(tempSvg.selectAll('.tick text').nodes(), d => d.getBBox().width);
    tempSvg.remove();

    margin.left = Math.ceil(maxTickWidth + 15);

    return margin
}

function plotAxis(svg, svgWidth, svgHeight) {
    // set x scale
    const xScale = d3.scaleLinear()
        .domain([0, window.maxTime - window.minTime])
        .range([0, svgWidth]);
    // set y scale
    const workerCoresMap = [];
    window.workerSummary.forEach(d => {
        for (let i = 1; i <= +d.cores; i++) {
            workerCoresMap.push(`${d.worker_id}-${i}`);
        }
    });
    const yScale = d3.scaleBand()
        .domain(workerCoresMap)
        .range([svgHeight, 0])
        .padding(0.1);
    // draw x axis
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
        .style('font-size', window.xTickFontSize);

    // draw y axis
    const totalWorkers = window.workerSummary.length;
    const maxTicks = 5;
    const tickInterval = Math.ceil(totalWorkers / maxTicks);
    const selectedTicks = [];
    for (let i = totalWorkers - 1; i >= 0; i -= tickInterval) {
        selectedTicks.unshift(`${window.workerSummary[i].worker_id}-${window.workerSummary[i].cores}`);
    }
    const yAxis = d3.axisLeft(yScale)
        .tickValues(selectedTicks)
        .tickFormat(d => d.split('-')[0]);
    svg.append('g')
        .call(yAxis)
        .selectAll('text')
        .style('font-size', window.yTickFontSize);

    return { xScale, yScale };
}

function handleResetClick() {
    document.querySelector('#execution-details').style.width = '100%';
    document.querySelector('#execution-details').style.height = '100%';
    plotExecutionDetails();
}
function handleDownloadClick() {
    downloadSVG('execution-details');
}

function setLegend() {
    var legendCell = d3.select("#legend-running-tasks");
    legendCell.selectAll('*').remove();
    const cellWidth = legendCell.node().offsetWidth;
    const cellHeight = legendCell.node().offsetHeight;
    const svgWidth = cellWidth;
    const svgHeight = cellHeight;
    var rectWidth = svgWidth * 0.8;
    var rectHeight = svgHeight * 0.6;
    var rectX = 0;
    var rectY = (svgHeight - rectHeight) / 2;

    legendCell = d3.select("#legend-running-tasks");
    legendCell.selectAll('*').remove();
    var svg = legendCell
        .append("svg")
        .attr("width", svgWidth)
        .attr("height", svgHeight);
    svg.append("rect")
        .attr("width", rectWidth)
        .attr("height", rectHeight)
        .attr("x", rectX)
        .attr("y", rectY)
        .attr("fill", colors['running-tasks'].normal);

    legendCell = d3.select("#legend-failed-tasks");
    legendCell.selectAll('*').remove();
    var svg = legendCell
        .append("svg")
        .attr("width", svgWidth)
        .attr("height", svgHeight);
    svg.append("rect")
        .attr("width", rectWidth)
        .attr("height", rectHeight)
        .attr("x", rectX)
        .attr("y", rectY)
        .attr("fill", colors['failed-tasks'].normal);

    legendCell = d3.select("#legend-recovery-tasks");
    legendCell.selectAll('*').remove();
    var svg = legendCell
        .append("svg")
        .attr("width", svgWidth)
        .attr("height", svgHeight);
    svg.append("rect")
        .attr("width", rectWidth)
        .attr("height", rectHeight)
        .attr("x", rectX)
        .attr("y", rectY)
        .attr("fill", colors['recovery-tasks'].normal);

    legendCell = d3.select("#legend-workers");
    legendCell.selectAll('*').remove();
    var svg = legendCell
        .append("svg")
        .attr("width", svgWidth)
        .attr("height", svgHeight);
    svg.append("rect")
        .attr("width", rectWidth)
        .attr("height", rectHeight)
        .attr("x", rectX)
        .attr("y", rectY)
        .attr("fill", colors['workers'].normal);
}


window.parent.document.addEventListener('dataLoaded', function() {
    if (!window.managerInfo) {
        return;
    }
    if (window.managerInfo.failed === 'True') {
        document.getElementById('execution-details-tip').style.visibility = 'visible';
    } else {
        document.getElementById('execution-details-tip').style.visibility = 'hidden';
    }
    setLegend();

    buttonDownload.removeEventListener('click', handleDownloadClick); 
    buttonDownload.addEventListener('click', handleDownloadClick);

    buttonReset.removeEventListener('click', handleResetClick);
    buttonReset.addEventListener('click', handleResetClick);

    plotExecutionDetails();
    setupZoomAndScroll('#execution-details', '#execution-details-container');
});

window.addEventListener('resize', _.debounce(() => plotExecutionDetails(), 300));