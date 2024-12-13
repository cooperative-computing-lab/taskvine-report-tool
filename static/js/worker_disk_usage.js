import { formatUnixTimestamp, downloadSVG } from './tools.js';

const buttonReset = document.getElementById('button-worker-disk-usage-reset');
const buttonDownload = document.getElementById('button-download-worker-disk-usage');
const buttonDisplayPercentages = document.getElementById('button-display-worker-disk-usage-by-percentage');
const buttonDisplayAccumulatedOnly = document.getElementById('button-display-accumulated-only');
const buttonAnalyzeWorker = document.getElementById('button-highlight-worker-disk-usage');

const svgElement = d3.select('#worker-disk-usage');
const svgContainer = document.getElementById('worker-disk-usage-container');

const tooltip = document.getElementById('vine-tooltip');

let maxDiskUsage;

function plotWorkerDiskUsage({ displayDiskUsageByPercentage = false, highlightWorkerID = null, displayAccumulationOnly = false } = {}) {
    if (!window.workerDiskUpdate) {
        console.log('workerDiskUpdate is not available');
        return;
    }

    // first remove all the elements in the svg
    svgElement.selectAll('*').remove();

    const groupedworkerDiskUpdate = d3.group(window.workerDiskUpdate, d => d.worker_id);
    
    // get the window.minTime, window.maxTime and maxDiskUsage
    let columnNameMB = 'disk_usage(MB)';
    let columnNamePercentage = 'disk_usage(%)';
    if (displayAccumulationOnly) {
        columnNameMB = 'disk_usage_accumulation(MB)';
        columnNamePercentage = 'disk_usage_accumulation(%)';
    }

    if (displayDiskUsageByPercentage) {
        maxDiskUsage = d3.max(window.workerDiskUpdate, function(d) { return +d[columnNamePercentage]; });
    } else {
        maxDiskUsage = d3.max(window.workerDiskUpdate, function(d) { return +d[columnNameMB]; });
    }

    const margin = calculateMargin();
    console.log('worker disk usage margin', margin);

    const svgWidth = svgContainer.clientWidth - margin.left - margin.right;
    const svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append("g")
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    // Setup scales
    const xScale = d3.scaleLinear()
        .domain([0, window.maxTime - window.minTime])
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
        .tickFormat(d3.format(window.xTickFormat));
    svg.append('g')
        .attr('transform', `translate(0, ${svgHeight})`)
        .call(xAxis)
        .selectAll('text')
        .style('font-size', window.xTickFontSize);

    const yScale = d3.scaleLinear()
        .domain([0, maxDiskUsage])
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
        .tickFormat(displayDiskUsageByPercentage === true ? d3.format(".4f") : d3.format(".2f"));
    svg.append('g')
        .call(yAxis)
        .selectAll('text')
        .style('font-size', window.yTickFontSize);

    // Create line generator
    const line = d3.line()
        .x(d => {
            if (d.when_stage_in_or_out - window.minTime < 0) {
                console.log('d.when_stage_in_or_out - window.minTime < 0, \
                    d.when_stage_in_or_out = ', d.when_stage_in_or_out, 'window.minTime = ', window.minTime);
                console.log('d.filename = ', d.filename, ' d[size(MB)] = ', d['size(MB)']);
            }
            if (isNaN(d.when_stage_in_or_out - window.minTime)) {
                console.log('d.when_stage_in_or_out - window.minTime is NaN', d);
            }
            return xScale(d.when_stage_in_or_out - window.minTime);
        })
        .y(d => {
            const diskUsage = displayDiskUsageByPercentage 
                ? d[columnNamePercentage]
                : d[columnNameMB];
            if (isNaN(diskUsage)) {
                console.log('diskUsage is NaN', d);
            }
            return yScale(diskUsage);
        })
        .curve(d3.curveStepAfter);

    // Draw accumulated disk usage
    groupedworkerDiskUpdate.forEach((value, key) => {
        key = +key;
        let lineColor;
        let strokeWidth = 0.8;
        if (highlightWorkerID !== null && key === highlightWorkerID) {
            lineColor = 'orange';
            strokeWidth = 2;
        } else if (highlightWorkerID !== null) {
            lineColor = 'lightgray';
        } else {
            lineColor = d3.schemeCategory10[key % 10];   // Assign color using a categorical scheme
        }
        const path = svg.append("path")
            .datum(value)
            .attr("class", "line")
            .attr("fill", "none")
            .attr("stroke", lineColor)
            .attr("original-color", lineColor)
            .attr("stroke-width", strokeWidth)
            .attr("original-stroke-width", strokeWidth)
            .attr("d", line);
        
        if (highlightWorkerID === null) {
            path.on("mouseover", function(event, d) {
                // change color
                d3.selectAll("path.line").attr("stroke", "lightgray");
                d3.select(this)
                    .raise()
                    .attr("stroke", "orange")
                    .attr("stroke-width", 2);
                // show tooltip
                const svgRect = svg.node().getBoundingClientRect();
                const xPosition = (event.clientX - svgRect.left) * (svgWidth / svgRect.width);
                const yPosition = (event.clientY - svgRect.top) * (svgHeight / svgRect.height);
                const xValue = xScale.invert(xPosition);
                const yValue = yScale.invert(yPosition);
                tooltip.innerHTML = `
                    worker id: ${key}<br>
                    event time: ${xValue.toFixed(2)}<br>
                    disk usage: ${yValue.toFixed(2)}<br>
                `;
                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on("mousemove", function(event) {
                const svgRect = svg.node().getBoundingClientRect();
                const xPosition = (event.clientX - svgRect.left) * (svgWidth / svgRect.width);
                const yPosition = (event.clientY - svgRect.top) * (svgHeight / svgRect.height);
                const xValue = xScale.invert(xPosition);
                const yValue = yScale.invert(yPosition);
                tooltip.innerHTML = `
                    worker id: ${key}<br>
                    event time: ${xValue.toFixed(2)}s<br>
                    disk usage: ${yValue.toFixed(2)}<br>
                `;

                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on("mouseout", function(d) {
                // remove highlight
                d3.selectAll("path.line").each(function() {
                    const originalColor = d3.select(this).attr("original-color");
                    d3.select(this).attr("stroke", originalColor);
                });
                const originalColor = d3.select(this).attr("original-color");
                d3.select(this)
                    .attr("stroke", originalColor)
                    .attr("stroke-width", 0.8);
                // hide tooltip
                tooltip.style.visibility = 'hidden';
            });
        }
    });
    
    // traverse the lines and raise the highlighted line
    let highlightedLine = null;
    if (highlightWorkerID) {
        d3.selectAll("path.line").each(function() {
            const workerID = +d3.select(this).datum()[0].worker_id;
            if (workerID === highlightWorkerID) {
                d3.select(this).raise();
                highlightedLine = d3.select(this);
            }
        });
    }

    if (highlightedLine) {
        svgElement.on("mousemove", function(event) {
            const [mouseX, mouseY] = d3.pointer(event, this);
            const positionX = xScale.invert(mouseX - margin.left);
            const positionY = yScale.invert(mouseY - margin.top);
    
            let minDistance = Infinity;
            let closestPoint = null;
    
            const lineData = highlightedLine.datum();

            lineData.forEach(point => {
                const pointX = point['when_stage_in_or_out'] - window.minTime;
                const pointY = point['disk_usage(MB)'];
    
                const distance = Math.sqrt(Math.pow(positionX - pointX, 2) + Math.pow(positionY - pointY, 2));

                if (distance < minDistance) {
                    minDistance = distance;
                    closestPoint = point;
                }
            });
    
            if (closestPoint) {
                const pointX = xScale(closestPoint['when_stage_in_or_out'] - window.minTime);
                const pointY = yScale(closestPoint[displayDiskUsageByPercentage ? columnNamePercentage : columnNameMB]);
                tooltip.innerHTML = `
                    worker id: ${closestPoint.worker_id}<br>
                    worker hash: ${closestPoint.worker_hash}<br>
                    filename: ${closestPoint.filename}<br>
                    time from start: ${(+closestPoint.when_stage_in_or_out - window.minTime).toFixed(2)}s<br>
                    time in human: ${formatUnixTimestamp(+closestPoint.when_stage_in_or_out)}<br>
                    disk contribute: ${(+closestPoint['size(MB)']).toFixed(4)}MB<br>
                    disk usage: ${(+closestPoint[displayDiskUsageByPercentage ? columnNamePercentage : columnNameMB]).toFixed(2)}${displayDiskUsageByPercentage ? '%' : 'MB'}<br>
                    current cached files: ${closestPoint['current_cached_files']} <br>
                    history cached files: ${closestPoint['history_cached_files']} <br>
                `;

                tooltip.style.visibility = 'visible';
                tooltip.style.top = (pointY + margin.top + svgContainer.getBoundingClientRect().top + window.scrollY + 5) + 'px';
                tooltip.style.left = (pointX + margin.left + svgContainer.getBoundingClientRect().left + window.scrollX + 5) + 'px';
            }
        });
    } else {
        tooltip.style.visibility = 'hidden';
        svgElement.on("mousemove", null);
    }
}

buttonDisplayPercentages.addEventListener('click', async function() {
    this.classList.toggle('report-button-active');
    // first clean the plot
    svgElement.selectAll('*').remove();
    // get the highlight worker id
    let highlightWorkerID;
    if (buttonAnalyzeWorker.classList.contains('report-button-active')) {
        highlightWorkerID = getHighlightWorkerID();
    }
    plotWorkerDiskUsage({displayDiskUsageByPercentage: this.classList.contains('report-button-active'),
        highlightWorkerID: highlightWorkerID,
        displayAccumulationOnly: buttonDisplayAccumulatedOnly.classList.contains('report-button-active'),
    });
});

buttonDisplayAccumulatedOnly.addEventListener('click', async function() {
    this.classList.toggle('report-button-active');
    // first clean the plot
    svgElement.selectAll('*').remove();
    // get the highlight worker id
    let highlightWorkerID = null;
    if (buttonAnalyzeWorker.classList.contains('report-button-active')) {
        highlightWorkerID = getHighlightWorkerID();
    }
    plotWorkerDiskUsage({displayDiskUsageByPercentage: buttonDisplayPercentages.classList.contains('report-button-active'),
        highlightWorkerID: highlightWorkerID,
        displayAccumulationOnly: this.classList.contains('report-button-active'),
    });
});

function getHighlightWorkerID() {
    if (!window.workerDiskUpdate) {
        return null;
    }
    let workerID = document.getElementById('input-highlight-worker-disk-usage').value;
    if (!window.workerDiskUpdate.some(d => +d.worker_id === +workerID)) {
        workerID = null;
    } else {
        workerID = +workerID;
    }
    return workerID;
}

buttonAnalyzeWorker.addEventListener('click', async function() {
    // get the highlight worker id
    let workerID = getHighlightWorkerID();
    if (this.classList.contains('report-button-active')) {
        if (workerID === null) {
            this.classList.toggle('report-button-active');
        }
    } else if (!this.classList.contains('report-button-active') && workerID !== null) {
        this.classList.toggle('report-button-active');
    }

    // get the percentage button
    plotWorkerDiskUsage({displayDiskUsageByPercentage: buttonDisplayPercentages.classList.contains('report-button-active'),
        highlightWorkerID: workerID,
        displayAccumulationOnly: buttonDisplayAccumulatedOnly.classList.contains('report-button-active'),
    });
});

function calculateMargin() {
    const margin = {top: 40, right: 30, bottom: 40, left: 30};
    const svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    const tempSvg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append("g")
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const yScale = d3.scaleLinear()
        .domain([0, maxDiskUsage])
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

    tempSvg.append('g')
        .call(yAxis)
        .selectAll('text')
        .style('font-size', window.yTickFontSize);

    const maxTickWidth = d3.max(tempSvg.selectAll('.tick text').nodes(), d => d.getBBox().width);
    tempSvg.remove();

    margin.left = Math.ceil(maxTickWidth + 20);

    return margin
}

function handleDownloadClick() {
    downloadSVG('worker-disk-usage');
}
function handleResetClick() {
    // deactivating the buttons
    buttonDisplayPercentages.classList.remove('report-button-active');
    buttonDisplayAccumulatedOnly.classList.remove('report-button-active');
    buttonAnalyzeWorker.classList.remove('report-button-active');
    plotWorkerDiskUsage({displayDiskUsageByPercentage: false});
}

window.parent.document.addEventListener('dataLoaded', function() {
    // deactivating the buttons
    buttonDisplayPercentages.classList.remove('report-button-active');
    buttonDisplayAccumulatedOnly.classList.remove('report-button-active');
    buttonAnalyzeWorker.classList.remove('report-button-active');

    buttonDownload.removeEventListener('click', handleDownloadClick); 
    buttonDownload.addEventListener('click', handleDownloadClick);

    buttonReset.removeEventListener('click', handleResetClick);
    buttonReset.addEventListener('click', handleResetClick);

    plotWorkerDiskUsage({displayDiskUsageByPercentage: false});
});

window.addEventListener('resize', _.debounce(() => plotWorkerDiskUsage({
    displayDiskUsageByPercentage: buttonDisplayPercentages.classList.contains('report-button-active'),
    highlightWorkerID: getHighlightWorkerID(),
    displayAccumulationOnly: buttonDisplayAccumulatedOnly.classList.contains('report-button-active'),
}), 300));

