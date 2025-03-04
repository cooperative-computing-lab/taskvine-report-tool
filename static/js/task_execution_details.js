import { downloadSVG } from './tools.js';
import { setupZoomAndScroll } from './tools.js';

const buttonReset = document.getElementById('button-reset-task-execution-details');
const buttonDownload = document.getElementById('button-download-task-execution-details');
const svgContainer = document.getElementById('execution-details-container');
const svgElement = d3.select('#execution-details');
const tooltip = document.getElementById('vine-tooltip');

const state = {
    successfulTasks: null,
    unsuccessfulTasks: null,
    workerInfo: null,
    xTickValues: null,
    tickFontSize: null,
};

// Global highlight color
const HIGHLIGHT_COLOR = 'orange';

// Define failure types with their bit values
const FAILURE_TYPES = {
    'VINE_RESULT_INPUT_MISSING': { value: 1, color: '#FFB6C1', label: 'Input Missing' },
    'VINE_RESULT_OUTPUT_MISSING': { value: 2, color: '#FF69B4', label: 'Output Missing' },
    'VINE_RESULT_STDOUT_MISSING': { value: 4, color: '#FF1493', label: 'Stdout Missing' },
    'VINE_RESULT_SIGNAL': { value: 1 << 3, color: '#CD5C5C', label: 'Signal' },
    'VINE_RESULT_RESOURCE_EXHAUSTION': { value: 2 << 3, color: '#8B0000', label: 'Resource Exhaustion' },
    'VINE_RESULT_MAX_END_TIME': { value: 3 << 3, color: '#B22222', label: 'Max End Time' },
    'VINE_RESULT_UNKNOWN': { value: 4 << 3, color: '#A52A2A', label: 'Unknown' },
    'VINE_RESULT_FORSAKEN': { value: 5 << 3, color: '#E331EE', label: 'Forsaken' },
    'VINE_RESULT_MAX_RETRIES': { value: 6 << 3, color: '#8B4513', label: 'Max Retries' },
    'VINE_RESULT_MAX_WALL_TIME': { value: 7 << 3, color: '#D2691E', label: 'Max Wall Time' },
    'VINE_RESULT_RMONITOR_ERROR': { value: 8 << 3, color: '#FF4444', label: 'Monitor Error' },
    'VINE_RESULT_OUTPUT_TRANSFER_ERROR': { value: 9 << 3, color: '#FF6B6B', label: 'Transfer Error' },
    'VINE_RESULT_FIXED_LOCATION_MISSING': { value: 10 << 3, color: '#FF8787', label: 'Location Missing' },
    'VINE_RESULT_CANCELLED': { value: 11 << 3, color: '#FFA07A', label: 'Cancelled' },
    'VINE_RESULT_LIBRARY_EXIT': { value: 12 << 3, color: '#FA8072', label: 'Library Exit' },
    'VINE_RESULT_SANDBOX_EXHAUSTION': { value: 13 << 3, color: '#E9967A', label: 'Sandbox Exhaustion' },
    'VINE_RESULT_MISSING_LIBRARY': { value: 14 << 3, color: '#F08080', label: 'Missing Library' },
    
    'WORKER_DISCONNECTED': { value: 15 << 3, color: '#FF0000', label: 'Worker Disconnected' }
};

// Update colors object
const colors = {
    'workers': 'lightgrey',
    'successful-committing-to-worker': '#4a4a4a',
    'successful-executing-on-worker': 'steelblue',
    'successful-retrieving-to-manager': '#cc5a12',
    'recovery-successful': '#FF69B4',
    'recovery-unsuccessful': '#E3314F',
    ...Object.fromEntries(Object.entries(FAILURE_TYPES).map(([key, value]) => [`unsuccessful-${key.toLowerCase()}`, value.color]))
};

// get the innerHTML of the task
function getTaskInnerHTML(task) {
    let htmlContent = `
        task id: ${task.task_id || 'N/A'}<br>
        worker:  ${task.worker_ip || 'N/A'}:${task.worker_port || 'N/A'}<br>
        core id: ${task.core_id || 'N/A'}<br>
        when ready: ${task.when_ready ? task.when_ready.toFixed(2) : 'N/A'}<br>
        when running: ${task.when_running ? task.when_running.toFixed(2) : 'N/A'}<br>
        num input files: ${task.num_input_files || 'N/A'}<br>
        num output files: ${task.num_output_files || 'N/A'}<br>
        time worker start: ${task.time_worker_start ? task.time_worker_start.toFixed(2) : 'N/A'}<br>
        time worker end: ${task.time_worker_end ? task.time_worker_end.toFixed(2) : 'N/A'}<br>   
        when waiting retrieval: ${task.when_waiting_retrieval ? task.when_waiting_retrieval.toFixed(2) : 'N/A'}<br>
        when retrieved: ${task.when_retrieved ? task.when_retrieved.toFixed(2) : 'N/A'}<br>
        when done: ${task.when_done ? task.when_done.toFixed(2) : 'N/A'}<br>
    `;

    return htmlContent;
}

function parseTimeArray(timeStr) {
    // Convert "[1739409589.49]" to array of numbers

    try {
        return JSON.parse(timeStr.replace(/'/g, '"')).map(Number);
    } catch (e) {
        console.error('Error parsing time:', timeStr);
        return [];
    }
}

function getFailureType(status) {
    // Find the failure type that matches the status value
    return Object.entries(FAILURE_TYPES).find(([_, type]) => type.value === status)?.[0];
}

function setLegend() {
    const legendContainer = document.getElementById('execution-details-legend');
    legendContainer.innerHTML = '';

    // Create failure items based on num_of_status data
    const failureItems = Object.entries(FAILURE_TYPES)
        .filter(([_, type]) => state.num_of_status && state.num_of_status[type.value] > 0)
        .map(([key, value]) => ({
            id: `unsuccessful-${key.toLowerCase()}`,
            label: `${value.label} (${state.num_of_status[value.value]})`,
            color: value.color,
            checked: false,
            count: state.num_of_status[value.value]
        }))
        .sort((a, b) => b.count - a.count);

    const legendGroups = [
        {
            title: `Successful Tasks (${state.num_of_status[0] || 0} total)`,
            items: [
                { id: 'successful-committing-to-worker', label: 'Committing', color: colors['successful-committing-to-worker'], checked: false },
                { id: 'successful-executing-on-worker', label: 'Executing', color: colors['successful-executing-on-worker'], checked: true },
                { id: 'successful-retrieving-to-manager', label: 'Retrieving', color: colors['successful-retrieving-to-manager'], checked: false }
            ]
        },
        {
            title: `Unsuccessful Tasks (${
                Object.entries(state.num_of_status || {})
                    .filter(([status, _]) => status !== '0')
                    .reduce((sum, [_, count]) => sum + count, 0)
            } total)`,
            items: failureItems
        },
        {
            title: `Recovery Tasks (${(state.num_successful_recovery_tasks || 0) + (state.num_unsuccessful_recovery_tasks || 0)} total)`,
            items: [
                { 
                    id: 'recovery-successful', 
                    label: `Successful (${state.num_successful_recovery_tasks || 0})`, 
                    color: colors['recovery-successful'], 
                    checked: false 
                },
                { 
                    id: 'recovery-unsuccessful', 
                    label: `Unsuccessful (${state.num_unsuccessful_recovery_tasks || 0})`, 
                    color: colors['recovery-unsuccessful'], 
                    checked: false 
                }
            ]
        },
        {
            title: 'Infrastructure',
            items: [
                { id: 'workers', label: 'Workers', color: colors['workers'], checked: true }
            ]
        }
    ];

    // Only include groups that have tasks
    const groupsToDisplay = legendGroups.filter(group => 
        group.title !== 'Unsuccessful Tasks' || group.items.length > 0
    );

    // Create a flex container for better layout control
    const flexContainer = document.createElement('div');
    flexContainer.className = 'legend-flex-container';
    legendContainer.appendChild(flexContainer);

    // Add groups to the flex container
    groupsToDisplay.forEach(group => {
        const groupDiv = document.createElement('div');
        groupDiv.className = 'legend-group';
        
        const titleContainer = document.createElement('div');
        titleContainer.className = 'legend-group-title-container';
        
        const groupTitle = document.createElement('div');
        groupTitle.className = 'legend-group-title';
        groupTitle.textContent = group.title;
        
        if (group.tooltip) {
            const questionMark = document.createElement('span');
            questionMark.className = 'legend-help-icon';
            questionMark.textContent = '?';
            questionMark.title = group.tooltip;
            titleContainer.appendChild(groupTitle);
            titleContainer.appendChild(questionMark);
        } else {
            titleContainer.appendChild(groupTitle);
        }
        
        groupDiv.appendChild(titleContainer);

        group.items.forEach(item => {
            const legendItem = document.createElement('div');
            legendItem.className = `legend-item${item.checked ? ' checked' : ''}`;
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `${item.id}-checkbox`;
            checkbox.checked = item.checked;
            checkbox.style.display = 'none';
            
            const colorBox = document.createElement('div');
            colorBox.className = 'legend-color';
            colorBox.style.setProperty('--color', item.color);
            
            const label = document.createElement('span');
            label.textContent = item.label;
            
            legendItem.appendChild(checkbox);
            legendItem.appendChild(colorBox);
            legendItem.appendChild(label);
            
            legendItem.addEventListener('click', () => {
                checkbox.checked = !checkbox.checked;
                legendItem.classList.toggle('checked');
                plotExecutionDetails();
            });
            
            groupDiv.appendChild(legendItem);
        });

        flexContainer.appendChild(groupDiv);
    });
}

// Update how we get checkbox states
function isTaskTypeChecked(taskType) {
    const checkbox = document.getElementById(`${taskType}-checkbox`);
    return checkbox && checkbox.checked;
}

function plotExecutionDetails() {
    if (!state.successfulTasks) {
        return;
    }

    let margin = calculateMargin();
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

    const { xScale, yScale, safeHeight } = plotAxis(svg, svgWidth, svgHeight);

    // Plot worker connection periods
    if (isTaskTypeChecked('workers') && state.workerInfo) {
        state.workerInfo.forEach(worker => {
            const connectTimes = worker.time_connected;
            const disconnectTimes = worker.time_disconnected;
            
            // Create rectangle for each connection period
            for (let i = 0; i < connectTimes.length; i++) {
                const connectTime = connectTimes[i];
                const disconnectTime = disconnectTimes[i] || state.maxTime;
                const height = Math.max(0, safeHeight() * worker.cores + 
                    (yScale.step() - safeHeight()) * (worker.cores - 1));

                svg.append('rect')
                    .attr('x', xScale(connectTime))
                    .attr('y', yScale(worker.id + '-' + worker.cores))
                    .attr('width', Math.max(0, xScale(disconnectTime) - xScale(connectTime)))
                    .attr('height', height)
                    .attr('fill', colors['workers'])
                    .attr('opacity', 0.3)
                    .on('mouseover', function(event) {
                        d3.select(this).attr('fill', HIGHLIGHT_COLOR);
                        tooltip.innerHTML = `
                            cores: ${worker.cores}<br>
                            worker id: ${worker.id}<br>
                            worker ip port: ${worker.worker_ip_port}<br>
                            when connected: ${(connectTime).toFixed(2)}s<br>
                            when disconnected: ${(disconnectTime).toFixed(2)}s<br>
                            life time: ${(disconnectTime - connectTime).toFixed(2)}s<br>`;
                        tooltip.style.visibility = 'visible';
                        tooltip.style.top = (event.pageY + 10) + 'px';
                        tooltip.style.left = (event.pageX + 10) + 'px';
                    })
                    .on('mouseout', function() {
                        d3.select(this).attr('fill', colors['workers']);
                        tooltip.style.visibility = 'hidden';
                    });
            }
        });
    }

    // Plot successful tasks
    state.successfulTasks.forEach(task => {
        // Committing to worker phase
        if (task.when_running && task.time_worker_start) {
            if (isTaskTypeChecked('successful-committing-to-worker')) {
                svg.append('rect')
                    .attr('x', xScale(task.when_running))
                    .attr('y', yScale(task.worker_id + '-' + task.core_id))
                    .attr('width', xScale(task.time_worker_start) - xScale(task.when_running))
                    .attr('height', safeHeight())
                    .attr('fill', colors['successful-committing-to-worker'])
                    .on('mouseover', function(event) {
                        d3.select(this).attr('fill', HIGHLIGHT_COLOR);
                        tooltip.innerHTML = getTaskInnerHTML(task);
                        tooltip.style.visibility = 'visible';
                        tooltip.style.top = (event.pageY + 10) + 'px';
                        tooltip.style.left = (event.pageX + 10) + 'px';
                    })
                    .on('mouseout', function() {
                        d3.select(this).attr('fill', colors['successful-committing-to-worker']);
                        tooltip.style.visibility = 'hidden';
                    });
            }
        }

        // Executing on worker phase
        if (task.time_worker_start && task.time_worker_end) {
            if (isTaskTypeChecked('successful-executing-on-worker')) {
                svg.append('rect')
                    .attr('x', xScale(task.time_worker_start))
                    .attr('y', yScale(task.worker_id + '-' + task.core_id))
                    .attr('width', xScale(task.time_worker_end) - xScale(task.time_worker_start))
                    .attr('height', safeHeight())
                    .attr('fill', colors['successful-executing-on-worker'])
                    .on('mouseover', function(event) {
                        d3.select(this).attr('fill', HIGHLIGHT_COLOR);
                        tooltip.innerHTML = getTaskInnerHTML(task);
                        tooltip.style.visibility = 'visible';
                        tooltip.style.top = (event.pageY + 10) + 'px';
                        tooltip.style.left = (event.pageX + 10) + 'px';
                    })
                    .on('mouseout', function() {
                        d3.select(this).attr('fill', colors['successful-executing-on-worker']);
                        tooltip.style.visibility = 'hidden';
                    });
            }
        }

        // Retrieving to manager phase
        if (task.time_worker_end && task.when_done) {
            if (isTaskTypeChecked('successful-retrieving-to-manager')) {
                svg.append('rect')
                    .attr('x', xScale(task.time_worker_end))
                    .attr('y', yScale(task.worker_id + '-' + task.core_id))
                    .attr('width', xScale(task.when_retrieved) - xScale(task.time_worker_end))
                    .attr('height', safeHeight())
                    .attr('fill', colors['successful-retrieving-to-manager'])
                    .on('mouseover', function(event) {
                        d3.select(this).attr('fill', HIGHLIGHT_COLOR);
                        tooltip.innerHTML = getTaskInnerHTML(task);
                        tooltip.style.visibility = 'visible';
                        tooltip.style.top = (event.pageY + 10) + 'px';
                        tooltip.style.left = (event.pageX + 10) + 'px';
                    })
                    .on('mouseout', function() {
                        d3.select(this).attr('fill', colors['successful-retrieving-to-manager']);
                        tooltip.style.visibility = 'hidden';
                    });
            }
        }
    });

    // Plot unsuccessful tasks
    if (state.unsuccessfulTasks) {
        state.unsuccessfulTasks.forEach(task => {
            const failureType = getFailureType(task.task_status);
            if (failureType) {
                const failureId = `unsuccessful-${failureType.toLowerCase()}`;
                if (isTaskTypeChecked(failureId)) {
                    const startTime = task.when_running || task.time_worker_start;
                    if (startTime) {
                        svg.append('rect')
                            .attr('x', xScale(startTime))
                            .attr('y', yScale(task.worker_id + '-' + task.core_id))
                            .attr('width', xScale(task.when_failure_happens) - xScale(startTime))
                            .attr('height', safeHeight())
                            .attr('fill', FAILURE_TYPES[failureType].color)
                            .on('mouseover', function(event) {
                                d3.select(this).attr('fill', HIGHLIGHT_COLOR);
                                tooltip.innerHTML = `
                                    Task ID: ${task.task_id}<br>
                                    Worker: ${task.worker_ip}:${task.worker_port}<br>
                                    Core: ${task.core_id}<br>
                                    Failure Type: ${FAILURE_TYPES[failureType].label}<br>
                                    Start time: ${(startTime).toFixed(2)}s<br>
                                    When Completes: ${(task.when_failure_happens).toFixed(2)}s<br>
                                    Duration: ${(task.when_failure_happens - startTime).toFixed(2)}s`;
                                tooltip.style.visibility = 'visible';
                                tooltip.style.top = (event.pageY + 10) + 'px';
                                tooltip.style.left = (event.pageX + 10) + 'px';
                            })
                            .on('mouseout', function() {
                                d3.select(this).attr('fill', FAILURE_TYPES[failureType].color);
                                tooltip.style.visibility = 'hidden';
                            });
                    }
                }
            }
        });
    }

    // Plot recovery tasks
    if (state.successfulTasks) {
        state.successfulTasks.forEach(task => {
            if (task.is_recovery_task === true) {
                if (isTaskTypeChecked('recovery-successful')) {
                    if (task.time_worker_start && task.time_worker_end) {
                        svg.append('rect')
                            .attr('x', xScale(task.time_worker_start))
                            .attr('y', yScale(task.worker_id + '-' + task.core_id))
                            .attr('width', xScale(task.time_worker_end) - xScale(task.time_worker_start))
                            .attr('height', safeHeight())
                            .attr('fill', colors['recovery-successful'])
                            .on('mouseover', function(event) {
                                const tooltip = document.getElementById('vine-tooltip');
                                d3.select(this).attr('fill', HIGHLIGHT_COLOR);
                                tooltip.innerHTML = `
                                    Task ID: ${task.task_id}<br>
                                    Worker: ${task.worker_ip}:${task.worker_port}<br>
                                    Core: ${task.core_id}<br>
                                    Type: Recovery Task (Successful)<br>
                                    Start time: ${(task.time_worker_start).toFixed(2)}s<br>
                                    End time: ${(task.time_worker_end).toFixed(2)}s<br>
                                    Duration: ${(task.time_worker_end - task.time_worker_start).toFixed(2)}s`;
                                tooltip.style.visibility = 'visible';
                                tooltip.style.top = (event.pageY -15) + 'px';
                                tooltip.style.left = (event.pageX + 10) + 'px';
                            })
                            .on('mouseout', function() {
                                const tooltip = document.getElementById('vine-tooltip');
                                d3.select(this).attr('fill', colors['recovery-successful']);
                                tooltip.style.visibility = 'hidden';
                            });
                    }
                }
            }
        });
    }

    if (state.unsuccessfulTasks) {
        state.unsuccessfulTasks.forEach(task => {
            if (task.is_recovery_task === true) {
                if (isTaskTypeChecked('recovery-unsuccessful')) {
                    const startTime = task.when_running || task.time_worker_start;
                    if (startTime) {
                        const endTime = task.when_failure_happens <= startTime ? 
                            startTime + 0.01 : task.when_failure_happens;
                        
                        svg.append('rect')
                            .attr('x', xScale(startTime))
                            .attr('y', yScale(task.worker_id + '-' + task.core_id))
                            .attr('width', xScale(endTime) - xScale(startTime))
                            .attr('height', safeHeight())
                            .attr('fill', colors['recovery-unsuccessful'])
                            .on('mouseover', function(event) {
                                const tooltip = document.getElementById('vine-tooltip');
                                d3.select(this).attr('fill', HIGHLIGHT_COLOR);
                                tooltip.innerHTML = `
                                    Task ID: ${task.task_id}<br>
                                    Worker: ${task.worker_ip}:${task.worker_port}<br>
                                    Core: ${task.core_id}<br>
                                    Type: Recovery Task (Unsuccessful)<br>
                                    Failure Type: ${FAILURE_TYPES[getFailureType(task.task_status)]?.label || 'Unknown'}<br>
                                    Start time: ${(startTime).toFixed(2)}s<br>
                                    When Completes: ${(endTime).toFixed(2)}s<br>
                                    Duration: ${(endTime - startTime).toFixed(2)}s`;
                                tooltip.style.visibility = 'visible';
                                tooltip.style.top = (event.pageY + 10) + 'px';
                                tooltip.style.left = (event.pageX + 10) + 'px';
                            })
                            .on('mouseout', function() {
                                const tooltip = document.getElementById('vine-tooltip');
                                d3.select(this).attr('fill', colors['recovery-unsuccessful']);
                                tooltip.style.visibility = 'hidden';
                            });
                    }
                }
            }
        });
    }
}

function calculateMargin() {
    const margin = { top: 40, right: 30, bottom: 60, left: 30 };  // Increased bottom margin

    const tempSvg = svgElement
        .append('g')
        .attr('class', 'temp');

    const tempYScale = d3.scaleBand()
        .domain(state.workerInfo.map(d => `${d.id}-${d.cores}`));

    const tempYAxis = d3.axisLeft(tempYScale)
        .tickFormat(d => d.split('-')[0]);

    tempSvg.call(tempYAxis);
    tempSvg.selectAll('text').style('font-size', state.tickFontSize);
    
    const maxYLabelWidth = d3.max(tempSvg.selectAll('.tick text').nodes(), 
        d => d.getBBox().width);
    tempSvg.remove();

    margin.left = Math.ceil(maxYLabelWidth + 20);

    return margin;
}

function plotAxis(svg, svgWidth, svgHeight) {
    // set x scale
    const xScale = d3.scaleLinear()
        .domain([state.xMin, state.xMax])
        .range([0, svgWidth]);

    // set y scale
    const workerCoresMap = [];
    state.workerInfo.forEach(d => {
        for (let i = 1; i <= +d.cores; i++) {
            workerCoresMap.push(`${d.id}-${i}`);
        }
    });
    
    // Sort workerCoresMap to have smaller worker IDs at the bottom and smaller core IDs at the bottom within each worker
    workerCoresMap.sort((a, b) => {
        const [workerIdA, coreIdA] = a.split('-').map(Number);
        const [workerIdB, coreIdB] = b.split('-').map(Number);
        if (workerIdA !== workerIdB) {
            return workerIdA - workerIdB;
        }
        return coreIdA - coreIdB;
    });

    const yScale = d3.scaleBand()
        .domain(workerCoresMap)
        .range([svgHeight, 0])  // Changed to [svgHeight, 0] to have smaller IDs at bottom
        .padding(0.1);

    // Ensure bandwidth is positive
    if (yScale.bandwidth() <= 0) {
        console.warn('Invalid bandwidth detected, adjusting scale parameters');
        yScale.padding(0.05);  // Reduce padding if bandwidth is too small
    }

    // draw x axis
    const xAxis = d3.axisBottom(xScale)
        .tickSizeOuter(0)
        .tickValues(state.xTickValues)
        .tickFormat(d => `${d3.format('.2f')(d)} s`);
    svg.append('g')
        .attr('transform', `translate(0, ${svgHeight})`)
        .call(xAxis)
        .selectAll('text')
        .style('font-size', state.tickFontSize)
        .attr('dy', '2em');  // Move tick labels down

    // draw y axis
    const maxTicks = 5;
    const selectedTicks = [];
    
    // Calculate step size based on number of unique worker IDs
    const uniqueWorkerIds = [...new Set(workerCoresMap.map(id => id.split('-')[0]))];
    const step = Math.max(1, Math.floor(uniqueWorkerIds.length / maxTicks));
    
    // Select ticks based on unique worker IDs
    for (let i = 0; i < uniqueWorkerIds.length; i += step) {
        // Find the first core entry for this worker ID
        const workerId = uniqueWorkerIds[i];
        const tick = workerCoresMap.find(id => id.split('-')[0] === workerId);
        if (tick) {
            selectedTicks.push(tick);
        }
    }
    
    // Always include the last worker's first core if not already included
    const lastWorkerId = uniqueWorkerIds[uniqueWorkerIds.length - 1];
    const lastTick = workerCoresMap.find(id => id.split('-')[0] === lastWorkerId);
    if (lastTick && !selectedTicks.includes(lastTick)) {
        selectedTicks.push(lastTick);
    }

    const yAxis = d3.axisLeft(yScale)
        .tickValues(selectedTicks)
        .tickSizeOuter(0)
        .tickFormat(d => d.split('-')[0]);
    
    svg.append('g')
        .call(yAxis)
        .selectAll('text')
        .style('font-size', state.tickFontSize);

    return { 
        xScale, 
        yScale,
        safeHeight: () => Math.max(0, yScale.bandwidth())
    };
}

function handleResetClick() {
    document.querySelector('#execution-details').style.width = '100%';
    document.querySelector('#execution-details').style.height = '100%';
    plotExecutionDetails();
}
function handleDownloadClick() {
    downloadSVG('execution-details');
}

async function initialize() {
    try {
        svgElement.selectAll('*').remove();

        const url = `/api/execution-details`;
        const response = await fetch(url);
        const data = await response.json();
        
        if (!data) {
            return;
        }

        state.successfulTasks = data.successfulTasks;
        state.unsuccessfulTasks = data.unsuccessfulTasks;
        state.workerInfo = data.workerInfo;
        state.tickFontSize = data.tickFontSize;
        state.xTickValues = data.xTickValues;
        state.xMin = data.xMin;
        state.xMax = data.xMax;
        state.num_of_status = data.num_of_status;
        state.num_successful_recovery_tasks = data.num_successful_recovery_tasks;
        state.num_unsuccessful_recovery_tasks = data.num_unsuccessful_recovery_tasks;

        setLegend();
        
        buttonDownload.removeEventListener('click', handleDownloadClick); 
        buttonDownload.addEventListener('click', handleDownloadClick);

        buttonReset.removeEventListener('click', handleResetClick);
        buttonReset.addEventListener('click', handleResetClick);

        document.querySelector('#execution-details').style.width = '100%';
        document.querySelector('#execution-details').style.height = '100%';
        plotExecutionDetails();
        setupZoomAndScroll('#execution-details', '#execution-details-container');
    } catch (error) {
        console.error('Error fetching execution details:', error);
    }
}

window.document.addEventListener('dataLoaded', initialize);
window.addEventListener('resize', _.debounce(() => plotExecutionDetails(), 300));

