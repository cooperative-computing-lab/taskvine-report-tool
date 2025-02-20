import { downloadSVG, getTaskInnerHTML } from './tools.js';
import { createTable } from './draw_tables.js';

const errorTip = document.getElementById('subgraph-info-error-tip');
const dagSelector = document.getElementById('dag-id-selector');

const inputAnalyzeTask = document.getElementById('input-task-id-in-dag');
const buttonAnalyzeTask = document.getElementById('button-analyze-task-in-dag');

const inputAnalyzeFile = document.getElementById('input-filename-id-in-dag');
const buttonAnalyzeFile = document.getElementById('button-analyze-file-in-dag');

const buttonHighlightCriticalPath = document.getElementById('button-highlight-critical-path');
const buttonDownload = document.getElementById('button-download-dag');
const analyzeTaskDisplayDetails = document.getElementById('analyze-task-display-details');

const criticalPathInfoDiv = document.getElementById('critical-path-info-div');
const criticalPathSvgContainer = document.getElementById('critical-path-container');
const criticalPathSvgElement = d3.select('#critical-path-svg');

const highlightTaskColor = '#f69697';

const taskInformationDiv = document.getElementById('analyze-task-display-task-information');

var graphNodeMap = {};
var criticalTasks = null;

const colorExecution = 'steelblue';
const colorHighlight = 'orange';

const tooltip = document.getElementById('vine-tooltip');

export async function plotSubgraph(dagID) {
    try {
        if (typeof window.graphInfo === 'undefined') {
            return;
        }

        try {
            const svgContainer = d3.select('#graph-information-svg');
            svgContainer.selectAll('*').remove();

            const svgContent = await d3.svg(`logs/${window.logName}/vine-logs/subgraph_${dagID}.svg`);
            svgContainer.node().appendChild(svgContent.documentElement);
            const insertedSVG = svgContainer.select('svg');

            insertedSVG
                .attr('preserveAspectRatio', 'xMidYMid meet');

            graphNodeMap = {};
            d3.select('#graph-information-svg svg').selectAll('.node').each(function() {
                var nodeText = d3.select(this).select('text').text();
                graphNodeMap[nodeText] = d3.select(this);
            });

        } catch (error) {
            console.error(error);
        }
    } catch (error) {
        console.error('error when parsing ', error);
    }
}

dagSelector.addEventListener('change', async function() {
    if (buttonHighlightCriticalPath.classList.contains('report-button-active')) {
        buttonHighlightCriticalPath.classList.toggle('report-button-active');
    }

    if (buttonAnalyzeTask.classList.contains('report-button-active')) {
        removeHighlightedTask();
        analyzeTaskDisplayDetails.style.display = 'none';
        buttonAnalyzeTask.classList.toggle('report-button-active');
    }
    
    const selectedDAGID = dagSelector.value;
    await plotSubgraph(selectedDAGID);

    window.graphInfo.forEach(function(d) {
        if (d.graph_id === +dagSelector.value) {
            criticalTasks = d.critical_tasks;
        }
    });
});

function handleDownloadClick() {
    const selectedDAGID = dagSelector.value;
    downloadSVG('graph-information-svg', 'subgraph_' + selectedDAGID + '.svg');
}
window.parent.document.addEventListener('dataLoaded', function() {
    if (typeof window.graphInfo === 'undefined' || window.graphInfo === null) {
        errorTip.style.visibility = 'visible';
        return;
    }
    errorTip.style.visibility = 'hidden';

    // first remove the previous options
    dagSelector.innerHTML = '';
    // update the options
    const dagIDs = window.graphInfo.map(dag => dag.graph_id);
    dagIDs.forEach(dagID => {
        const option = document.createElement('option');
        option.value = dagID;
        option.text = `${dagID}`;
        dagSelector.appendChild(option);
    });
    dagSelector.dispatchEvent(new Event('change'));

    if (buttonHighlightCriticalPath.classList.contains('report-button-active')) {
        buttonHighlightCriticalPath.classList.toggle('report-button-active');
        criticalPathInfoDiv.style.display = 'none';
    }

    buttonDownload.removeEventListener('click', handleDownloadClick); 
    buttonDownload.addEventListener('click', handleDownloadClick);
});

function removeHighlightedTask() {
    if (typeof window.highlitedTask === 'undefined') {
        return;
    }
    graphNodeMap[window.highlitedTask].select('ellipse').style('fill', 'white');
    
    // set the color to critical path color if the task is in the critical path
    if (window.highlitedTask in criticalTasks && buttonHighlightCriticalPath.classList.contains('report-button-active')) {
        graphNodeMap[window.highlitedTask].select('ellipse').style('fill', 'orange');
    }

    window.parentTasks.forEach(function(taskID) {
        graphNodeMap[taskID].select('ellipse').style('fill', 'white');
    });
    window.parentTasks = [];

    window.childTasks.forEach(function(taskID) {
        graphNodeMap[taskID].select('ellipse').style('fill', 'white');
    });
    window.highlitedTask = undefined;
}

function highlightTask(taskID) {
    taskID = +taskID;
    if (isNaN(taskID) || taskID === 0) {
        return;
    }
    if (!window.doneTasks.some(d => +d.task_id === taskID)) {
        return;
    }
    removeHighlightedTask();
    window.highlitedTask = taskID;
    window.previousTaskColor = graphNodeMap[taskID].select('ellipse').style('fill');
    graphNodeMap[taskID].select('ellipse').style('fill', highlightTaskColor);
}


function displayAnalyzedTaskInfo(taskID) {
    // show the information div
    var taskData = window.doneTasks.find(d => d.task_id === taskID);

    // analyzed task table
    var specificSettings = {
        "ajax": {
            "url": 'tasks_completed',
            "type": "GET",
            "data": function(d) {
                d.log_name = window.logName;
                d.search.type = 'task-id';
                d.search.value = taskData.task_id;
                d.timestamp_type = 'relative';
            },
            "dataSrc": function(response) {
                response.data.forEach(function(task) {
                    task.task_id = parseInt(task.task_id, 10);
                    task.try_id = parseInt(task.try_id, 10);
                    task.worker_id = parseInt(task.worker_id, 10);
                });
                return response.data;
            }
        },
        "info": false,           // Will Disabled "1 to n of n entries" Text at bottom
        "pagingType": "simple",  // 'Previous' and 'Next' buttons only
        "columns": [
            { "data": "task_id" },
            { "data": "try_id" },
            { "data": "worker_id" },
            { "data": "execution_time" },
            { "data": "when_ready" },
            { "data": "when_running" },
            { "data": "time_worker_start" },
            { "data": "time_worker_end" },
            { "data": "when_waiting_retrieval" },
            { "data": "when_retrieved" },
            { "data": "when_done" },
            { "data": "category" },
            { "data": "graph_id" },
            { "data": "size_input_files_mb" },
            { "data": "size_output_files_mb" },
            { "data": "input_files" },
            { "data": "output_files" },
        ],
    };
    var table = createTable('#analyzed-task-table', specificSettings);

    // update the input files table
    const inputFilesSet = new Set(taskData.input_files);
    const tableData = window.fileInfo.filter(file => inputFilesSet.has(file.filename))
        .map(file => {
            let fileWaitingTime = (taskData.time_worker_start - file['worker_holding'][0][1]).toFixed(2);
            let dependencyTime = file['producers'].length <= 1 ? "0" : (() => {
                for (let i = file['producers'].length - 1; i >= 0; i--) {
                    let producerTaskID = +file['producers'][i];
                    let producerTaskData = window.doneTasks.find(d => +d.task_id === producerTaskID);
            
                    if (producerTaskData && producerTaskData.time_worker_end < taskData.time_worker_start) {
                        return (taskData.time_worker_start - producerTaskData.time_worker_end).toFixed(2);
                    }
                }
                return "0"; 
            })();                
            let formattedWorkerHolding = file['worker_holding'].map(tuple => {
                const worker_id = tuple[0];
                const time_stage_in = (tuple[1] - window.time_manager_start).toFixed(2);
                const time_stage_out = (tuple[2] - window.time_manager_start).toFixed(2);
                const lifetime = tuple[3].toFixed(2);
                return `worker${worker_id}: ${time_stage_in}s-${time_stage_out}s (${lifetime}s)`;
            }).join(', ');
            return {
                filename: file.filename,
                size: file['size(MB)'],
                fileWaitingTime: fileWaitingTime,
                dependencyTime: dependencyTime,
                producers: file.producers,
                consumers: file.consumers,
                workerHolding: formattedWorkerHolding
            };
        });
    table = $('#task-input-files-table');
    if ($.fn.dataTable.isDataTable(table)) {
        table.DataTable().destroy();
    }
    var specificSettings = {
        "processing": false,
        "serverSide": false,
        "data": tableData,
        "columns": [
            { "data": 'filename' },
            { "data": 'size' },
            { "data": 'fileWaitingTime' },
            { "data": 'dependencyTime' },
            { "data": 'producers' },
            { "data": 'consumers' },
            { "data": 'workerHolding' }
        ],
    }
    table = createTable('#task-input-files-table', specificSettings);
}

function highlightParentsAndChildren(taskID) {
    // parents
    window.parentTasks = [];
    function getTaskParents(taskID) {
        const taskData = window.doneTasks.find(d => d.task_id === taskID);
        const inputFiles = taskData.input_files;
        if (inputFiles.length === 0) {
            return;
        }
    
        const thisParents = [];
        inputFiles.forEach(inputFile => {
            const producers = window.fileInfo.find(d => d.filename === inputFile).producers;
            if (producers.length === 0) {
                return;
            }
            for (let i = 0; i < producers.length; i++) {
                // an input file should only has one producer, and the end time of the producer should be less than the start time of the task
                const producerTaskID = +producers[i];
                const producerTaskData = window.doneTasks.find(d => +d.task_id === producerTaskID);
                if (producerTaskData.when_output_fully_lost > taskData.time_worker_start) {
                    thisParents.push(producerTaskID);
                    break;
                }
            }
        });
        if (thisParents.length === 0) {
            return;
        }
        parentTasks.push(...thisParents);
        thisParents.forEach(parentTaskID => {
            graphNodeMap[parentTaskID].select('ellipse').style('fill', 'orange');
            getTaskParents(parentTaskID);
        });
    }
    getTaskParents(taskID);

    // children
    window.childTasks = [];
    function getTaskChildren(taskID) {
        const taskData = window.doneTasks.find(d => d.task_id === taskID);
        const outputFiles = taskData.output_files;
    
        const thisChildren = [];
        outputFiles.forEach(outputFile => {
            const consumers = window.fileInfo.find(d => d.filename === outputFile).consumers;
            if (consumers.length === 0) {
                return;
            }
            for (let i = 0; i < consumers.length; i++) {
                // an output file can have multiple consumers
                const consumerTaskID = +consumers[i];
                const consumerTaskData = window.doneTasks.find(d => +d.task_id === consumerTaskID);
                if (consumerTaskData.when_ready > taskData.when_done && taskData.when_output_fully_lost > consumerTaskData.time_worker_start) {
                    thisChildren.push(consumerTaskID);
                }
            }
        });
        if (thisChildren.length === 0) {
            return;
        }
        childTasks.push(...thisChildren);
        thisChildren.forEach(childTaskID => {
            graphNodeMap[childTaskID].select('ellipse').style('fill', 'orange');
            getTaskChildren(childTaskID);
        });
    }
    getTaskChildren(taskID);
}


function handleAnalyzeTaskClick(filename) {
    const inputValue = inputAnalyzeTask.value;
    const taskID = +inputValue;
    // invalid input
    if (inputValue === "" || isNaN(taskID) || !window.doneTasks.some(d => d.task_id === taskID)) {
        removeHighlightedTask();
        analyzeTaskDisplayDetails.style.display = 'none';
        if (this.classList.contains('report-button-active')) {
            this.classList.toggle('report-button-active');
        }
        return;
    }
    if (taskID === window.highlitedTask) {
        return;
    }

    // a valid task id
    highlightTask(taskID);
    if (!this.classList.contains('report-button-active')) {
        this.classList.toggle('report-button-active');
        analyzeTaskDisplayDetails.style.display = 'block';
    }

    displayAnalyzedTaskInfo(taskID);

    // hightlight the parents and children
    highlightParentsAndChildren(taskID);
}

buttonAnalyzeTask.addEventListener('click', handleAnalyzeTaskClick);


function displayCriticalTasksTable() {
    var specificSettings = {
        "ajax": {
            "url": 'tasks_completed',
            "type": "GET",
            "data": function(d) {
                d.log_name = window.logName;
                d.search.type = 'task-ids';
                d.search.value = criticalTasks.join(',');
                d.timestamp_type = 'relative';
            },
            "dataSrc": function(response) {
                response.data.forEach(function(task) {
                    task.task_id = parseInt(task.task_id, 10);
                    task.try_id = parseInt(task.try_id, 10);
                    task.worker_id = parseInt(task.worker_id, 10);
                });
                return response.data;
            }
        },
        "columns": [
            { "data": "task_id" },
            { "data": "try_id" },
            { "data": "worker_id" },
            { "data": "execution_time" },
            { "data": "when_ready" },
            { "data": "when_running" },
            { "data": "time_worker_start" },
            { "data": "time_worker_end" },
            { "data": "when_waiting_retrieval" },
            { "data": "when_retrieved" },
            { "data": "when_done" },
            { "data": "category" },
            { "data": "graph_id" },
            { "data": "size_input_files_mb" },
            { "data": "size_output_files_mb" },
            { "data": "input_files" },
            { "data": "output_files" },
        ],
    };
    var table = createTable('#critical-tasks-table', specificSettings);
}


function displayCriticalPathInfo() {
    if (buttonHighlightCriticalPath.classList.contains('report-button-active')) {
        criticalPathInfoDiv.style.display = 'block';
    } else {
        criticalPathInfoDiv.style.display = 'none';
        return;
    }
    const margin = {top: 20, right: 30, bottom: 20, left: 30};
    const svgWidth = criticalPathSvgContainer.clientWidth - margin.left - margin.right;
    const svgHeight = criticalPathSvgContainer.clientHeight - margin.top - margin.bottom;

    var graphStartTime;
    var graphEndTime;
    window.graphInfo.forEach(function(d) {
        if (d.graph_id === +dagSelector.value) {
            graphStartTime = d.time_start;
            graphEndTime = d.time_end;
        }
    });

    criticalPathSvgElement.selectAll('*').remove();

    const svg = criticalPathSvgElement
        .attr('viewBox', `0 0 ${criticalPathSvgContainer.clientWidth} ${criticalPathSvgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const xScale = d3.scaleLinear()
        .domain([0, graphEndTime - graphStartTime])
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
        .tickFormat(d3.format(".1f"));
    svg.append('g')
        .attr('transform', `translate(0, ${svgHeight})`)
        .call(xAxis);

    const yScale = d3.scaleBand()
        .domain([0])
        .range([svgHeight, 0]);

    criticalTasks.forEach(function(taskID) {
        var taskData = window.doneTasks.find(d => d.task_id === taskID);

        svg.append('rect')
            .attr('x', xScale(taskData.when_ready - graphStartTime))
            .attr('y', yScale(0))
            .attr('width', xScale(taskData.when_done - graphStartTime) - xScale(taskData.when_ready - graphStartTime))
            .attr('height', yScale.bandwidth())
            .attr('fill', colorExecution)
            .on('mouseover', function(event, d) {
                d3.select(this).attr('fill', colorHighlight);
                tooltip.innerHTML = getTaskInnerHTML(taskData);
                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mousemove', function(event) {
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mouseout', function() {
                d3.select(this).attr('fill', colorExecution);
                tooltip.style.visibility = 'hidden';
            });
    });
}

function hideCriticalPathInfo() {
    criticalPathInfoDiv.style.display = 'none';
    criticalPathSvgElement.selectAll('*').remove();
}

function removeHighlightedCriticalPath() {
    criticalTasks.forEach(function(taskID) {
        // recover the highlighted task
        graphNodeMap[taskID].select('ellipse').style('fill', 'white');
        if (taskID === window.highlitedTask && buttonAnalyzeTask.classList.contains('report-button-active')) {
            graphNodeMap[taskID].select('ellipse').style('fill', highlightTaskColor);
        }
        // recover the highlighted output file
        var outputFiles = window.doneTasks.find(d => +d.task_id === taskID).output_files;
        if (outputFiles.length !== 1) {
            console.log(`Task ${taskID} has ${outputFiles.length} output files`);
            return;
        }
        var outputFile = outputFiles[0];
        var targetNode = graphNodeMap[outputFile];
        targetNode.select('polygon').style('fill', 'white');
    });

}

function highlightCriticalPath() {
    criticalTasks.forEach(function(taskID) {
        // highlight the task
        graphNodeMap[`${taskID}`].select('ellipse').style('fill', 'orange');
        if (taskID === window.highlitedTask && buttonAnalyzeTask.classList.contains('report-button-active')) {
            graphNodeMap[taskID].select('ellipse').style('fill', highlightTaskColor);
        }
        // find the output file
        var outputFiles = window.doneTasks.find(d => +d.task_id === taskID).output_files;
        if (outputFiles.length !== 1) {
            console.log(`Task ${taskID} has ${outputFiles.length} output files`);
            return;
        }
        var outputFile = outputFiles[0];
        var targetNode = graphNodeMap[outputFile];
        targetNode.select('polygon').style('fill', 'orange');
    });
}

buttonHighlightCriticalPath.addEventListener('click', async function() {
    if (typeof window.graphInfo === 'undefined') {
        return;
    }

    this.classList.toggle('report-button-active');
    if (this.classList.contains('report-button-active')) {
        displayCriticalTasksTable();
        displayCriticalPathInfo();
        highlightCriticalPath();
    } else {
        hideCriticalPathInfo();
        removeHighlightedCriticalPath();
    }

    // if the analyze button is active, invoke after highlighting the critical path
    if (buttonAnalyzeTask.classList.contains('report-button-active')) {
        handleAnalyzeTaskClick(window.highlitedTask);
    }
});


window.parent.document.getElementById('log-selector').addEventListener('change', () => {
    analyzeTaskDisplayDetails.style.display = 'none';
    buttonAnalyzeTask.classList.remove('report-button-active');
    if (buttonHighlightCriticalPath.classList.contains('report-button-active')) {
        buttonHighlightCriticalPath.classList.remove('report-button-active');
    }
    hideCriticalPathInfo();
    inputAnalyzeTask.value = '';
});


window.addEventListener('resize', _.debounce(() => displayCriticalPathInfo(), 2000));
