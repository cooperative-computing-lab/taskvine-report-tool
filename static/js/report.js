import { fetchCSVData } from './tools.js';

// Some global variables
window.xTickFormat = ".0f";
window.xTickFontSize = "12px";
window.yTickFontSize = "12px";

async function loadAllCSVData() {
    try {
        const [
            managerInfo,
            manager_disk_usage,
            taskDone,
            taskConcurrency,
            taskFailedOnManager,
            taskFailedOnWorker,
            workerDiskUpdate,
            workerConcurrency,
            workerSummary,
            fileInfo,
            categoryInfo,
            graphInfo,
        ] = await Promise.all([
            fetchCSVData(window.logName, "manager_info.csv"),
            fetchCSVData(window.logName, "manager_disk_usage.csv"),
            fetchCSVData(window.logName, "task_done.csv"),
            fetchCSVData(window.logName, "task_concurrency.csv"),
            fetchCSVData(window.logName, "task_failed_on_manager.csv"),
            fetchCSVData(window.logName, "task_failed_on_worker.csv"),
            fetchCSVData(window.logName, "worker_disk_usage.csv"),
            fetchCSVData(window.logName, "worker_concurrency.csv"),
            fetchCSVData(window.logName, "worker_summary.csv"),
            fetchCSVData(window.logName, "file_info.csv"),
            fetchCSVData(window.logName, "category_info.csv"),
            fetchCSVData(window.logName, "graph_info.csv"),
        ]);

        window.managerInfo = managerInfo;
        window.managerDiskUsage = manager_disk_usage;
        window.taskDone = taskDone;
        window.taskConcurrency = taskConcurrency;
        window.taskFailedOnManager = taskFailedOnManager;
        window.taskFailedOnWorker = taskFailedOnWorker;
        window.workerDiskUpdate = workerDiskUpdate;
        window.workerConcurrency = workerConcurrency;
        window.workerSummary = workerSummary;
        window.fileInfo = fileInfo;
        window.categoryInfo = categoryInfo;
        window.graphInfo = graphInfo;

        window.managerInfo = window.window.managerInfo[0];
        window.time_manager_start = window.managerInfo.time_start;
        window.time_manager_end = window.managerInfo.time_end;
        window.when_first_task_start_commit = window.managerInfo.when_first_task_start_commit;
        window.when_last_task_done = window.managerInfo.when_last_task_done
        window.when_first_worker_connect = window.managerInfo.when_first_worker_connect;
        window.when_last_worker_disconnect = window.managerInfo.when_last_worker_disconnect

        window.minTime = window.when_first_worker_connect;
        window.maxTime = window.when_last_worker_disconnect;

        // window.minTime = window.when_first_task_start_commit;
        // window.maxTime = window.when_last_task_done;

    } catch (error) {
        console.error("Error loading data:", error);
    }
}

async function handleLogChange() {
    window.logName = this.value;

    // update the url
    window.parent.history.pushState({}, '', `/logs/${window.logName}`);

    // remove all the svgs
    var svgs = d3.selectAll('svg');
    svgs.each(function() {
        d3.select(this).selectAll('*').remove();
    });

    // hidden some divs
    const headerTips = window.parent.document.getElementsByClassName('error-tip');
    for (let i = 0; i < headerTips.length; i++) {
        headerTips[i].style.display = 'none';
    }

    // load all csv data and set global variables
    await loadAllCSVData();

    window.parent.document.dispatchEvent(new Event('dataLoaded'));
}

window.addEventListener('load', function() {
    const logSelector = window.parent.document.getElementById('log-selector');

    // Get the log name from the URL (in case of refresh)
    const currentPath = window.parent.location.pathname;
    const currentLogName = currentPath.split('/')[2];

    if (currentLogName) {
        window.logName = currentLogName;
        logSelector.value = currentLogName;
    }
    
    // Bind the change event listener to logSelector
    logSelector.addEventListener('change', handleLogChange);

    // Initialize the report iframe if the logSelector has an initial value
    if (logSelector.value) {
        logSelector.dispatchEvent(new Event('change'));
    }
});
