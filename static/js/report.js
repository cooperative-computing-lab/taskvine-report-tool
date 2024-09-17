import { setupZoomAndScroll, fetchCSVData } from './tools.js';


async function loadAllCSVData() {
    try {
        const [
            managerInfo,
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
            fetchCSVData("manager_info.csv"),
            fetchCSVData("task_done.csv"),
            fetchCSVData("task_concurrency.csv"),
            fetchCSVData("task_failed_on_manager.csv"),
            fetchCSVData("task_failed_on_worker.csv"),
            fetchCSVData("worker_disk_usage.csv"),
            fetchCSVData("worker_concurrency.csv"),
            fetchCSVData("worker_summary.csv"),
            fetchCSVData("file_info.csv"),
            fetchCSVData("category_info.csv"),
            fetchCSVData("graph_info.csv"),
        ]);

        window.managerInfo = managerInfo;
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

    } catch (error) {
        console.error("Error loading data:", error);
    }
}

window.addEventListener('load', function() {
    
    async function handleLogChange() {
        window.logName = this.value;
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

        await loadAllCSVData();

        window.managerInfo = window.window.managerInfo[0];
        window.time_manager_start = window.managerInfo.time_start;
        window.time_manager_end = window.managerInfo.time_end;

        window.minTime = window.time_manager_start;
        window.maxTime = window.time_manager_end;

        window.parent.document.dispatchEvent(new Event('dataLoaded'));
    }

    // Bind the change event listener to logSelector
    const logSelector = window.parent.document.getElementById('log-selector');
    logSelector.addEventListener('change', handleLogChange);

    // Initialize the report iframe if the logSelector has an initial value
    if (logSelector.value) {
        logSelector.dispatchEvent(new Event('change'));
    }
});
