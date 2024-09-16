import { setupZoomAndScroll, fetchFile, fetchCSVData } from './tools.js';
import { plotExecutionDetails } from './execution_details.js';
import { plotWorkerDiskUsage } from './worker_disk_usage.js';


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

        window.managerInfo = await fetchCSVData("manager_info.csv");
        window.taskDone = await fetchCSVData("task_done.csv");
        window.taskConcurrency = await fetchCSVData("task_concurrency.csv");
        window.taskFailedOnManager = await fetchCSVData("task_failed_on_manager.csv");
        window.taskFailedOnWorker = await fetchCSVData("task_failed_on_worker.csv");
        window.workerDiskUpdate = await fetchCSVData("worker_disk_usage.csv");
        window.workerConcurrency = await fetchCSVData("worker_concurrency.csv");
        window.workerSummary = await fetchCSVData("worker_summary.csv");
        window.fileInfo = await fetchCSVData("file_info.csv");
        window.categoryInfo = await fetchCSVData("category_info.csv");
        window.graphInfo = await fetchCSVData("graph_info.csv");

        window.managerInfo = window.window.managerInfo[0];
        window.time_manager_start = window.managerInfo.time_start;
        window.time_manager_end = window.managerInfo.time_end;

        window.minTime = window.time_manager_start;
        window.maxTime = window.time_manager_end;

        window.parent.document.dispatchEvent(new Event('dataLoaded'));

        try {
            plotExecutionDetails();
            setupZoomAndScroll('#execution-details', '#execution-details-container');
        
            plotWorkerDiskUsage({displayDiskUsageByPercentage: false});

        } catch (error) {
            console.error('Error fetching data directory:', error);
        }
    }

    // Bind the change event listener to logSelector
    const logSelector = window.parent.document.getElementById('log-selector');
    logSelector.addEventListener('change', handleLogChange);

    // Initialize the report iframe if the logSelector has an initial value
    if (logSelector.value) {
        logSelector.dispatchEvent(new Event('change'));
    }
});
