import { fetchFile } from './tools.js';

const factoryDescriptionContainer = document.getElementById('factory-description-container');

function fillMgrDescription() {
    var time_zone_offset_hours;
    if (window.managerInfo.time_zone_offset_hours >= 0) {
        time_zone_offset_hours = '+' + window.managerInfo.time_zone_offset_hours + ':00';
    } else {
        time_zone_offset_hours = window.managerInfo.time_zone_offset_hours + ':00';
    }

    document.getElementById('start-time').textContent = window.managerInfo.time_start_human + ` (UTC${time_zone_offset_hours})`;
    document.getElementById('end-time').textContent = window.managerInfo.time_end_human + ` (UTC${time_zone_offset_hours})`;
    document.getElementById('lift-time').textContent = window.managerInfo['lifetime(s)'] + 's';
    document.getElementById('tasks-submitted').textContent = window.managerInfo.tasks_submitted;
    document.getElementById('tasks-done').textContent = window.managerInfo.tasks_done;
    document.getElementById('tasks-waiting').textContent = window.managerInfo.tasks_failed_on_manager;
    document.getElementById('tasks-failed').textContent = window.managerInfo.tasks_failed_on_worker;
    document.getElementById('workers-connected').textContent = window.managerInfo.total_workers;
    document.getElementById('workers-active').textContent = window.managerInfo.active_workers;
    document.getElementById('max-concurrent-workers').textContent = window.managerInfo.max_concurrent_workers;
    document.getElementById('size-of-all-files').textContent = window.managerInfo['size_of_all_files(MB)'] + ' MB';
    document.getElementById('peak-disk-load').textContent = window.managerInfo['cluster_peak_disk_usage(MB)'] + ' MB';
}

async function fillFactoryDescription() {
    return;
    try {
        var factory = await fetchFile(`logs/${window.logName}/vine-logs/factory.json`);
        factory = JSON.parse(factory);
        factory = JSON.stringify(factory, null, 2);
        factoryDescriptionContainer.innerHTML = `<pre class="formatted-json"><b>factory.json</b> ${factory}</pre>`;
    } catch (error) {
        // pass
    }
}


window.parent.document.addEventListener('dataLoaded', function() {
    fillMgrDescription();
    fillFactoryDescription();
});