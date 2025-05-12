
export function getTaskInnerHTML(task) {
    const precision = 3;

    const format = val =>
        typeof val === 'number' ? val.toFixed(precision) : 'N/A';

    return `
        Task ID: ${task.task_id}<br>
        Try ID: ${task.try_id}<br>
        Worker: ${task.worker_ip}:${task.worker_port}<br>
        Core ID: ${task.core_id}<br>
        Inputs: ${task.input_files || 'N/A'}<br>
        Outputs: ${task.output_files || 'N/A'}<br>
        When Ready: ${format(task.when_ready)}<br>
        When Running: ${format(task.when_running)}<br>
        When Start on Worker: ${format(task.time_worker_start)}<br>
        When End on Worker: ${format(task.time_worker_end)}<br>
        When Waiting Retrieval: ${format(task.when_waiting_retrieval)}<br>
        When Retrieved: ${format(task.when_retrieved)}<br>
        When Done: ${format(task.when_done)}<br>
        Task Status: ${task.task_status}<br>
    `;
}

export function getWorkerInnerHTML(worker) {
    return `
        cores: ${worker.cores}<br>
        worker id: ${worker.id}<br>
        worker ip port: ${worker.worker_ip_port}<br>
    `
}

export function escapeWorkerId(workerId) {
    return workerId.replace(/[.:]/g, '-');
}

const colorPalette = d3.schemeCategory10.concat(d3.schemeSet3).concat(d3.schemeTableau10);
const workerColorMap = {};
export function getWorkerColor(worker, idx) {
    if (!(worker in workerColorMap)) {
        workerColorMap[worker] = colorPalette[idx % colorPalette.length];
    }
    return workerColorMap[worker];
}