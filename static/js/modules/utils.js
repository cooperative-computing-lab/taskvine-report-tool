/* task failure types with bit values */
export const TASK_TYPE_FAILURES = [
    {value: 1, vineResult: 'VINE_RESULT_INPUT_MISSING', label: 'Input Missing', color: '#FFB6C1', checkboxName: 'unsuccessful-input-missing'},
    {value: 2, vineResult: 'VINE_RESULT_OUTPUT_MISSING', label: 'Output Missing', color: '#FF69B4', checkboxName: 'unsuccessful-output-missing'},
    {value: 4, vineResult: 'VINE_RESULT_STDOUT_MISSING', label: 'Stdout Missing', color: '#FF1493', checkboxName: 'unsuccessful-stdout-missing'},
    {value: 1 << 3, vineResult: 'VINE_RESULT_SIGNAL', label: 'Signal', color: '#CD5C5C', checkboxName: 'unsuccessful-signal'},
    {value: 2 << 3, vineResult: 'VINE_RESULT_RESOURCE_EXHAUSTION', label: 'Resource Exhaustion', color: '#8B0000', checkboxName: 'unsuccessful-resource-exhaustion'},
    {value: 3 << 3, vineResult: 'VINE_RESULT_MAX_END_TIME', label: 'Max End Time', color: '#B22222', checkboxName: 'unsuccessful-max-end-time'},
    {value: 4 << 3, vineResult: 'VINE_RESULT_UNKNOWN', label: 'Unknown', color: '#A52A2A', checkboxName: 'unsuccessful-unknown'},
    {value: 5 << 3, vineResult: 'VINE_RESULT_FORSAKEN', label: 'Forsaken', color: '#E331EE', checkboxName: 'unsuccessful-forsaken'},
    {value: 6 << 3, vineResult: 'VINE_RESULT_MAX_RETRIES', label: 'Max Retries', color: '#8B4513', checkboxName: 'unsuccessful-max-retries'},
    {value: 7 << 3, vineResult: 'VINE_RESULT_MAX_WALL_TIME', label: 'Max Wall Time', color: '#D2691E', checkboxName: 'unsuccessful-max-wall-time'},
    {value: 8 << 3, vineResult: 'VINE_RESULT_RMONITOR_ERROR', label: 'Monitor Error', color: '#FF4444', checkboxName: 'unsuccessful-monitor-error'},
    {value: 9 << 3, vineResult: 'VINE_RESULT_OUTPUT_TRANSFER_ERROR', label: 'Transfer Error', color: '#FF6B6B', checkboxName: 'unsuccessful-transfer-error'},
    {value: 10 << 3, vineResult: 'VINE_RESULT_FIXED_LOCATION_MISSING', label: 'Location Missing', color: '#FF8787', checkboxName: 'unsuccessful-location-missing'},
    {value: 11 << 3, vineResult: 'VINE_RESULT_CANCELLED', label: 'Cancelled', color: '#FFA07A', checkboxName: 'unsuccessful-cancelled'},
    {value: 12 << 3, vineResult: 'VINE_RESULT_LIBRARY_EXIT', label: 'Library Exit', color: '#FA8072', checkboxName: 'unsuccessful-library-exit'},
    {value: 13 << 3, vineResult: 'VINE_RESULT_SANDBOX_EXHAUSTION', label: 'Sandbox Exhaustion', color: '#E9967A', checkboxName: 'unsuccessful-sandbox-exhaustion'},
    {value: 14 << 3, vineResult: 'VINE_RESULT_MISSING_LIBRARY', label: 'Missing Library', color: '#F08080', checkboxName: 'unsuccessful-missing-library'},
    {value: 15 << 3, vineResult: 'WORKER_DISCONNECTED', label: 'Worker Disconnected', color: '#FF0000', checkboxName: 'unsuccessful-worker-disconnected' },
];

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