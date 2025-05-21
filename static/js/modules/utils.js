export function getTaskInnerHTML(task) {
    const precision = 3;

    const format = val =>
        typeof val === 'number' ? val.toFixed(precision) : 'N/A';

    return `
        Task ID: ${task.task_id}<br>
        Try ID: ${task.try_id}<br>
        Worker: ${task.worker_entry}<br>
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
        worker entry: ${worker.worker_entry}<br>
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

export function updateSidebarButtons() {
    const sectionHeaders = Array.from(document.querySelectorAll('.section-header'));
    const sidebar = document.querySelector('#sidebar');
    
    sidebar.style.overflowX = 'auto';
    sidebar.style.whiteSpace = 'nowrap';

    const existingButtons = sidebar.querySelectorAll('.report-scroll-btn');
    existingButtons.forEach(btn => btn.remove());

    sectionHeaders.sort((a, b) => {
        return a.getBoundingClientRect().top - b.getBoundingClientRect().top;
    });

    sectionHeaders.forEach(header => {
        const title = header.querySelector('.section-title');
        if (title) {
            const button = document.createElement('button');
            button.textContent = title.textContent;
            button.classList.add('report-scroll-btn');
            button.addEventListener('click', () => {
                header.scrollIntoView({ behavior: 'smooth' });
            });
            sidebar.appendChild(button);
        }
    });
}