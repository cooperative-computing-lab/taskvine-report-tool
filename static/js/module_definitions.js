import {
    initTaskExecutionDetails,
    onResizeTaskExecutionDetails,
} from './task_execution_details.js';


export const modules = [
    { id: 'task-execution-details', title: 'Task Execution Details' },
    /*
    { id: 'task-response-time', title: 'Task Response Time' },
    { id: 'task-execution-time', title: 'Task Execution Time' },
    { id: 'task-concurrency', title: 'Task Concurrency' },
    { id: 'worker-storage-consumption', title: 'Worker Storage Consumption' },
    { id: 'file-transfers', title: 'File Transfers' },
    { id: 'file-sizes', title: 'File Sizes' },
    { id: 'file-replicas', title: 'File Replicas' },
    { id: 'subgraphs', title: 'Subgraphs' }
    */
];

export const moduleRegistry = {
    'task-execution-details': {
        init: initTaskExecutionDetails,
        onResize: onResizeTaskExecutionDetails,
    },
    /*
    'task-response-time': {
        init: initTaskResponseTime,
        registerButtons: registerButtonsTaskResponseTime,
        registerLegend: registerLegendTaskResponseTime
    },
    'task-execution-time': {
        init: initTaskExecutionTime,
        registerButtons: registerButtonsTaskExecutionTime,
        registerLegend: registerLegendTaskExecutionTime
    },
    'task-concurrency': {
        init: initTaskConcurrency,
        registerButtons: registerButtonsTaskConcurrency,
        registerLegend: registerLegendTaskConcurrency
    },
    'worker-storage-consumption': {
        init: initWorkerStorageConsumption,
        registerButtons: registerButtonsWorkerStorageConsumption,
        registerLegend: registerLegendWorkerStorageConsumption
    },
    'file-transfers': {
        init: initFileTransfers,
        registerButtons: registerButtonsFileTransfers,
        registerLegend: registerLegendFileTransfers
    },
    'file-sizes': {
        init: initFileSizes,
        registerButtons: registerButtonsFileSizes,
        registerLegend: registerLegendFileSizes
    },
    'file-replicas': {
        init: initFileReplicas,
        registerButtons: registerButtonsFileReplicas,
        registerLegend: registerLegendFileReplicas
    },
    'subgraphs': {
        init: initSubgraphs,
        registerButtons: registerButtonsSubgraphs,
        registerLegend: registerLegendSubgraphs
    }
        */
};

export function renderModuleSkeleton({ id, title }) {
    const section = document.createElement('div');
    section.className = 'section';
    section.id = id;

    section.innerHTML = `
        <div class="section-header" id="${id}-header">
            <h2 class="section-title">${title}</h2>
            <div class="section-buttons" id="${id}-buttons">
                <button id="${id}-reset-button">Reset</button>
                <button id="${id}-download-button">Download</button>
            </div>
        </div>

        <div class="section-legend" id="${id}-legend"></div>
        <div class="section-controls" id="${id}-controls"></div>

        <div class="section-content">
            <div class="container-alpha" id="${id}-container">
                <div class="loading-spinner" id="${id}-loading"></div>
                <svg id="${id}" xmlns="http://www.w3.org/2000/svg"></svg>
            </div>
        </div>
    `;

    //const svgElement = section.querySelector(`#${id}`);
    //if (svgElement) {
    //    svgElement.style.width = '100%';
    //    svgElement.style.height = '100%';
    //}

    return section;
}

const debouncedResizeMap = new Map();

export function initModules() {
    const root = document.getElementById('content');
    if (!root) {
        console.error('Content root element not found');
        return;
    }

    modules.forEach(({ id, title }) => {
        try {
            const section = renderModuleSkeleton({ id, title });
            root.appendChild(section);

            const entry = moduleRegistry[id];
            if (!entry || typeof entry.init !== 'function') {
                throw new Error(`Module "${id}" is missing required registration functions`);
            }
            
            entry.init();

            if (!debouncedResizeMap.has(id)) {
                debouncedResizeMap.set(id, _.debounce(entry.onResize, 300));
            }
            window.addEventListener('resize', debouncedResizeMap.get(id));

            if (typeof entry.registerButtons === 'function') {
                entry.registerButtons();
            }

            if (typeof entry.registerLegend === 'function') {
                entry.registerLegend();
            }

            console.log(`Module "${id}" initialized successfully`);
        } catch (error) {
            console.error(`Failed to initialize module "${id}":`, error);
        }
    });

    const dataLoadedEvent = new CustomEvent('dataLoaded', {
        detail: {
            hideSpinner: (moduleId) => {
                const spinner = document.getElementById(`${moduleId}-loading`);
                if (spinner) {
                    spinner.style.display = 'none';
                }
            }
        }
    });
    window.document.dispatchEvent(dataLoadedEvent);
}
