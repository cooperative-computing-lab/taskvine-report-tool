import { TaskExecutionDetailsModule } from './task_execution_details.js';
import { WorkerStorageConsumptionModule } from './worker_storage_consumption.js';
// import other modules here...

export const moduleClasses = {
    'task-execution-details': TaskExecutionDetailsModule,
    'worker-storage-consumption': WorkerStorageConsumptionModule,
    // 'task-response-time': TaskResponseTimeModule,
    // ...
};

export const moduleConfigs = [
    { id: 'task-execution-details', title: 'Task Execution Details', api_url: '/api/task-execution-details' },
    { id: 'worker-storage-consumption', title: 'Worker Storage Consumption', api_url: '/api/worker-storage-consumption' },
    // ...
];
