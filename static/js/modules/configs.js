import { TaskExecutionDetailsModule } from './task_execution_details.js';
import { WorkerStorageConsumptionModule } from './worker_storage_consumption.js';
import { TaskConcurrencyModule } from './task_concurrency.js';
import { TaskResponseTimeModule } from './task_response_time.js';
// import other modules here...

export const moduleClasses = {
    'task-execution-details': TaskExecutionDetailsModule,
    'task-concurrency': TaskConcurrencyModule,
    'task-response-time': TaskResponseTimeModule,
    'worker-storage-consumption': WorkerStorageConsumptionModule,
    // ...
};

export const moduleConfigs = [
    { id: 'task-execution-details', title: 'Task Execution Details', api_url: '/api/task-execution-details' },
    { id: 'task-concurrency', title: 'Task Concurrency', api_url: '/api/task-concurrency' },
    { id: 'task-response-time', title: 'Task Response Time', api_url: '/api/task-response-time' },
    { id: 'worker-storage-consumption', title: 'Worker Storage Consumption', api_url: '/api/worker-storage-consumption' },
    // ...
];
