import { TaskExecutionDetailsModule } from './task_execution_details.js';
import { WorkerStorageConsumptionModule } from './worker_storage_consumption.js';
import { TaskConcurrencyModule } from './task_concurrency.js';
import { TaskResponseTimeModule } from './task_response_time.js';
import { TaskExecutionTimeModule } from './task_execution_time.js';
import { WorkerIncomingTransfersModule } from './worker_incoming_transfers.js';
import { WorkerOutgoingTransfersModule } from './worker_outgoing_transfers.js';
import { FileSizesModule } from './file_sizes.js';


export const moduleClasses = {
    'task-execution-details': TaskExecutionDetailsModule,
    'task-concurrency': TaskConcurrencyModule,
    'task-response-time': TaskResponseTimeModule,
    'task-execution-time': TaskExecutionTimeModule,
    'worker-storage-consumption': WorkerStorageConsumptionModule,
    'worker-incoming-transfers': WorkerIncomingTransfersModule,
    'worker-outgoing-transfers': WorkerOutgoingTransfersModule,
    'file-sizes': FileSizesModule,
};

export const moduleConfigs = [
    { id: 'task-execution-details', title: 'Task Execution Details', api_url: '/api/task-execution-details' },
    { id: 'task-concurrency', title: 'Task Concurrency', api_url: '/api/task-concurrency' },
    { id: 'task-response-time', title: 'Task Response Time', api_url: '/api/task-response-time' },
    { id: 'task-execution-time', title: 'Task Execution Time', api_url: '/api/task-execution-time' },
    { id: 'worker-storage-consumption', title: 'Worker Storage Consumption', api_url: '/api/worker-storage-consumption' },
    { id: 'worker-incoming-transfers', title: 'Worker Incoming Transfers', api_url: '/api/worker-incoming-transfers' },
    { id: 'worker-outgoing-transfers', title: 'Worker Outgoing Transfers', api_url: '/api/worker-outgoing-transfers' },
    { id: 'file-sizes', title: 'File Sizes', api_url: '/api/file-sizes' },
];
