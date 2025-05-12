import { TaskExecutionDetailsModule } from './task_execution_details.js';
import { WorkerStorageConsumptionModule } from './worker_storage_consumption.js';
import { TaskConcurrencyModule } from './task_concurrency.js';
import { TaskResponseTimeModule } from './task_response_time.js';
import { TaskExecutionTimeModule } from './task_execution_time.js';
import { TaskCompletionPercentilesModule } from './task_completion_percentiles.js';
import { WorkerIncomingTransfersModule } from './worker_incoming_transfers.js';
import { WorkerOutgoingTransfersModule } from './worker_outgoing_transfers.js';
import { FileSizesModule } from './file_sizes.js';
import { FileReplicasModule } from './file_replicas.js';
import { TaskRetrievalTimeModule } from './task_retrieval_time.js';
import { WorkerConcurrencyModule } from './worker_concurrency.js';
import { WorkerExecutinigTasksModule } from './worker_executing_tasks.js';
import { WorkerWaitingRetrievalTasksModule } from './worker_waiting_retrieval_tasks.js';
import { FileTransferredSizeModule } from './file_transferred_size.js';
import { FileCreatedSizeModule } from './file_created_size.js';

export const moduleClasses = {
    'task-execution-details': TaskExecutionDetailsModule,
    'task-concurrency': TaskConcurrencyModule,
    'task-response-time': TaskResponseTimeModule,
    'task-execution-time': TaskExecutionTimeModule,
    'task-retrieval-time': TaskRetrievalTimeModule,
    'task-completion-percentiles': TaskCompletionPercentilesModule,
    'worker-storage-consumption': WorkerStorageConsumptionModule,
    'worker-concurrency': WorkerConcurrencyModule,
    'worker-incoming-transfers': WorkerIncomingTransfersModule,
    'worker-outgoing-transfers': WorkerOutgoingTransfersModule,
    'worker-executing-tasks': WorkerExecutinigTasksModule,
    'worker-waiting-retrieval-tasks': WorkerWaitingRetrievalTasksModule,
    'file-sizes': FileSizesModule,
    'file-replicas': FileReplicasModule,
    'file-transferred-size': FileTransferredSizeModule,
    'file-created-size': FileCreatedSizeModule,
};

export const moduleConfigs = [
    { id: 'task-execution-details', title: 'Task Execution Details', api_url: '/api/task-execution-details' },
    { id: 'task-concurrency', title: 'Task Concurrency', api_url: '/api/task-concurrency' },
    { id: 'task-response-time', title: 'Task Response Time', api_url: '/api/task-response-time' },
    { id: 'task-execution-time', title: 'Task Execution Time', api_url: '/api/task-execution-time' },
    { id: 'task-retrieval-time', title: 'Task Retrieval Time', api_url: '/api/task-retrieval-time' },
    { id: 'task-completion-percentiles', title: 'Task Completion Percentiles', api_url: '/api/task-completion-percentiles' },
    { id: 'worker-storage-consumption', title: 'Worker Storage Consumption', api_url: '/api/worker-storage-consumption' },
    { id: 'worker-concurrency', title: 'Worker Concurrency', api_url: '/api/worker-concurrency' },
    { id: 'worker-incoming-transfers', title: 'Worker Incoming Transfers', api_url: '/api/worker-incoming-transfers' },
    { id: 'worker-outgoing-transfers', title: 'Worker Outgoing Transfers', api_url: '/api/worker-outgoing-transfers' },
    { id: 'worker-executing-tasks', title: 'Worker Executing Tasks', api_url: '/api/worker-executing-tasks' },
    { id: 'worker-waiting-retrieval-tasks', title: 'Worker Waiting Retrieval Tasks', api_url: '/api/worker-waiting-retrieval-tasks' },
    { id: 'file-sizes', title: 'File Sizes', api_url: '/api/file-sizes' },
    { id: 'file-replicas', title: 'File Replicas', api_url: '/api/file-replicas' },
    { id: 'file-transferred-size', title: 'File Transferred Size', api_url: '/api/file-transferred-size' },
    { id: 'file-created-size', title: 'File Created Size', api_url: '/api/file-created-size' },
];
