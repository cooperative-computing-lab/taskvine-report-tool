/**
 * Section visibility switch. Set to false to hide a section.
 * Omitted entries default to true (visible).
 */
export const sectionVisibility = {
    'task-execution-details': true,
    'task-concurrency': true,
    'task-response-time': true,
    'task-execution-time': true,
    'task-retrieval-time': true,
    'task-completion-percentiles': false,
    'task-dependencies': false,
    'task-dependents': false,
    'task-subgraphs': false,
    'worker-concurrency': true,
    'worker-storage-consumption': true,
    'worker-incoming-transfers': true,
    'worker-outgoing-transfers': true,
    'worker-executing-tasks': true,
    'worker-waiting-retrieval-tasks': false,
    'worker-lifetime': true,
    'file-sizes': true,
    'file-concurrent-replicas': true,
    'file-retention-time': true,
    'file-transferred-size': true,
    'file-created-size': true,
};

import { TaskExecutionDetailsModule } from './task_execution_details.js';
import { TaskConcurrencyModule } from './task_concurrency.js';
import { TaskResponseTimeModule } from './task_response_time.js';
import { TaskExecutionTimeModule } from './task_execution_time.js';
import { TaskCompletionPercentilesModule } from './task_completion_percentiles.js';
import { TaskDependenciesModule } from './task_dependencies.js';
import { TaskDependentsModule } from './task_dependents.js';
import { TaskRetrievalTimeModule } from './task_retrieval_time.js';
import { TaskSubgraphsModule } from './task_subgraphs.js';
import { WorkerIncomingTransfersModule } from './worker_incoming_transfers.js';
import { WorkerOutgoingTransfersModule } from './worker_outgoing_transfers.js';
import { WorkerStorageConsumptionModule } from './worker_storage_consumption.js';
import { WorkerConcurrencyModule } from './worker_concurrency.js';
import { WorkerExecutinigTasksModule } from './worker_executing_tasks.js';
import { WorkerWaitingRetrievalTasksModule } from './worker_waiting_retrieval_tasks.js';
import { WorkerLifetimeModule } from './worker_lifetime.js';
import { FileTransferredSizeModule } from './file_transferred_size.js';
import { FileCreatedSizeModule } from './file_created_size.js';
import { FileRetentionTimeModule } from './file_retention_time.js';
import { FileSizesModule } from './file_sizes.js';
import { FileConcurrentReplicasModule } from './file_concurrent_replicas.js';

export const moduleClasses = {
    'task-execution-details': TaskExecutionDetailsModule,
    'task-concurrency': TaskConcurrencyModule,
    'task-response-time': TaskResponseTimeModule,
    'task-execution-time': TaskExecutionTimeModule,
    'task-retrieval-time': TaskRetrievalTimeModule,
    'task-completion-percentiles': TaskCompletionPercentilesModule,
    'task-dependencies': TaskDependenciesModule,
    'task-dependents': TaskDependentsModule,
    'task-subgraphs': TaskSubgraphsModule,
    'worker-storage-consumption': WorkerStorageConsumptionModule,
    'worker-concurrency': WorkerConcurrencyModule,
    'worker-incoming-transfers': WorkerIncomingTransfersModule,
    'worker-outgoing-transfers': WorkerOutgoingTransfersModule,
    'worker-executing-tasks': WorkerExecutinigTasksModule,
    'worker-waiting-retrieval-tasks': WorkerWaitingRetrievalTasksModule,
    'worker-lifetime': WorkerLifetimeModule,
    'file-sizes': FileSizesModule,
    'file-concurrent-replicas': FileConcurrentReplicasModule,
    'file-retention-time': FileRetentionTimeModule,
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
    { id: 'task-dependencies', title: 'Task Dependencies', api_url: '/api/task-dependencies' },
    { id: 'task-dependents', title: 'Task Dependents', api_url: '/api/task-dependents' },
    { id: 'task-subgraphs', title: 'Task Subgraphs', api_url: '/api/task-subgraphs' },
    { id: 'worker-concurrency', title: 'Worker Concurrency', api_url: '/api/worker-concurrency' },
    { id: 'worker-storage-consumption', title: 'Worker Storage Consumption', api_url: '/api/worker-storage-consumption' },
    { id: 'worker-incoming-transfers', title: 'Worker Incoming Transfers', api_url: '/api/worker-incoming-transfers' },
    { id: 'worker-outgoing-transfers', title: 'Worker Outgoing Transfers', api_url: '/api/worker-outgoing-transfers' },
    { id: 'worker-executing-tasks', title: 'Worker Executing Tasks', api_url: '/api/worker-executing-tasks' },
    { id: 'worker-waiting-retrieval-tasks', title: 'Worker Waiting Retrieval Tasks', api_url: '/api/worker-waiting-retrieval-tasks' },
    { id: 'worker-lifetime', title: 'Worker Lifetime', api_url: '/api/worker-lifetime' },
    { id: 'file-sizes', title: 'File Sizes', api_url: '/api/file-sizes' },
    { id: 'file-concurrent-replicas', title: 'File Concurrent Replicas', api_url: '/api/file-concurrent-replicas' },
    { id: 'file-retention-time', title: 'File Retention Time', api_url: '/api/file-retention-time' },
    { id: 'file-transferred-size', title: 'File Transferred Size', api_url: '/api/file-transferred-size' },
    { id: 'file-created-size', title: 'File Created Size', api_url: '/api/file-created-size' },
];

/** Module configs filtered by sectionVisibility (only enabled sections) */
export const enabledModuleConfigs = moduleConfigs.filter(
    (c) => sectionVisibility[c.id] !== false
);
