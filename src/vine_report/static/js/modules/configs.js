/**
 * Section visibility switch. Set to false to hide a section.
 * Omitted entries default to true (visible).
 */
export const sectionVisibility = {
    'task-execution-details': true,
    'task-concurrency': false,
    'task-throughput': true,
    'task-latency': true,
    'failure-timeline': true,
    'task-response-time': true,
    'task-execution-time': true,
    'task-retrieval-time': true,
    'task-completion-percentiles': false,
    'task-dependencies': false,
    'task-dependents': false,
    'task-subgraphs': false,
    'worker-concurrency': true,
    'worker-capacity': true,
    'worker-churn': true,
    'worker-storage-consumption': true,
    'worker-incoming-transfers': true,
    'worker-outgoing-transfers': true,
    'transfer-throughput': true,
    'worker-executing-tasks': true,
    'worker-waiting-retrieval-tasks': false,
    'worker-lifetime': true,
    'file-sizes': true,
    'file-concurrent-replicas': true,
    'file-retention-time': true,
    'file-transferred-size': true,
    'file-created-size': true,
};

export const sectionDescriptions = {
    'task-throughput': 'Shows how quickly application task attempts finish. Compare successful and failed rates to identify slowdowns or bursts of failure.',
    'task-latency': 'Compares the queue, execution, and retrieval time distributions. Curves farther to the left indicate lower latency.',
    'failure-timeline': 'Groups failed application attempts by outcome over the run, making failure clusters and recurring causes easier to spot.',
    'task-execution-details': 'Places every recorded task attempt on its worker and time axis. Use it to find idle gaps, long-running work, and retries.',
    'task-concurrency': 'Tracks how many tasks occupy each lifecycle state as the workflow progresses.',
    'task-response-time': 'Shows the recorded delay from a task becoming ready until it runs or fails before dispatch.',
    'task-execution-time': 'Shows the time each task attempt spent executing, helping reveal outliers and uneven workloads.',
    'task-retrieval-time': 'Shows how long completed task outputs remained in retrieval before reaching the manager.',
    'task-completion-percentiles': 'Shows when successive percentages of tasks completed relative to the beginning of the run.',
    'task-dependencies': 'Shows how many upstream producer tasks each task depends on through its input files.',
    'task-dependents': 'Shows how many downstream tasks consume each task’s output files.',
    'task-subgraphs': 'Summarizes connected task groups inferred from input and output relationships.',
    'worker-concurrency': 'Tracks the number of connected workers over time, revealing scale-up, scale-down, and availability gaps.',
    'worker-capacity': 'Compares available cores, committed cores, and running tasks over time. Committed cores represent scheduler occupancy, not CPU utilization.',
    'worker-churn': 'Tracks active workers as connections open and close, making unstable worker populations visible.',
    'worker-storage-consumption': 'Shows the amount of TaskVine-managed file data retained on each worker over time.',
    'worker-incoming-transfers': 'Shows concurrent transfers arriving at each worker, helping identify receivers under sustained transfer pressure.',
    'worker-outgoing-transfers': 'Shows concurrent transfers leaving each worker, helping identify heavily used data sources.',
    'transfer-throughput': 'Shows completed input and output payload bytes per second. This is payload throughput, not physical network utilization.',
    'worker-executing-tasks': 'Shows how many tasks each worker executes over time, revealing load balance and idle capacity.',
    'worker-waiting-retrieval-tasks': 'Shows completed tasks waiting for output retrieval on each worker.',
    'worker-lifetime': 'Compares how long workers remained connected to the manager during the run.',
    'file-sizes': 'Compares the sizes of files observed in the workflow and highlights unusually large data objects.',
    'file-concurrent-replicas': 'Shows the maximum number of simultaneous replicas observed for each file.',
    'file-retention-time': 'Shows how long each file remained present across worker transfers, from first stage-in to last stage-out.',
    'file-transferred-size': 'Tracks the cumulative volume of file data transferred as the workflow progresses.',
    'file-created-size': 'Tracks the cumulative size of files first observed or produced during the run.',
};

import { TaskExecutionDetailsModule } from './task_execution_details.js?v=5.0';
import { TaskConcurrencyModule } from './task_concurrency.js?v=5.0';
import { TaskResponseTimeModule } from './task_response_time.js?v=5.0';
import { TaskExecutionTimeModule } from './task_execution_time.js?v=5.0';
import { TaskCompletionPercentilesModule } from './task_completion_percentiles.js?v=5.0';
import { TaskDependenciesModule } from './task_dependencies.js?v=5.0';
import { TaskDependentsModule } from './task_dependents.js?v=5.0';
import { TaskRetrievalTimeModule } from './task_retrieval_time.js?v=5.0';
import { TaskSubgraphsModule } from './task_subgraphs.js?v=5.0';
import { WorkerIncomingTransfersModule } from './worker_incoming_transfers.js?v=5.0';
import { WorkerOutgoingTransfersModule } from './worker_outgoing_transfers.js?v=5.0';
import { WorkerStorageConsumptionModule } from './worker_storage_consumption.js?v=5.0';
import { WorkerConcurrencyModule } from './worker_concurrency.js?v=5.0';
import { WorkerExecutinigTasksModule } from './worker_executing_tasks.js?v=5.0';
import { WorkerWaitingRetrievalTasksModule } from './worker_waiting_retrieval_tasks.js?v=5.0';
import { WorkerLifetimeModule } from './worker_lifetime.js?v=5.0';
import { FileTransferredSizeModule } from './file_transferred_size.js?v=5.0';
import { FileCreatedSizeModule } from './file_created_size.js?v=5.0';
import { FileRetentionTimeModule } from './file_retention_time.js?v=5.0';
import { FileSizesModule } from './file_sizes.js?v=5.0';
import { FileConcurrentReplicasModule } from './file_concurrent_replicas.js?v=5.0';
import { SourceMetricModule } from './source_metric.js?v=5.3';

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
    'task-throughput': SourceMetricModule,
    'task-latency': SourceMetricModule,
    'failure-timeline': SourceMetricModule,
    'worker-capacity': SourceMetricModule,
    'worker-churn': SourceMetricModule,
    'transfer-throughput': SourceMetricModule,
};

export const moduleConfigs = [
    { id: 'task-throughput', group: 'tasks', timeLinked: true, title: 'Application Throughput', api_url: '' },
    { id: 'task-latency', group: 'tasks', title: 'Task Latency Distribution', api_url: '' },
    { id: 'failure-timeline', group: 'tasks', timeLinked: true, title: 'Failure Timeline', api_url: '' },
    { id: 'task-execution-details', group: 'tasks', timeLinked: true, title: 'Task Execution Timeline', api_url: '/api/task-execution-details' },
    { id: 'task-concurrency', group: 'tasks', timeLinked: true, title: 'Task Concurrency', api_url: '/api/task-concurrency' },
    { id: 'task-response-time', group: 'tasks', title: 'Task Response Time', api_url: '/api/task-response-time' },
    { id: 'task-execution-time', group: 'tasks', title: 'Task Execution Time', api_url: '/api/task-execution-time' },
    { id: 'task-retrieval-time', group: 'tasks', title: 'Task Retrieval Time', api_url: '/api/task-retrieval-time' },
    { id: 'task-completion-percentiles', group: 'tasks', title: 'Task Completion Percentiles', api_url: '/api/task-completion-percentiles' },
    { id: 'task-dependencies', group: 'tasks', title: 'Task Dependencies', api_url: '/api/task-dependencies' },
    { id: 'task-dependents', group: 'tasks', title: 'Task Dependents', api_url: '/api/task-dependents' },
    { id: 'task-subgraphs', group: 'tasks', title: 'Task Subgraphs', api_url: '/api/task-subgraphs' },
    { id: 'worker-concurrency', group: 'workers', timeLinked: true, title: 'Worker Concurrency', api_url: '/api/worker-concurrency' },
    { id: 'worker-capacity', group: 'workers', timeLinked: true, title: 'Worker Capacity', api_url: '' },
    { id: 'worker-churn', group: 'workers', timeLinked: true, title: 'Worker Churn', api_url: '' },
    { id: 'worker-storage-consumption', group: 'workers', timeLinked: true, title: 'Worker Storage Consumption', api_url: '/api/worker-storage-consumption' },
    { id: 'worker-incoming-transfers', group: 'transfers', timeLinked: true, title: 'Incoming Transfers', api_url: '/api/worker-incoming-transfers' },
    { id: 'worker-outgoing-transfers', group: 'transfers', timeLinked: true, title: 'Outgoing Transfers', api_url: '/api/worker-outgoing-transfers' },
    { id: 'transfer-throughput', group: 'transfers', timeLinked: true, title: 'Transfer Throughput', api_url: '' },
    { id: 'worker-executing-tasks', group: 'workers', timeLinked: true, title: 'Executing Tasks by Worker', api_url: '/api/worker-executing-tasks' },
    { id: 'worker-waiting-retrieval-tasks', group: 'workers', timeLinked: true, title: 'Waiting Retrieval by Worker', api_url: '/api/worker-waiting-retrieval-tasks' },
    { id: 'worker-lifetime', group: 'workers', title: 'Worker Lifetime', api_url: '/api/worker-lifetime' },
    { id: 'file-sizes', group: 'files', title: 'File Sizes', api_url: '/api/file-sizes' },
    { id: 'file-concurrent-replicas', group: 'files', title: 'Concurrent Replicas', api_url: '/api/file-concurrent-replicas' },
    { id: 'file-retention-time', group: 'files', title: 'File Retention Time', api_url: '/api/file-retention-time' },
    { id: 'file-transferred-size', group: 'files', timeLinked: true, title: 'Cumulative Data Transferred', api_url: '/api/file-transferred-size' },
    { id: 'file-created-size', group: 'files', timeLinked: true, title: 'Cumulative Data Created', api_url: '/api/file-created-size' },
];

/** Module configs filtered by sectionVisibility (only enabled sections) */
export const enabledModuleConfigs = moduleConfigs.filter(
    (c) => sectionVisibility[c.id] !== false
).map((config) => ({ ...config, description: sectionDescriptions[config.id] || '' }));
