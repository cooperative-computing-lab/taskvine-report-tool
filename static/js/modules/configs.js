import { TaskExecutionDetailsModule } from './task_execution_details.js';
// import other modules here...

export const moduleClasses = {
    'task-execution-details': TaskExecutionDetailsModule,
    // 'task-response-time': TaskResponseTimeModule,
    // ...
};

export const moduleConfigs = [
    { id: 'task-execution-details', title: 'Task Execution Details', api_url: '/api/task-execution-details' },
    // ...
];
