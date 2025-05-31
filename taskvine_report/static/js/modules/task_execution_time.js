import { BaseModule } from './base.js';

export class TaskExecutionTimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    initLegend() {
        /** get counts from backend data */
        const ranToCompletionCount = this.data['ran_to_completion_count'];
        const failedCount = this.data['failed_count'];

        const legendItems = [
            {
                id: 'ran-to-completion',
                label: `Ran to Completion (${ranToCompletionCount})`,
                color: 'steelblue',
                checked: true
            },
            {
                id: 'failed',
                label: `Failed (${failedCount})`,
                color: 'red',
                checked: true
            }
        ];

        this.createLegendRow(legendItems, {
            checkboxName: 'task-execution-time',
            onToggle: (id, visible) => {
                this.legendOnToggle(id, visible);
            }
        });
    }

    legendOnToggle(id, visible) {
        const points = this.svg.selectAll(`circle.task-execution-time-point-${id}`);
        points.style('display', visible ? null : 'none');
    }

    plot() {
        if (!this.data) return;

        /** Split points into ran to completion and failed */
        const ranToCompletionPoints = this.data['points'].filter(p => p[4]);
        const failedPoints = this.data['points'].filter(p => !p[4]);

        /** Plot ran to completion tasks */
        this.plotPoints(ranToCompletionPoints, {
            tooltipFormatter: d => `Global Index: ${d[0]}<br>Task ID: ${d[2]}<br>Try ID: ${d[3]}<br>Execution Time: ${d[1].toFixed(2)}s<br>Status: Ran to Completion`,
            className: 'task-execution-time-point-ran-to-completion',
            color: 'steelblue'
        });

        /** Plot failed tasks */
        this.plotPoints(failedPoints, {
            tooltipFormatter: d => `Global Index: ${d[0]}<br>Task ID: ${d[2]}<br>Try ID: ${d[3]}<br>Execution Time: ${d[1].toFixed(2)}s<br>Status: Failed`,
            className: 'task-execution-time-point-failed',
            color: 'red'
        });
    }
}