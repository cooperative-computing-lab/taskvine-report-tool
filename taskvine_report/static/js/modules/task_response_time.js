import { BaseModule } from './base.js';

export class TaskResponseTimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    legendOnToggle(id, visible) {
        const points = this.svg.selectAll(`circle.task-response-time-point-${id}`);
        points.style('display', visible ? null : 'none');
    }

    initLegend() {
        /** get counts from backend data */
        const dispatchedCount = this.data['dispatched_count'];
        const undispatchedCount = this.data['undispatched_count'];

        const legendItems = [
            {
                id: 'dispatched-tasks',
                label: `Dispatched Tasks (${dispatchedCount})`,
                color: 'steelblue',
                checked: true
            },
            {
                id: 'undispatched-tasks',
                label: `Undispatched Tasks (${undispatchedCount})`,
                color: 'red',
                checked: true
            }
        ];

        this.createLegendRow(legendItems, {
            checkboxName: 'task-response-time',
            onToggle: (id, visible) => {
                this.legendOnToggle(id, visible);
            }
        });
    }

    plot() {
        if (!this.data) return;

        /** Split points into dispatched and undispatched */
        const dispatchedPoints = this.data['points'].filter(p => p[4]);
        const undispatchedPoints = this.data['points'].filter(p => !p[4]);

        /** Plot dispatched tasks */
        this.plotPoints(dispatchedPoints, {
            tooltipFormatter: d => `Global Index: ${d[0]}<br>Task ID: ${d[2]}<br>Try ID: ${d[3]}<br>Response Time: ${d[1].toFixed(2)}s<br>Status: Dispatched`,
            className: 'task-response-time-point-dispatched-tasks',
            color: 'steelblue'
        });

        /** Plot undispatched tasks */
        this.plotPoints(undispatchedPoints, {
            tooltipFormatter: d => `Global Index: ${d[0]}<br>Task ID: ${d[2]}<br>Try ID: ${d[3]}<br>Response Time: ${d[1].toFixed(2)}s<br>Status: Undispatched`,
            className: 'task-response-time-point-undispatched-tasks',
            color: 'red'
        });
    }
} 