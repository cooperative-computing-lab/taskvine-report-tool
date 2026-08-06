import { BaseModule } from './base.js';
import { escapeWorkerId, getWorkerColor } from './utils.js';

export class WorkerWaitingRetrievalTasksModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    legendOnToggle(id, visible) {
        const path = this.svg.selectAll(`#${id}`);
        path.style('display', visible ? null : 'none');
    }

    getUniqueWorkerId(worker) {
        return `${this.id}-waiting-retrieval-${escapeWorkerId(worker)}`;
    }

    initLegend() {
        const legendItems = Object.keys(this.data.waiting_retrieval_tasks_data).map((worker, idx) => ({
            id: this.getUniqueWorkerId(worker),
            label: worker,
            color: getWorkerColor(worker, idx)
        }));
        this.createLegendRow(legendItems, {
            checkboxName: 'worker-waiting-retrieval-tasks',
            onToggle: (id, visible) => {
                this.legendOnToggle(id, visible);
            }
        });
    }

    plot() {
        if (!this.data) return;

        Object.entries(this.data.waiting_retrieval_tasks_data).forEach(([worker, points], idx) => {
            const color = getWorkerColor(worker, idx);
            this.plotPath(points, {
                stroke: color,
                className: 'waiting-retrieval-line',
                id: this.getUniqueWorkerId(worker),
                tooltipInnerHTML: `${worker}`
            });
        });
    }
} 