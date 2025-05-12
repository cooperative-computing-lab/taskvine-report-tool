import { BaseModule } from './base.js';
import { escapeWorkerId, getWorkerColor } from './utils.js';

export class WorkerWaitingRetrievalTasksModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    initLegend() {
        const legendItems = Object.keys(this.data.waiting_retrieval_tasks_data).map((worker, idx) => ({
            id: escapeWorkerId(worker),
            label: worker,
            color: getWorkerColor(worker, idx)
        }));
        this.createLegendRow(legendItems, {
            lineWidth: 4,
            checkboxName: 'worker-waiting-retrieval-tasks',
            onToggle: async (id, visible) => {
                const path = this.svg.selectAll(`#waiting-retrieval-${id}`);
                path.style('display', visible ? null : 'none');
            }
        });
    }

    plot() {
        if (!this.data) return;
        this.initSVG();
        Object.entries(this.data.waiting_retrieval_tasks_data).forEach(([worker, points], idx) => {
            const safeId = escapeWorkerId(worker);
            const color = getWorkerColor(worker, idx);
            this.plotPath(points, {
                stroke: color,
                className: 'waiting-retrieval-line',
                id: `waiting-retrieval-${safeId}`,
                tooltipInnerHTML: `${worker}`
            });
        });
    }
} 