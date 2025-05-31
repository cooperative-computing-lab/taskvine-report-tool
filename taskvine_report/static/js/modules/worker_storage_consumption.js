import { BaseModule } from './base.js';
import { escapeWorkerId, getWorkerColor } from './utils.js';


export class WorkerStorageConsumptionModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    legendOnToggle(id, visible) {
        const path = this.svg.selectAll(`#${this.id}-storage-${id}`);
        path.style('display', visible ? null : 'none');
    }

    initLegend() {
        const legendItems = Object.keys(this.data.storage_data).map((worker, idx) => ({
            id: escapeWorkerId(worker),
            label: worker,
            color: getWorkerColor(worker, idx)
        }));
        this.createLegendRow(legendItems, {
            checkboxName: 'storage-consumption',
            onToggle: (id, visible) => {
                this.legendOnToggle(id, visible);
            }
        });
    }

    plot() {
        if (!this.data) return;
        
        Object.entries(this.data.storage_data).forEach(([worker, points], idx) => {
            const safeId = escapeWorkerId(worker);
            const color = getWorkerColor(worker, idx);
            this.plotPath(points, {
                stroke: color,
                className: 'storage-line',
                id: `${this.id}-storage-${safeId}`,
                tooltipInnerHTML: `${worker}`
            });
        });
    }
}
