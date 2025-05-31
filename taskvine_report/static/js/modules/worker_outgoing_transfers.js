import { BaseModule } from './base.js';
import { escapeWorkerId, getWorkerColor } from './utils.js';


export class WorkerOutgoingTransfersModule extends BaseModule {
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
        return `${this.id}-outgoing-transfer-${escapeWorkerId(worker)}`;
    }

    initLegend() {
        const legendItems = Object.keys(this.data.transfers)
            .map((worker, idx) => ({
                id: this.getUniqueWorkerId(worker),
                label: worker,
                color: getWorkerColor(worker, idx)
            }))
            .sort((a, b) => a.label.localeCompare(b.label));
        this.createLegendRow(legendItems, {
            checkboxName: 'outgoing-transfers',
            onToggle: (id, visible) => {
                this.legendOnToggle(id, visible);
            }
        });
    }
    
    plot() {
        if (!this.data) return;

        Object.entries(this.data.transfers).forEach(([worker, points], idx) => {
            const color = getWorkerColor(worker, idx);
            this.plotPath(points, {
                stroke: color,
                className: 'outgoing-transfer-line',
                id: this.getUniqueWorkerId(worker),
                tooltipInnerHTML: `${worker}`,
            });
        });
    }
} 