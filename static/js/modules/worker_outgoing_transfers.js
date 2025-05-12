import { BaseModule } from './base.js';
import { escapeWorkerId, getWorkerColor } from './utils.js';


export class WorkerOutgoingTransfersModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    initLegend() {
        const legendItems = Object.keys(this.data.transfers)
            .map((worker, idx) => ({
                id: escapeWorkerId(worker),
                label: worker,
                color: getWorkerColor(worker, idx)
            }))
            .sort((a, b) => a.label.localeCompare(b.label));
        this.createLegendRow(this.legendContainer, legendItems, {
            checkboxName: 'outgoing-transfers',
            onToggle: async (id, visible) => {
                const path = this.svg.selectAll(`#transfer-${id}`);
                path.style('display', visible ? null : 'none');
            }
        });
    }
    
    plot() {
        if (!this.data) return;

        this.initSVG();

        Object.entries(this.data.transfers).forEach(([worker, points], idx) => {
            const safeId = escapeWorkerId(worker);
            const color = getWorkerColor(worker, idx);
            this.plotPath(points, {
                stroke: color,
                className: 'transfer-line',
                id: `transfer-${safeId}`,
                tooltipInnerHTML: `${worker}`,
            });
        });
    }
} 