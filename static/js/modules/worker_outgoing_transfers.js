import { BaseModule } from './base.js';
import { escapeWorkerId, getWorkerColor } from './utils.js';


export class WorkerOutgoingTransfersModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;

        const svg = this.initSVG();

        /* plot each worker's transfers with unique color */
        Object.entries(this.data.transfers).forEach(([worker, points], idx) => {
            const safeId = escapeWorkerId(worker);
            const color = getWorkerColor(worker, idx);
            this.plotPath(svg, points, {
                stroke: color,
                className: 'transfer-line',
                id: `transfer-${safeId}`,
                tooltipInnerHTML: `${worker}`,
            });
        });

        /* add legend */
        this.legendContainer.innerHTML = '';
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
                const path = svg.selectAll(`#transfer-${id}`);
                path.style('display', visible ? null : 'none');
            }
        });
    }
} 