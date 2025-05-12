import { BaseModule } from './base.js';
import { escapeWorkerId, getWorkerColor } from './utils.js';


export class WorkerStorageConsumptionModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;
        const svg = this.initSVG();
        
        /* plot each worker's storage consumption with unique color */
        Object.entries(this.data.storage_data).forEach(([worker, points], idx) => {
            const safeId = escapeWorkerId(worker);
            const color = getWorkerColor(worker, idx);
            this.plotPath(svg, points, {
                stroke: color,
                className: 'storage-line',
                id: `storage-${safeId}`,
                tooltipInnerHTML: `${worker}`
            });
        });

        /* add legend */
        const legendContainer = document.getElementById('worker-storage-consumption-legend');
        if (legendContainer) {
            legendContainer.innerHTML = '';
            const legendItems = Object.keys(this.data.storage_data).map((worker, idx) => ({
                id: escapeWorkerId(worker),
                label: worker,
                color: getWorkerColor(worker, idx)
            }));
            this.createLegendRow(legendContainer, legendItems, {
                lineWidth: 4,
                checkboxName: 'storage-consumption',
                onToggle: async (id, visible) => {
                    const path = svg.selectAll(`#storage-${id}`);
                    path.style('display', visible ? null : 'none');
                }
            });
        }
    }
}
