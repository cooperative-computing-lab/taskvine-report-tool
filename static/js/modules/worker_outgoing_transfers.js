import { BaseModule } from './base.js';
import { escapeWorkerId, getWorkerColor } from './utils.js';


export class WorkerOutgoingTransfersModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');

        this.setBottomFormatter(d => `${d3.format('.2f')(d)} s`);
        this.setLeftFormatter(d => `${d3.format('.0f')(d)}`);
    }

    async fetchData() {
        this.clearSVG();

        const response = await fetch(this.api_url);
        const data = await response.json();
        
        if (!data || !data.transfers) {
            console.warn('Invalid or missing worker outgoing transfers data');
            return;
        }

        this.data = data;

        this.setBottomDomain([data.x_domain[0], data.x_domain[1]]);
        this.setLeftDomain([data.y_domain[0], data.y_domain[1]]);
        this.setBottomTickValues(data.x_tick_values);
        this.setLeftTickValues(data.y_tick_values);
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
                id: `transfer-${safeId}`
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