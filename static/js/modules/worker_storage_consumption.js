import { BaseModule } from './base.js';

export class WorkerStorageConsumptionModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');

        this.setBottomFormatter(d => `${d3.format('.2f')(d)} s`);
        this.setLeftFormatter(d => `${d3.format('.2f')(d)} ${this.data?.file_size_unit || 'MB'}`);
    }

    async fetchData() {
        this.clearSVG();

        const response = await fetch(this.api_url);
        const data = await response.json();
        
        if (!data) {
            return;
        }

        this.data = data;

        this.setBottomDomain(data.x_domain);
        this.setLeftDomain(data.y_domain);
        this.setBottomTickValues(data.x_tick_values);
        this.setLeftTickValues(data.y_tick_values);
    }

    _getLegendColor(workerId) {
        const index = Object.keys(this.data['storage_data']).indexOf(workerId);
        return d3.schemeCategory10[index % 10];
    }

    initLegend() {
        if (!this.legendContainer) return;

        return;
    }

    _formatSize(size, unit) {
        if (size >= 1024 && unit === 'MB') {
            return `${(size/1024).toFixed(2)} GB`;
        }
        return `${size.toFixed(2)} ${unit}`;
    }

    plot() {
        if (!this.data || !this.data['storage_data']) return;

        const svg = this.initSVG();

        /* plot each worker's storage consumption */
        for (const [workerId, points] of Object.entries(this.data['storage_data'])) {
            if (points && points.length > 0) {
                this.plotPath(svg, points, {
                    stroke: this._getLegendColor(workerId),
                    strokeWidth: 1.0,
                    className: 'worker-storage',
                    id: workerId
                });
            }
        }
    }
}
