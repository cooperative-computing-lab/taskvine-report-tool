import { BaseModule } from './base.js';

export class WorkerLifetimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this._hasScatterPlot = true;
        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;

        const yFormatter = eval(this.data['y_tick_formatter']);
        this.plotPoints(this.data['points'], {
            tooltipFormatter: d => `Worker: ${this.data['idx_to_worker_key'][d[0]] ?? d[0]}<br>Lifetime: ${yFormatter(d[1])}`,
            className: 'worker-lifetime-point',
        });
    }
}