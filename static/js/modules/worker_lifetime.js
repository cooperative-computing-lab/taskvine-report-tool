import { BaseModule } from './base.js';

export class WorkerLifetimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this.setBottomScaleType('band');
        this.setLeftScaleType('linear');
    }

    async plot() {
        if (!this.data) return;

        const xWidth = this.bottomScale.bandwidth() * 0.8;
        const yFormatter = eval(this.data['y_tick_formatter']);

        this.data['points'].forEach(([worker_idx, lifetime]) => {
            const innerHTML = `Worker: ${this.data['idx_to_worker_key'][worker_idx]}<br>Lifetime: ${yFormatter(lifetime)} s`;
            this.plotVerticalRect(worker_idx, xWidth, lifetime, 'steelblue', 1, innerHTML);
        });
    }
}