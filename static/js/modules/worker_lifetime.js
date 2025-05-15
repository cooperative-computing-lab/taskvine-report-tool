import { BaseModule } from './base.js';

export class WorkerLifetimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this.setBottomScaleType('band');
        this.setLeftScaleType('linear');
    }

    async plot() {
        if (!this.data) return;
        this.initSVG();

        const barWidth = this.bottomScale.bandwidth() * 0.8;

        this.data['points'].forEach(([worker_idx, lifetime]) => {
            const x = this.bottomScale(worker_idx);
            const y = this.leftScale(lifetime);
            const height = -(this.leftScale(lifetime) - this.leftScale(0));

            this.plotRect(
                x,
                y,
                barWidth,
                height,
                'steelblue',
                1,
                `Worker: ${this.data['idx_to_worker_key'][worker_idx]}<br>Lifetime: ${lifetime} s`
            );
        });
    }
}