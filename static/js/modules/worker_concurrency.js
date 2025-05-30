import { BaseModule } from './base.js';

export class WorkerConcurrencyModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;

        this.plotPath(this.data.points, {
            stroke: '#2077B4',
            className: 'worker-concurrency-path',
            id: `${this.id}-worker-concurrency`,
        });
    }
} 