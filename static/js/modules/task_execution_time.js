import { BaseModule } from './base.js';

export class TaskExecutionTimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('point');
    }

    plot() {
        if (!this.data) return;

        this.initSVG();

        this.plotPoints(this.data.points, {
            tooltipFormatter: d => `Task ID: ${d[0]}<br>Execution Time: ${d[1].toFixed(2)}s`,
            className: 'execution-time-point'
        });
    }
}