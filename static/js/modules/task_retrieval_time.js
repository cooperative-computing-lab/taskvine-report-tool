import { BaseModule } from './base.js';

export class TaskRetrievalTimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;
        const svg = this.initSVG();
        this.plotPoints(this.data.points, {
            tooltipFormatter: d => `Task ID: ${d[0]}<br>Retrieval Time: ${eval(this.data.y_tick_formatter)(d[1])}`,
            className: 'task-retrieval-time-point'
        });
    }
}
