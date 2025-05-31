import { BaseModule } from './base.js';

export class TaskRetrievalTimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;

        this.plotPoints(this.data.points, {
            tooltipFormatter: d => `Global Index: ${d[0]}<br>Task ID: ${d[2]}<br>Try ID: ${d[3]}<br>Retrieval Time: ${eval(this.data.y_tick_formatter)(d[1])}`,
            className: 'task-retrieval-time-point'
        });
    }
}
