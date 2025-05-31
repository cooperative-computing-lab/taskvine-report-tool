import { BaseModule } from './base.js';

export class TaskDependentsModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;

        this.plotPoints(this.data['points'], {
            tooltipFormatter: d => `Task ID: ${d[0]}<br>Dependencies: ${d[1]}`,
            className: 'task-dependents-point'
        });
    }
}