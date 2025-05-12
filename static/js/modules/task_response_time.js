import { BaseModule } from './base.js';

export class TaskResponseTimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;

        const svg = this.initSVG();

        this.plotPoints(svg, this.data['points'], {
            tooltipFormatter: d => `Task ID: ${d[0]}<br>Response Time: ${d[1].toFixed(2)}s`,
            className: 'task-response-time-point'
        });
    }
} 