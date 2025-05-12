import { BaseModule } from './base.js';

export class TaskExecutionTimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    initLegend() {
        
    }

    plot() {
        if (!this.data) return;

        const svg = this.initSVG();

        this.plotPoints(svg, this.data.points, {
            tooltipFormatter: d => `Task ID: ${d[0]}<br>Execution Time: ${d[1].toFixed(2)}s`,
            className: 'execution-time-point'
        });
    }
} 