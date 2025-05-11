import { BaseModule } from './base.js';

export class TaskExecutionTimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');

        this.setBottomFormatter(d => `${d3.format('.0f')(d)}`);
        this.setLeftFormatter(d => `${d3.format('.2f')(d)} s`);

        this.PRIMARY_COLOR = '#2077B4';
        this.HIGHLIGHT_COLOR = 'orange';
    }

    async fetchData() {
        this.clearSVG();

        const response = await fetch(this.api_url);
        const data = await response.json();
        
        if (!data || !data.points) {
            console.warn('Invalid or missing task execution time data');
            return;
        }

        this.data = data;

        this.setBottomDomain(data.x_domain);
        this.setLeftDomain(data.y_domain);
        this.setBottomTickValues(data.x_tick_values);
        this.setLeftTickValues(data.y_tick_values);
    }

    plot() {
        if (!this.data) return;

        const svg = this.initSVG();

        /* plot execution time points */
        this.plotPoints(svg, this.data.points, {
            tooltipFormatter: d => `Task ID: ${d[0]}<br>Execution Time: ${d[1].toFixed(2)}s`,
            className: 'execution-time-point'
        });
    }
} 