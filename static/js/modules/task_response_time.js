import { BaseModule } from './base.js';

export class TaskResponseTimeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);

        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');

        this.setBottomFormatter(d => `${d3.format('.0f')(d)}`);
        this.setLeftFormatter(d => `${d3.format('.2f')(d)} s`);

        this.PRIMARY_COLOR = '#2077B4';
        this.HIGHLIGHT_COLOR = 'orange';
        this.dotRadius = 1.5;
        this.highlightRadius = 3;
    }

    async fetchData() {
        this.clearSVG();

        const response = await fetch(this.api_url);
        const data = await response.json();
        
        if (!data || !data.points) {
            console.warn('Invalid or missing task response time data');
            return;
        }

        this.data = data;

        // Set domains and tick values
        this.setBottomDomain(data.x_domain);
        this.setLeftDomain(data.y_domain);
        this.setBottomTickValues(data.x_tick_values);
        this.setLeftTickValues(data.y_tick_values);
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