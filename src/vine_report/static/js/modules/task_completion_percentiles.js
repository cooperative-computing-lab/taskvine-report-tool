import { BaseModule } from './base.js';

export class TaskCompletionPercentilesModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this._hasScatterPlot = true;
        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;

        const xFormatter = eval(this.data['x_tick_formatter']);

        this.plotPath(this.data.points, {
            stroke: '#2077B4',
            strokeWidth: 1.5,
            className: 'task-completion-percentiles-path',
            id: `${this.id}-path`,
            curveType: d3.curveLinear,
            disableHover: true,
        });

        this.plotPoints(this.data.points, {
            radius: 2,
            color: '#2077B4',
            tooltipFormatter: d => `Time: ${xFormatter(d[0])}<br>Percentile: ${d[1]}%`,
            className: 'task-completion-percentiles-point',
        });
    }
} 