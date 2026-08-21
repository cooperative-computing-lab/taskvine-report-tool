import { BaseModule } from './base.js?v=5.4';

const COLORS = ['#d96f24', '#747474', '#a9a9a5', '#4e4e4e', '#e6a06d', '#94948f'];

function finiteExtent(values) {
    const finite = values.map(Number).filter(Number.isFinite);
    if (!finite.length) return [0, 1];
    const lo = Math.min(...finite);
    const hi = Math.max(...finite);
    if (lo === hi) return [Math.min(0, lo), hi === 0 ? 1 : hi * 1.05];
    return [Math.min(0, lo), hi];
}

export class SourceMetricModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this.endpointKind = 'metrics';
        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
        this.seriesVisibility = new Map();
    }

    transformFetchedData(payload) {
        const contract = payload?.contract || {};
        const title = document.querySelector(`#${this.id}-header .section-title`);
        if (title) {
            const coverage = Number(contract.coverage);
            const coverageText = Number.isFinite(coverage) ? `; coverage ${(coverage * 100).toFixed(1)}%` : '';
            title.title = contract.source
                ? `Source: ${contract.source}${coverageText}. ${contract.caveat || ''}`.trim()
                : '';
        }
        if (!payload?.available) {
            const status = contract.status || 'UNAVAILABLE';
            const reason = contract.caveat || 'The required source evidence is unavailable.';
            return { error: `${status}: ${reason}` };
        }
        const series = (payload.series || []).filter(item => Array.isArray(item.points) && item.points.length);
        const allPoints = series.flatMap(item => item.points);
        if (!allPoints.length) return { error: 'No validated observations are available for this metric.' };
        const xDomain = finiteExtent(allPoints.map(point => point[0]));
        const yDomain = finiteExtent(allPoints.map(point => point[1]));
        if (payload.kind === 'cdf') {
            yDomain[1] = 1;
        } else {
            const span = Math.max(yDomain[1] - yDomain[0], Math.abs(yDomain[1]), 1);
            yDomain[1] += span * 0.05;
        }
        return {
            ...payload,
            series,
            x_domain: xDomain,
            y_domain: yDomain,
            x_tick_values: d3.ticks(xDomain[0], xDomain[1], 6),
            y_tick_values: d3.ticks(yDomain[0], yDomain[1], 6),
            x_tick_formatter: payload.metric_id === 'task-latency'
                ? '(d) => d < 1 ? `${Math.round(d * 1000)}ms` : d3.format("~s")(d) + "s"'
                : '(d) => d3.format("~s")(d) + "s"',
            y_tick_formatter: payload.kind === 'cdf'
                ? '(d) => d3.format(".0%")(d)'
                : payload.metric_id === 'transfer-throughput'
                    ? '(d) => d3.format("~s")(d) + "B/s"'
                    : payload.metric_id === 'task-throughput'
                        ? '(d) => d3.format(".2~f")(d)'
                    : '(d) => d3.format("~s")(d)',
        };
    }

    initLegend() {
        if (!this.data?.series) return;
        const items = this.data.series.map((series, index) => ({
            id: `${this.id}-series-${index}`,
            label: series.name,
            color: COLORS[index % COLORS.length],
        }));
        this.createLegendRow(items, {
            checkboxName: `${this.id}-series`,
            columnsPerRow: Math.min(4, items.length),
            onToggle: (id, visible) => {
                const index = Number(id.split('-').pop());
                this.seriesVisibility.set(index, visible);
                this.svg?.select(`#${id}`).style('display', visible ? null : 'none');
            },
        });
    }

    plot() {
        if (!this.data?.series) return;
        const cursorSeries = [];
        this.data.series.forEach((series, index) => {
            const id = `${this.id}-series-${index}`;
            this.plotPath(series.points, {
                id,
                className: `${this.id}-metric-line`,
                stroke: COLORS[index % COLORS.length],
                strokeWidth: index === 0 ? 2.2 : 1.8,
                curveType: this.data.kind === 'cdf' ? d3.curveMonotoneX : d3.curveStepAfter,
                disableHover: true,
            });
            cursorSeries.push({
                id,
                name: series.name,
                color: COLORS[index % COLORS.length],
                points: series.points,
            });
            if (this.seriesVisibility.get(index) === false) {
                this.svg.select(`#${id}`).style('display', 'none');
            }
        });
        this.installLineCursor(cursorSeries, {
            interpolation: this.data.kind === 'cdf' ? 'linear' : 'step',
            xFormatter: eval(this.data.x_tick_formatter),
            yFormatter: eval(this.data.y_tick_formatter),
            xLabel: this.data.axes?.x?.label || 'Time',
            xUnit: this.data.axes?.x?.unit || 'seconds',
            isVisible: (_series, index) => this.seriesVisibility.get(index) !== false,
        });
    }
}
