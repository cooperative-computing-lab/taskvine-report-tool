import { BaseModule } from './base.js?v=5.7';
import { getWorkerColor } from './utils.js?v=5.0';
import { durationAxisSpec } from './time_format.js?v=1.0';
import { metricContractDetails, metricContractError } from './metric_contract.js?v=1.0';

const COLORS = ['#d96f24', '#747474', '#a9a9a5', '#4e4e4e', '#e6a06d', '#94948f'];

function formatterExpression(unit) {
    const formats = {
        bytes: '(d) => { const a=Math.abs(d); if(a>=1099511627776)return d3.format(".2~f")(d/1099511627776)+" TiB"; if(a>=1073741824)return d3.format(".2~f")(d/1073741824)+" GiB"; if(a>=1048576)return d3.format(".2~f")(d/1048576)+" MiB"; if(a>=1024)return d3.format(".2~f")(d/1024)+" KiB"; return d3.format("~s")(d)+" B"; }',
        percent: '(d) => d3.format(".0f")(d) + "%"',
        ratio: '(d) => d3.format(".0%")(d)',
        count: '(d) => d3.format("~s")(d)',
        attempts_per_second: '(d) => d3.format(".2~f")(d)',
        bytes_per_second: '(d) => d3.format("~s")(d) + " B/s"',
    };
    return formats[unit] || formats.count;
}

function finiteDomain(domain) {
    if (!Array.isArray(domain) || domain.length !== 2) return [0, 1];
    const values = domain.map(Number);
    if (!values.every(Number.isFinite)) return [0, 1];
    if (values[0] === values[1]) values[1] = values[0] + 1;
    return values;
}

export class GenericMetricModule extends BaseModule {
    constructor(id, title, api_url, config = {}) {
        super(id, title, api_url);
        this.endpointKind = 'metrics';
        this.config = config;
        this._hasScatterPlot = ['scatter', 'line_points'].includes(config.renderer);
        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
        const defaultView = config.options?.default_view;
        if (defaultView) this.defineFetchDataParams({ view: defaultView });
        this.seriesVisibility = new Map();
    }

    transformFetchedData(payload) {
        const contract = payload?.contract || {};
        const title = document.querySelector(`#${this.id}-header .section-title`);
        if (title) {
            const coverage = Number(contract.coverage);
            const coverageText = Number.isFinite(coverage) ? `; coverage ${(coverage * 100).toFixed(1)}%` : '';
            const omitted = Number(payload?.metadata?.omitted_series || 0);
            const selectionText = omitted
                ? ` Showing ${payload.metadata.selected_series} of ${payload.metadata.available_series} series by ${payload.metadata.series_selection}.`
                : '';
            title.title = `Source: ${contract.source || this.config.source || 'unknown'}${coverageText}. ${metricContractDetails(contract)}${selectionText}`.trim();
        }
        if (!payload?.available || !Array.isArray(payload.series) || !payload.series.some(item => item.points?.length)) {
            return { error: metricContractError(contract, 'No observations are available for this metric.') };
        }
        const xDomain = finiteDomain(payload.axes?.x?.domain);
        const yDomain = finiteDomain(payload.axes?.y?.domain);
        const xAxis = payload.axes?.x?.unit === 'seconds' ? durationAxisSpec(xDomain) : null;
        const xFormatter = payload.axes?.x?.unit === 'count'
            ? '(d) => d3.format(",.0f")(d)'
            : formatterExpression(payload.axes?.x?.unit);
        const ySpan = Math.max(yDomain[1] - yDomain[0], Math.abs(yDomain[1]), 1);
        if (this.config.renderer === 'scatter') yDomain[1] += ySpan * 0.04;
        const yAxis = payload.axes?.y?.unit === 'seconds' ? durationAxisSpec(yDomain) : null;
        return {
            ...payload,
            x_domain: xDomain,
            y_domain: yDomain,
            x_tick_values: xAxis?.tickValues || d3.ticks(xDomain[0], xDomain[1], 6),
            y_tick_values: yAxis?.tickValues || d3.ticks(yDomain[0], yDomain[1], 6),
            x_tick_formatter: xAxis?.formatterExpression || xFormatter,
            y_tick_formatter: yAxis?.formatterExpression || formatterExpression(payload.axes?.y?.unit),
        };
    }

    _addCustomToolboxItems() {
        const views = this.config.options?.views || [];
        if (views.length < 2) return;
        const choices = views.map(view => ({
            value: view,
            label: view === 'by-worker' ? 'Separate by worker' : 'Accumulated',
        }));
        const current = this._fetchDataParams.view || this.config.options?.default_view || views[0];
        this.addToolboxSelectorItem(this.toolbox.createSelectorItem(
            `${this.id}-view`, 'Display', choices,
            (_id, value) => {
                this.updateFetchDataParam('view', value);
                this.fetchDataAndPlot();
            },
        ));
        setTimeout(() => {
            const selector = document.getElementById(`selector-${this.id}-view`);
            if (selector) selector.value = current;
        }, 0);
    }

    _seriesElementId(index) {
        return `${this.id}-series-${index}`;
    }

    initLegend() {
        if (!this.data?.series || this.data.series.length <= 1) return;
        const items = this.data.series.map((series, index) => ({
            id: this._seriesElementId(index),
            label: series.name,
            color: getWorkerColor(series.id || series.name, index),
        }));
        this.createLegendRow(items, {
            checkboxName: `${this.id}-series`,
            columnsPerRow: Math.min(4, items.length),
            onToggle: (id, visible) => {
                const index = Number(id.split('-').pop());
                this.seriesVisibility.set(index, visible);
                this.svg?.select(`#${id}`).style('display', visible ? null : 'none');
                this.svg?.selectAll(`.${id}-point`).style('display', visible ? null : 'none');
            },
        });
    }

    _tooltip(point, series) {
        const xFormatter = eval(this.data.x_tick_formatter);
        const yFormatter = eval(this.data.y_tick_formatter);
        const fields = this.data.point_fields || [];
        const lookup = this.data.metadata?.lookup || {};
        const rows = [
            series.name,
            `${fields[0] || 'X'}: ${xFormatter(point[0])}`,
            `${fields[1] || 'Value'}: ${yFormatter(point[1])}`,
        ];
        if (lookup[String(point[0])] != null) rows.splice(1, 0, `Entity: ${lookup[String(point[0])]}`);
        fields.slice(2).forEach((field, index) => rows.push(`${field}: ${point[index + 2]}`));
        return rows.join('<br>');
    }

    plot() {
        if (!this.data?.series) return;
        const renderer = this.data.renderer || this.config.renderer;
        const cursorSeries = [];
        this.data.series.forEach((series, index) => {
            const id = this._seriesElementId(index);
            const color = getWorkerColor(series.id || series.name, index) || COLORS[index % COLORS.length];
            if (renderer === 'scatter') {
                this.plotPoints(series.points, {
                    color,
                    className: `${id}-point`,
                    tooltipFormatter: point => this._tooltip(point, series),
                });
            } else {
                this.plotPath(series.points, {
                    id,
                    className: `${this.id}-metric-line`,
                    stroke: color,
                    strokeWidth: index === 0 ? 2.1 : 1.6,
                    curveType: this.data.curve === 'step' ? d3.curveStepAfter : d3.curveLinear,
                    disableHover: true,
                });
                cursorSeries.push({ id, name: series.name, color, points: series.points });
                if (renderer === 'line_points') {
                    this.plotPoints(series.points, {
                        radius: 2,
                        color,
                        className: `${id}-point`,
                        tooltipFormatter: point => this._tooltip(point, series),
                    });
                }
            }
            if (this.seriesVisibility.get(index) === false) {
                this.svg.select(`#${id}`).style('display', 'none');
                this.svg.selectAll(`.${id}-point`).style('display', 'none');
            }
        });
        if (renderer !== 'scatter' && cursorSeries.length) {
            this.installLineCursor(cursorSeries, {
                interpolation: this.data.curve === 'step' ? 'step' : 'linear',
                xFormatter: eval(this.data.x_tick_formatter),
                yFormatter: eval(this.data.y_tick_formatter),
                xLabel: this.data.axes?.x?.label || this.data.point_fields?.[0] || 'X',
                xUnit: this.data.axes?.x?.unit,
                isVisible: (_series, index) => this.seriesVisibility.get(index) !== false,
            });
        }
    }
}
