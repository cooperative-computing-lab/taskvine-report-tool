import { BaseModule } from './base.js';

export class TaskSubgraphsModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this._current_subgraph_id = 1;
        this.folder = null;
    }

    async fetchData(folder, subgraph_id = 1, plot_failed_task = true, plot_recovery_task = true) {
        if (!subgraph_id) return;
        this.folder = folder;
        const response = await fetch(
            `${this.api_url}?` +
            `folder=${folder}` +
            `&subgraph_id=${subgraph_id}` +
            `&plot_failed_task=${plot_failed_task}` +
            `&plot_recovery_task=${plot_recovery_task}`
        );
        this.data = await response.json();
        this._current_subgraph_id = subgraph_id;

        this._setAxesFromFetchedData();
    }

    resetPlot() {
        super.resetPlot();

        this.data = null;
        this._current_subgraph_id = 1;
        this.folder = null;
    }

    legendOnToggle(id, visible) {
        const checkboxes = this._queryAllLegendCheckboxes();
        
        /* if there are other checkboxes, uncheck them */
        checkboxes.each(function () {
            const checkbox = d3.select(this);
            if (checkbox.attr('id') !== id) {
                checkbox.property('checked', false);
            }
        });

        /* if the current checkbox is checked, fetch data and plot */
        if (visible) {
            this.fetchData(this.folder, id);
            this.plot();
        }
    }

    initLegend() {
        if (!this.data || !this.data.legend) return;

        this.createLegendRow(this.data.legend, {
            checkboxName: 'task-subgraph-legend',
            singleSelect: true,
            onToggle: (id, visible) => {
                this.legendOnToggle(id, visible);
            }
        });
    }

    plot() {
        this.clearSVG();
        if (!this.data) return;

        const svgElement = new DOMParser().parseFromString(this.data.subgraph_svg_content, 'image/svg+xml').documentElement;
        this.svgNode.parentNode.replaceChild(svgElement, this.svgNode);
        this.svgNode = svgElement;
        this.svgElement = d3.select(this.svgNode);
        this.svgElement.attr('preserveAspectRatio', 'xMidYMid meet');
    }
} 