import { BaseModule } from './base.js';

export class TaskSubgraphsModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this._current_subgraph_id = 1;
    }

    async fetchData(folder, subgraph_id = 1, plot_failed_task = true, plot_recovery_task = true) {
        if (!subgraph_id) return;

        const response = await fetch(
            `${this.api_url}?subgraph_id=${subgraph_id}` +
            `&plot_failed_task=${plot_failed_task}` +
            `&plot_recovery_task=${plot_recovery_task}`
        );

        this.data = await response.json();
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