import { BaseModule } from './base.js';

export class TaskSubgraphsModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        
        this.defineFetchDataParams({
            subgraph_id: 0
        });
    }

    resetPlot() {
        super.resetPlot();

        this.data = null;
        this.updateFetchDataParam('subgraph_id', 0);
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

        if (visible) {
            this.updateFetchDataParam('subgraph_id', id);
            this.fetchDataAndPlot();
        } else {
            /* if the current checkbox is unchecked, clear the SVG */
            this.clearSVG();
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
        if (!this.data || !this.data.subgraph_svg_content || this.data.subgraph_id === 0) return;

        const svgElement = new DOMParser().parseFromString(this.data.subgraph_svg_content, 'image/svg+xml').documentElement;
        this.svgNode.parentNode.replaceChild(svgElement, this.svgNode);
        this.svgNode = svgElement;
        this.svgElement = d3.select(this.svgNode);
        this.svgElement.attr('preserveAspectRatio', 'xMidYMid meet');
    }
} 